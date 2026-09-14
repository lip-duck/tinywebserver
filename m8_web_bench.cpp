#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <mysql/mysql.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <streambuf>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#define MAX_EVENTS 1024
#define MAX_FDS 65535
#define connpool_size 8
#define threadpool_size 4

// 宏定义，方便调用
#define LOG_DEBUG(fmt, ...) Logger::instance().log(Logger::DEBUG, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...) Logger::instance().log(Logger::INFO, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...) Logger::instance().log(Logger::WARN, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) Logger::instance().log(Logger::ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__)

using Buffer = std::vector<char>;  // 一个 4MB 的缓冲区

class ConnPool {
public:
    static ConnPool& instance() {
        static ConnPool inst;
        return inst;
    }

    // 从池里取一个连接
    MYSQL* get_conn() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (queue_.empty()) {
            // 队列为空，等 1 秒看有没有人归还
            if (cond_.wait_for(lock, std::chrono::seconds(1)) == std::cv_status::timeout) {
                return nullptr;  // 超时还没等到
            }
        }
        MYSQL* conn = queue_.front();
        queue_.pop();
        lock.unlock();

        // 检查连接是否还活着（ping）
        if (mysql_ping(conn)) {
            // 连接断了，重新连
            mysql_close(conn);
            conn = create_conn();
        }
        return conn;
    }

    // 用完归还连接
    void release_conn(MYSQL* conn) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push(conn);
        }
        cond_.notify_one();
    }

    void init(const char* host, const char* user, const char* passwd,
              const char* db, int pool_size) {
        host_ = host;
        user_ = user;
        passwd_ = passwd;
        db_ = db;
        for (int i = 0; i < pool_size; i++) {
            MYSQL* conn = create_conn();
            if (conn) queue_.push(conn);
        }
        std::cout << "连接池初始化完成，大小: " << queue_.size() << std::endl;
    }

    ~ConnPool() {
        while (!queue_.empty()) {
            MYSQL* conn = queue_.front();
            queue_.pop();
            mysql_close(conn);
        }
    }

private:
    ConnPool() = default;
    ConnPool(const ConnPool&) = delete;
    ConnPool& operator=(const ConnPool&) = delete;

    MYSQL* create_conn() {
        MYSQL* conn = mysql_init(nullptr);
        if (!conn) return nullptr;
        if (!mysql_real_connect(conn, host_.c_str(), user_.c_str(), passwd_.c_str(), db_.c_str(), 3306, nullptr, 0)) {
            std::cerr << "连接失败: " << mysql_error(conn) << std::endl;
            mysql_close(conn);
            return nullptr;
        }
        return conn;
    }

    std::queue<MYSQL*> queue_;
    std::mutex mutex_;
    std::condition_variable cond_;
    std::string host_, user_, passwd_, db_;
};

// RAII 封装：取连接，用完自动归还
class ConnGuard {
public:
    explicit ConnGuard(MYSQL* conn) : conn_(conn) {}
    ~ConnGuard() {
        if (conn_) ConnPool::instance().release_conn(conn_);
    }
    MYSQL* get() { return conn_; }

private:
    MYSQL* conn_;
};

class Logger {
public:
    // 获取单例
    static Logger& instance() {
        static Logger inst;
        return inst;
    }

    // 初始化：打开日志文件
    void init(const char* filename) {
        fp_ = fopen(filename, "a");         // append 模式
        next_buffer_.reserve(4096 * 1000);  // 【新增】备用缓冲区也预分配
        running_ = true;
        backend_thread_ = std::thread(&Logger::backend_thread_func, this);  // 【新增】
    }

    // 日志级别
    enum Level {
        DEBUG,
        INFO,
        WARN,
        ERROR
    };

    // 写日志（变参函数）
    void log(Level level, const char* file, int line, const char* fmt, ...) {
        // 1. 获取当前时间，格式：2026-09-12 14:30:45
        char timebuf[32];
        time_t now = time(nullptr);
        struct tm* tm = localtime(&now);
        strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", tm);

        // 2. 级别字符串
        const char* level_str[] = {"DEBUG", "INFO", "WARN", "ERROR"};

        // 3. 拼到临时缓冲区（不再直接写文件！）
        char linebuf[1024];
        int len = snprintf(linebuf, sizeof(linebuf),
                           "[%s] [%s] [%s:%d] ", timebuf, level_str[level], file, line);

        va_list ap;
        va_start(ap, fmt);
        len += vsnprintf(linebuf + len, sizeof(linebuf) - len, fmt, ap);
        va_end(ap);

        linebuf[len++] = '\n';

        // 4. 追加到异步缓冲区
        append(linebuf, len);
    }

    void append(const char* log_line, size_t len) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (current_buffer_.capacity() - current_buffer_.size() >= len) {
            // 当前缓冲区还够，直接追加
            current_buffer_.insert(current_buffer_.end(), log_line, log_line + len);
        } else {
            // 当前缓冲区满了：
            // 1. 把满的缓冲区丢到待写队列
            buffers_to_write_.push_back(std::move(current_buffer_));
            // 2. 用备用缓冲区替换
            current_buffer_ = std::move(next_buffer_);
            // 3. 写入新数据
            current_buffer_.insert(current_buffer_.end(), log_line, log_line + len);
            // 4. 唤醒后台线程
            cond_.notify_one();
        }
    }

    void backend_thread_func() {
        while (running_) {
            std::unique_lock<std::mutex> lock(mutex_);
            // 等待有待写缓冲区
            cond_.wait_for(lock, std::chrono::seconds(3));

            // 把当前缓冲区也丢进去（即使没满，3秒到了也要刷）
            if (!current_buffer_.empty()) {
                buffers_to_write_.push_back(std::move(current_buffer_));
                current_buffer_ = std::move(next_buffer_);

                // 【新增】确保新的 current_buffer_ 有空间
                if (current_buffer_.capacity() == 0) {
                    current_buffer_.reserve(4096 * 1000);
                }
            }

            // 解锁，开始写文件
            lock.unlock();

            // 批量写文件
            for (auto& buf : buffers_to_write_) {
                fwrite(buf.data(), 1, buf.size(), fp_);
                buf.clear();  // 清空，下次复用
            }
            fflush(fp_);

            // 把空缓冲区放回备用
            buffers_to_write_.clear();
        }
    }

private:
    Logger() : fp_(nullptr) {
        current_buffer_.reserve(4096 * 1000);  // 预分配 4MB
    }
    ~Logger() {
        running_ = false;
        cond_.notify_one();  // 唤醒后台线程退出
        if (backend_thread_.joinable()) {
            backend_thread_.join();  // 等后台线程跑完
        }
        // 把剩余缓冲区写文件
        if (current_buffer_.size() > 0) {
            fwrite(current_buffer_.data(), 1, current_buffer_.size(), fp_);
        }
        fflush(fp_);
        if (fp_) fclose(fp_);
    }
    FILE* fp_;

    Buffer current_buffer_;                 // 当前正在写的
    Buffer next_buffer_;                    // 备用的
    std::vector<Buffer> buffers_to_write_;  // 待写队列（满了的缓冲区放这里）

    std::mutex mutex_;
    std::condition_variable cond_;
    std::thread backend_thread_;
    bool running_;
};

class Threadpool {
public:
    explicit Threadpool(int num_threads) {
        stop_ = false;
        for (int i = 0; i < num_threads; i++) {
            workers_.emplace_back([this]() { this->worker(); });
        }
        LOG_INFO("线程池启动，工作线程数%d", num_threads);
    }

    // 提交任务（对外接口）
    void submit(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stop_) {
                std::cerr << "线程池已停止，拒绝提交任务" << std::endl;
                return;
            }
            tasks_.push(std::move(task));
        }
        cv_.notify_one();
    }

    // 停止所有线程
    void stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stop_) return;  // 防止重复调用
            stop_ = true;
        }
        cv_.notify_all();  // 唤醒所有等待的线程

        // 等待所有工作线程退出
        for (auto& t : workers_) {
            if (t.joinable()) {
                t.join();
            }
        }
        LOG_INFO("线程池已停止，所有工作线程已退出");
    }

    // 析构函数：自动停止
    ~Threadpool() {
        stop();
    }

    // 禁止拷贝和移动（含 mutex 和 thread，不可拷贝）
    Threadpool(const Threadpool&) = delete;
    Threadpool& operator=(const Threadpool&) = delete;

private:
    void worker() {
        std::function<void()> task;
        while (1) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this]() { return stop_ || !tasks_.empty(); });
                if (stop_ && tasks_.empty()) {
                    return;
                }
                task = std::move(tasks_.front());
                tasks_.pop();
            }
            task();
        }
    }
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::vector<std::thread> workers_;
    std::condition_variable cv_;
    bool stop_;
};
// 全局变量
Threadpool* g_pool = nullptr;

class channel {
public:
    channel(int fd) {
        fd_ = fd;
        events_ = 0;
        revents_ = 0;
    }
    // setevents
    void setreadevents() {
        events_ |= EPOLLIN;
    }
    // 声明
    void enable_write();
    void disable_write();
    // set callback functions
    void setreadcallback(std::function<void()> cb) {
        readcallback_ = std::move(cb);
    }
    void set_in_epoll(bool v) { in_epoll_ = v; }
    bool in_epoll() const { return in_epoll_; }

    /*
    void setwritecallback(std::function<void()> cb) {
        writecallback_ = std::move(cb);
    }
        void setwriteevents() {
        events_ |= EPOLLOUT;
    }
    */
    void set_close_callback(std::function<void()> cb) {
        close_callback_ = std::move(cb);
    }
    // 外部调用send
    void write(const std::string& data) {
        if (!write_buf.empty()) {
            write_buf += data;
            return;
        }

        int n_sent = send(fd_, data.c_str(), data.size(), 0);

        if (n_sent == -1) {
            // send出错
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                write_buf = data;
                enable_write();
            } else {
                perror("send");
            }
            return;
        }

        if (n_sent < (int)data.size()) {
            write_buf += data.substr(n_sent);
            enable_write();
        }

        // 情况4：全部发完了
        if (n_sent == (int)data.size()) {
            if (close_after_write_ && close_callback_) {
                close_callback_();  // 【新增】全部发完了，直接关闭
            }
        }
    }

    // 处理writebuf
    void handle_write() {
        if (write_buf.empty()) {
            disable_write();
            return;
        }

        // 发送缓冲区里的数据
        int n_sent = send(fd_, write_buf.c_str(), write_buf.size(), 0);

        if (n_sent == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;  // 又满了，等下一次 EPOLLOUT
            }
            perror("send");
            return;
        }

        // 移除已发送的部分
        write_buf.erase(0, n_sent);

        // 发完了
        if (write_buf.empty()) {
            disable_write();  // 【关键】取消 EPOLLOUT，否则 LT 模式下一直触发，CPU 100%

            // 短连接：发完后关闭
            if (close_after_write_) {
                if (close_callback_) {
                    close_callback_();  // 调用清理回调（close fd、delete 等）
                }
            }
        }
    }

    // handle events
    void handle_events() {
        if (revents_ & EPOLLIN) {
            if (readcallback_) readcallback_();
        }
        if (revents_ & EPOLLOUT) {
            handle_write();
        }
    }

    // getters and setters
    int getfd() const {
        return fd_;
    }
    uint32_t getevents() const {
        return events_;
    }
    void setrevents(uint32_t revents) {
        revents_ = revents;
    }
    void set_close_after_write(bool v) {
        close_after_write_ = v;
    }

private:
    int fd_;
    uint32_t events_;
    uint32_t revents_;
    std::function<void()> readcallback_;
    std::function<void()> writecallback_;

    std::string write_buf;
    bool close_after_write_ = false;
    std::function<void()> close_callback_;

    bool in_epoll_ = false;
};

class eventloop {
public:
    eventloop() {
        channels_.resize(MAX_FDS, nullptr);
        epollfd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epollfd_ == -1) {
            perror("epoll_create");
            exit(1);
        }
        looping_ = false;

        // 【新增】创建 eventfd 唤醒 fd
        wakeup_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wakeup_fd_ == -1) {
            perror("eventfd");
            exit(1);
        }
        // 【新增】把 wakeup_fd 包装成 channel，加入 epoll
        wakeup_channel_ = new channel(wakeup_fd_);
        wakeup_channel_->setreadevents();
        wakeup_channel_->setreadcallback([this]() {
            handle_wakeup();
        });
        updatechannel(wakeup_channel_);
    }
    ~eventloop() {
        close(wakeup_fd_);
        delete wakeup_channel_;
        close(epollfd_);
    }

    // 【核心方法】把任务丢回主线程执行
    void runInLoop(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pending_tasks.push_back(std::move(task));
        }
        // 写 eventfd，唤醒主线程的 epoll_wait
        uint64_t one = 1;
        write(wakeup_fd_, &one, sizeof(one));
    }
    // 判断竞态
    channel* get_channel(int fd) {
        return channels_[fd];
    }

    // change channel's events
    void updatechannel(channel* ch) {
        struct epoll_event ev;
        ev.events = ch->getevents();
        ev.data.ptr = ch;
        int fd = ch->getfd();

        if (fd < 0 || fd >= (int)channels_.size()) {
            LOG_ERROR("updatechannel fd 越界: %d", fd);
            close(fd);
            delete ch;
            return;
        }

        int op;
        if (!ch->in_epoll()) {
            op = EPOLL_CTL_ADD;
            ch->set_in_epoll(true);
        } else {
            op = EPOLL_CTL_MOD;
        }

        if (epoll_ctl(epollfd_, op, ch->getfd(), &ev) == -1) {
            perror("epoll_ctl");
            return;  // 不要exit()
        }
        channels_[fd] = ch;  // 【修复】把 ch 存到 channels_ 数组
    }

    void deletechannel(channel* ch) {
        int fd = ch->getfd();
        if (fd < 0 || fd >= (int)channels_.size()) return;
        if (epoll_ctl(epollfd_, EPOLL_CTL_DEL, ch->getfd(), nullptr) == -1) {
            perror("epoll_ctl_delete");
            return;
        }
        channels_[ch->getfd()] = nullptr;
    }

    void remove_from_epoll(channel* ch) {
        int fd = ch->getfd();
        if (fd < 0 || fd >= (int)channels_.size()) return;
        epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, nullptr);
        // 注意：不把 channels_[fd] 设为 nullptr！
        ch->set_in_epoll(false);  // 标记不在 epoll 里
    }

    void add_timer(int fd) {
        time_t expire = time(nullptr) + TIMEOUT_SEC;
        timer_map_.insert({expire, fd});
        // LOG_INFO("add_timer");
    }

    void delete_timer(int fd) {
        for (auto it = timer_map_.begin(); it != timer_map_.end();) {
            if (it->second == fd) {
                it = timer_map_.erase(it);
            } else {
                it++;
            }
        }
        // LOG_INFO("delete_timer");
    }

    void update_timer(int fd) {
        for (auto it = timer_map_.begin(); it != timer_map_.end();) {
            if (it->second == fd) {
                it = timer_map_.erase(it);
            } else {
                it++;
            }
        }
        time_t expire = time(nullptr) + TIMEOUT_SEC;
        timer_map_.insert({expire, fd});
    }

    void handle_expired() {
        time_t now = time(nullptr);
        // LOG_DEBUG("handle_expired, timer_count=%d", timer_map_.size());
        //  multimap 按 key（过期时间）升序排列，begin() 是最早过期的
        while (!timer_map_.empty()) {
            auto it = timer_map_.begin();
            // LOG_DEBUG("检查 fd=%d,expire=%d,now=%d", it->second, it->first, now);
            if (it->first > now) {
                break;  // 最早的都没过期，后面的更晚，不用检查了
            }
            int fd = it->second;
            timer_map_.erase(it);  // 先从定时器里移除
            // 【新增】fd 有效性检查
            if (fd >= 0 && fd < (int)channels_.size()) {
                close_connection(fd);
            } else {
                std::cerr << "handle_expired: 无效 fd=" << fd << std::endl;
            }
        }
    }

    void close_connection(int fd);

    // loop
    void loop() {
        looping_ = true;
        struct epoll_event events[MAX_EVENTS];

        while (looping_) {
            int nready = epoll_wait(epollfd_, events, MAX_EVENTS, 1000);
            if (nready == -1) {
                if (errno == EINTR) continue;  // 被信号中断，重试
                perror("epoll_wait");
                break;
            }

            for (int i = 0; i < nready; i++) {
                channel* ch = static_cast<channel*>(events[i].data.ptr);
                ch->setrevents(events[i].events);
                ch->handle_events();
            }
            handle_expired();  // 每次循环检查超时连接
        }
    }

    void quit() {
        looping_ = false;
    }

private:
    void handle_wakeup() {
        // 1. 读 eventfd，清零计数器（必须读，否则一直触发 EPOLLIN）
        uint64_t one;
        ssize_t n = read(wakeup_fd_, &one, sizeof(one));
        if (n != sizeof(one)) {
            // 非阻塞下可能读不到（理论上不会，因为 EPOLLIN 触发说明有数据）
        }

        // 2. 取出所有待执行任务（用 swap 避免长时间占锁）
        std::vector<std::function<void()>> tasks;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks.swap(pending_tasks);  // O(1) 交换，pending_tasks_ 变空
        }

        // 3. 在主线程执行所有任务
        for (auto& task : tasks) {
            task();
        }
    }
    int epollfd_;
    bool looping_;
    std::vector<channel*> channels_;

    std::multimap<time_t, int> timer_map_;
    static const int TIMEOUT_SEC = 15;

    // runInloop
    int wakeup_fd_;
    channel* wakeup_channel_;
    std::vector<std::function<void()>> pending_tasks;
    std::mutex mutex_;
};
// 全局变量
eventloop* g_loop = nullptr;

void channel::enable_write() {
    events_ |= EPOLLOUT;
    g_loop->runInLoop([this]() {
        g_loop->updatechannel(this);  // ✅ 丢回主线程执行
    });
}
void channel::disable_write() {
    events_ &= ~EPOLLOUT;
    g_loop->runInLoop([this]() {
        g_loop->updatechannel(this);  // ✅ 丢回主线程执行
    });
}
// 请求解析状态机
class Httprequest {
public:
    enum parse_state { REQUEST_LINE,
                       HEADERS,
                       BODY,
                       FINISHED };
    // 重置状态，准备解析下一个请求（长连接/粘包用）
    void reset() {
        state_ = REQUEST_LINE;
        method_.clear();
        path_.clear();
        version_.clear();
        headers_.clear();
        content_length_ = 0;
        body_.clear();
        // 注意：buffer_ 不清除！因为里面可能还有下一个请求的粘包数据
    }
    Httprequest() {
        reset();
    }

    // 核心parse
    bool parse(const char* data, int len) {
        buffer_.append(data, len);

        while (1) {
            if (state_ == REQUEST_LINE) {
                // 检查是否有完整的请求行
                auto pos = buffer_.find("\r\n");
                if (pos == std::string::npos) {
                    break;  // 请求行未完整接收
                }
                // 解析请求行
                std::string line = buffer_.substr(0, pos);
                sscanf(line.c_str(), "%s %s %s", method_buf, path_buf, version_buf);
                method_ = method_buf;
                path_ = path_buf;
                version_ = version_buf;
                // 移除已解析
                buffer_.erase(0, pos + 2);
                state_ = HEADERS;
            } else if (state_ == HEADERS) {
                // 检查是否有完整的请求头
                auto pos = buffer_.find("\r\n");
                if (pos == std::string::npos) {
                    break;  // 请求行未完整接收
                }
                // 解析请求行
                std::string line = buffer_.substr(0, pos);
                buffer_.erase(0, pos + 2);

                if (line.empty()) {
                    // 空行，表示请求头结束
                    if (content_length_ > 0) {
                        state_ = BODY;
                    } else {
                        state_ = FINISHED;
                    }
                } else {
                    // 解析请求头
                    auto colon_pos = line.find(':');
                    if (colon_pos != std::string::npos) {
                        std::string key = line.substr(0, colon_pos);
                        std::string value = line.substr(colon_pos + 2);
                        headers_[key] = value;
                        if (key == "Content-Length") {
                            content_length_ = std::stoi(value);
                        }
                    }
                }
            } else if (state_ == BODY) {
                if ((int)buffer_.size() < content_length_) {
                    break;  // 请求体未完整接收
                }
                body_ = buffer_.substr(0, content_length_);
                buffer_.erase(0, content_length_);
                state_ = FINISHED;
            } else if (state_ == FINISHED) {
                break;
            }
        }

        return (state_ == FINISHED);
    }

    // getters
    const std::string& getmethod() const {
        return method_;
    }
    const std::string& getpath() const {
        return path_;
    }
    const std::string& getversion() const {
        return version_;
    }
    const std::unordered_map<std::string, std::string>& getheaders() const {
        return headers_;
    }
    int getcontentlength() const {
        return content_length_;
    }
    const std::string& getbody() const {
        return body_;
    }
    // 获取某个请求头的值
    std::string getheader(const std::string& key) const {
        auto it = headers_.find(key);
        if (it != headers_.end()) {
            return it->second;
        }
        return "";
    }

    // 打印解析结果（调试用）
    void print() const {
        printf("===== HTTP 请求解析结果 =====\n");
        printf("方法: %s\n", method_.c_str());
        printf("URL:  %s\n", path_.c_str());
        printf("版本: %s\n", version_.c_str());
        printf("请求头:\n");
        for (auto& kv : headers_) {
            printf("  %s: %s\n", kv.first.c_str(), kv.second.c_str());
        }
        if (content_length_ > 0) {
            printf("请求体长度: %d\n", content_length_);
            printf("请求体: %s\n", body_.c_str());
        }
        printf("==============================\n");
    }

private:
    parse_state state_;
    std::string buffer_;

    std::string method_;
    std::string path_;
    std::string version_;
    std::unordered_map<std::string, std::string> headers_;
    int content_length_;
    std::string body_;

    // sscanf临时缓冲区
    char method_buf[16];
    char path_buf[256];
    char version_buf[16];
};

// 每个fd对应一个Httprequest对象，存储解析状态
std::unordered_map<int, Httprequest*> g_http_requests;
std::mutex g_http_mutex;  // 【新增】保护 g_http_requests

void eventloop::close_connection(int fd) {
    // 【新增】fd 范围检查，防止越界
    if (fd < 0 || fd >= (int)channels_.size()) {
        close(fd);
        return;
    }
    // LOG_DEBUG("close_connection fd=%d,channels_size=%d", fd, channels_.size());
    channel* ch = channels_[fd];  // channels_ 数组就是 fd -> channel* 映射
    if (ch) {
        remove_from_epoll(ch);  // 从 epoll 移除
        close(fd);              // 关闭 fd
        delete ch;              // 释放 channel
        channels_[fd] = nullptr;
    }
    {
        std::lock_guard<std::mutex> lock(g_http_mutex);
        g_http_requests.erase(fd);  // 【修改】只 erase，不 delete req
    }
    // LOG_INFO("关闭连接");
}

// url路径映射
std::string url_to_path(const std::string& url) {
    std::string path;

    if (url == "/") {
        path = "resources/index.html";
    } else {
        path = "resources" + url;
    }

    if (url.find("..") != std::string::npos) {
        return "";  // 防止目录遍历攻击
    }
    return path;
}

// 根据文件后缀名获取Content-Type
std::string get_content_type(const std::string& path) {
    size_t dot_pos = path.rfind('.');
    if (dot_pos == std::string::npos) {
        return "application/octet-stream";  // 默认类型
    }
    std::string ext = path.substr(dot_pos + 1);
    if (ext == "html" || ext == "htm") {
        return "text/html; charset=utf-8";
    } else if (ext == "css") {
        return "text/css";
    } else if (ext == "js") {
        return "application/javascript";
    } else if (ext == "png") {
        return "image/png";
    } else if (ext == "jpg" || ext == "jpeg") {
        return "image/jpeg";
    } else if (ext == "gif") {
        return "image/gif";
    } else if (ext == "txt") {
        return "text/plain";
    }
    return "application/octet-stream";  // 默认类型
}

// 读取文件内容
bool read_file(const std::string& path, std::string& body) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        return false;
    }
    body.assign((std::istreambuf_iterator<char>(file)),
                std::istreambuf_iterator<char>());
    file.close();
    return true;
}

// 状态码转描述文字
std::string status_to_text(int code) {
    switch (code) {
        case 200:
            return "OK";
        case 400:
            return "Bad Request";
        case 403:
            return "Forbidden";
        case 404:
            return "Not Found";
        case 405:
            return "Method Not Allowed";
        case 500:
            return "Internal Server Error";
        default:
            return "Unknown";
    }
}

// 构造默认错误页面（当自定义错误页面不存在时用）
std::string make_default_error_page(int code, const std::string& message) {
    return "<!DOCTYPE html><html><head><meta charset='utf-8'>"
           "<title>" +
           std::to_string(code) + " " + status_to_text(code) +
           "</title></head>"
           "<body style='font-family: sans-serif; text-align: center; padding: 50px;'>"
           "<h1 style='font-size: 72px; color: #666;'>" +
           std::to_string(code) +
           "</h1>"
           "<h2>" +
           status_to_text(code) +
           "</h2>"
           "<p style='color: #9f0a0a93;'>" +
           message +
           "</p>"
           "<hr><p><a href='/'>返回首页</a></p>"
           "</body></html>";
}

// 统一构造错误响应
// 优先读取 resources/xxx.html 自定义错误页面，不存在就用默认页面
std::string make_error_response(int code, const std::string& message) {
    std::string body;

    // 尝试读取自定义错误页面，比如 resources/404.html
    std::string error_page_path = "resources/" + std::to_string(code) + ".html";
    if (!read_file(error_page_path, body)) {
        // 自定义页面不存在，用默认页面
        body = make_default_error_page(code, message);
    }

    // 构造完整响应
    std::string response;
    response += "HTTP/1.1 " + std::to_string(code) + " " + status_to_text(code) + "\r\n";
    response += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    response += "Content-Type: text/html; charset=utf-8\r\n";
    response += "\r\n";
    response += body;
    return response;
}

// 工作线程的read函数
void process_request(int fd, Httprequest* req, channel* ch) {
    // 【新增】打印当前线程 ID
    LOG_INFO("[线程池] 线程ID:%d,处理 fd=%d,URL=%s", std::this_thread::get_id(), fd, req->getpath().c_str());
    req->print();
    std::string response;
    bool keep_alive = true;
    // 判断是否保持连接
    std::string connection_header = req->getheader("Connection");
    if (connection_header == "close") {
        keep_alive = false;
    } else if (req->getversion() == "HTTP/1.0" && connection_header != "keep-alive") {
        keep_alive = false;
    }

    if (req->getmethod() == "POST" && req->getpath() == "/register") {
        // 注册：解析 body 里的 username 和 password
        // body 格式：username=zhangsan&password=123456
        std::string body = req->getbody();
        std::string username, password;
        // 简单解析 body（后面再优化）
        size_t pos = body.find("username=");
        if (pos != std::string::npos) {
            size_t start = pos + 9;
            size_t end = body.find("&", start);
            username = body.substr(start, end - start);
        }
        pos = body.find("password=");
        if (pos != std::string::npos) {
            size_t start = pos + 9;
            password = body.substr(start);
        }

        // 查数据库
        ConnGuard guard(ConnPool::instance().get_conn());
        MYSQL* conn = guard.get();

        // 先查用户名是否存在
        char sql[512];
        snprintf(sql, sizeof(sql),
                 "SELECT id FROM user WHERE username='%s'", username.c_str());
        mysql_query(conn, sql);
        MYSQL_RES* res = mysql_store_result(conn);
        if (mysql_num_rows(res) > 0) {
            response += "HTTP/1.1 409 Conflict\r\n";
            response += "Content-Type: text/plain\r\n";
            response += "Content-Length: 18\r\n";
            response += "\r\n";
            response += "用户名已存在";
        } else {
            // 插入新用户
            snprintf(sql, sizeof(sql),
                     "INSERT INTO user (username, password) VALUES ('%s','%s')",
                     username.c_str(), password.c_str());
            if (mysql_query(conn, sql) == 0) {
                response += "HTTP/1.1 200 OK\r\n";
                response += "Content-Type: text/plain\r\n";
                response += "Content-Length: 12\r\n";
                response += "\r\n";
                response += "注册成功";

            } else {
                response += "HTTP/1.1 500 Internal Error\r\n";
                response += "Content-Type: text/plain\r\n";
                response += "Content-Length: 12\r\n";
                response += "\r\n";
                response += "服务器错误";
            }
        }
        mysql_free_result(res);
    } else if (req->getmethod() == "POST" && req->getpath() == "/login") {
        // 登录：验证用户名密码
        std::string body = req->getbody();
        std::string username, password;
        size_t pos = body.find("username=");
        if (pos != std::string::npos) {
            size_t start = pos + 9;
            size_t end = body.find("&", start);
            username = body.substr(start, end - start);
        }
        pos = body.find("password=");
        if (pos != std::string::npos) {
            size_t start = pos + 9;
            password = body.substr(start);
        }

        ConnGuard guard(ConnPool::instance().get_conn());
        MYSQL* conn = guard.get();

        char sql[512];
        snprintf(sql, sizeof(sql),
                 "SELECT password FROM user WHERE username='%s'", username.c_str());
        mysql_query(conn, sql);
        MYSQL_RES* res = mysql_store_result(conn);
        MYSQL_ROW row = mysql_fetch_row(res);
        if (row && password == row[0]) {
            response += "HTTP/1.1 200 OK\r\n";
            response += "Content-Type: text/plain\r\n";
            response += "Content-Length: 12\r\n";
            response += "\r\n";
            response += "登录成功";
        } else {
            response += "HTTP/1.1 401 Unauthorized\r\n";
            response += "Content-Type: text/plain\r\n";
            response += "Content-Length: 24\r\n";
            response += "\r\n";
            response += "用户名或密码错误";
        }
        mysql_free_result(res);
    } else if (req->getmethod() == "GET") {
        // url映射
        std::string file_path = url_to_path(req->getpath());
        LOG_INFO("Mapped URL: %s to file path: %s", req->getpath().c_str(), file_path.c_str());
        // 2.检查非法路径
        if (file_path.empty()) {
            response = make_error_response(403, "Forbidden");
        } else {
            // 3.检查文件是否存在
            std::string body;
            if (!read_file(file_path, body)) {
                response = make_error_response(404, "Not Found");
                LOG_INFO("File not found:%s, sending 404 response. ", file_path.c_str());
            } else {
                // 4.文件存在，构造200响应
                std::string content_type = get_content_type(file_path);
                LOG_INFO("Serving file:%swith Content-Type:%s", file_path.c_str(), content_type.c_str());
                LOG_INFO("File size:%dbytes", body.size());
                response += "HTTP/1.1 200 OK\r\n";
                response += "Content-Length: " + std::to_string(body.size()) + "\r\n";
                response += "Content-Type: " + content_type + "\r\n";
                response += "\r\n";
                response += body;
            }
        }
    } else {
        LOG_INFO("Unsupported HTTP method:%s, sending 405 response.", req->getmethod().c_str());
        response = make_error_response(405, "Method Not Allowed");
    }

    // 添加 Connection 响应头
    auto header_end = response.find("\r\n\r\n");
    if (header_end != std::string::npos) {
        std::string conn_line = "Connection: " +
                                std::string(keep_alive ? "keep-alive" : "close") + "\r\n";
        response.insert(header_end + 2, conn_line);
    }

    // 【修改】发完响应后的回调：不再关闭连接，而是重新注册读事件
    ch->set_close_callback([fd]() {
        // 用 runInLoop 把"重新注册读事件"丢回主线程执行
        g_loop->runInLoop([fd]() {
            channel* ch = g_loop->get_channel(fd);
            if (!ch) {
                return;
            }

            Httprequest* req;
            {
                std::lock_guard<std::mutex> lock(g_http_mutex);
                auto it = g_http_requests.find(fd);
                if (it == g_http_requests.end()) return;
                req = it->second;
            }
            req->reset();

            // 2. 重新注册读事件（ch 可能之前被 deletechannel 了，所以走 ADD 分支）
            ch->setreadevents();
            g_loop->updatechannel(ch);

            // 3. 更新定时器
            g_loop->update_timer(fd);

            // 4. 【粘包处理】检查 buffer_ 里是否还有完整的请求
            //    如果有，立即提交任务处理（不需要等新的网络数据）
            if (req->parse("", 0)) {
                g_loop->remove_from_epoll(ch);
                process_request(fd, req, ch);
            }
        });
    });

    ch->set_close_after_write(true);  // 短连接：发完就关

    // 【修改】不再自己循环 send，交给 channel 异步发送
    ch->write(response);
}

// 已连接fd的读处理函数
void handle_read(channel* ch) {
    int fd = ch->getfd();
    char buf[1024];

    // 检查http连接
    std::unordered_map<int, Httprequest*>::iterator it;
    {
        std::lock_guard<std::mutex> lock(g_http_mutex);
        it = g_http_requests.find(fd);
    }

    if (it == g_http_requests.end()) {
        // g_loop->deletechannel(ch);
        g_loop->delete_timer(fd);
        close(fd);
        // delete ch;
        return;
    }
    Httprequest* req = it->second;

    while (1) {
        memset(buf, 0, sizeof(buf));
        int n = recv(fd, buf, sizeof(buf), 0);
        if (n == 0) {
            g_loop->deletechannel(ch);
            g_loop->delete_timer(fd);
            close(fd);
            // 不 delete ch 和 req，标记待删除，让 handle_expired 统一清理
            {
                std::lock_guard<std::mutex> lock(g_http_mutex);
                g_http_requests.erase(fd);
            }
            break;
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                perror("recv");
                g_loop->remove_from_epoll(ch);
                g_loop->delete_timer(fd);
                close(fd);
                // 不 delete ch 和 req，让 close_connection 统一清理
                {
                    std::lock_guard<std::mutex> lock(g_http_mutex);
                    g_http_requests.erase(fd);
                }
                break;
            }
        }
        g_loop->update_timer(fd);
        bool done = req->parse(buf, n);
        if (done) {
            LOG_INFO("parse done, fd=%d, submitting to pool", fd);
            g_loop->remove_from_epoll(ch);
            g_pool->submit([fd]() {
                channel* ch = g_loop->get_channel(fd);
                if (!ch) return;
                Httprequest* req;
                {
                    std::lock_guard<std::mutex> lock(g_http_mutex);
                    auto it = g_http_requests.find(fd);
                    if (it == g_http_requests.end()) return;
                    req = it->second;
                }
                process_request(fd, req, ch);
            });
            break;
        }
    }
}
// listenfd的accept处理函数
void handle_accept(channel* ch) {
    struct sockaddr_in client_addr;
    socklen_t len = sizeof(client_addr);

    int connfd = accept(ch->getfd(), (struct sockaddr*)&client_addr, &len);
    if (connfd == -1) {
        perror("accept");
        return;
    }
    // 设置非阻塞
    int flags = fcntl(connfd, F_GETFL, 0);
    flags |= O_NONBLOCK;
    int ret = fcntl(connfd, F_SETFL, flags);
    if (ret == -1) {
        perror("fcntl F_SETFL");
    }

    channel* client_ch = new channel(connfd);
    client_ch->setreadevents();

    client_ch->setreadcallback([client_ch]() {
        handle_read(client_ch);
    });

    g_loop->updatechannel(client_ch);
    // 创建一个新的http连接
    {
        std::lock_guard<std::mutex> lock(g_http_mutex);
        g_http_requests[connfd] = new Httprequest();
    }
    g_loop->add_timer(connfd);
}
int main() {
    g_loop = new eventloop();
    Logger::instance().init("server.log");
    ConnPool::instance().init("localhost", "lip", "123456", "webserver", 8);
    g_pool = new Threadpool(4);

    // 循环监听 20 个端口
    for (int i = 0; i < 20; i++) {
        int listenfd = socket(AF_INET, SOCK_STREAM, 0);
        if (listenfd == -1) {
            perror("socket");
            exit(1);
        }

        int opt = 1;
        setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        int flags = fcntl(listenfd, F_GETFL, 0);
        flags |= O_NONBLOCK;
        fcntl(listenfd, F_SETFL, flags);

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(9000 + i);
        addr.sin_addr.s_addr = htonl(INADDR_ANY);

        if (bind(listenfd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
            perror("bind");
            exit(1);
        }
        if (listen(listenfd, SOMAXCONN) == -1) {
            perror("listen");
            exit(1);
        }

        channel* listen_ch = new channel(listenfd);
        listen_ch->setreadevents();
        listen_ch->setreadcallback([listen_ch]() { handle_accept(listen_ch); });
        g_loop->updatechannel(listen_ch);
    }

    std::cout << "服务器启动，监听 9000~9019 端口" << std::endl;
    g_loop->loop();
    return 0;
}
