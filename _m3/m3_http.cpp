#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <streambuf>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include<mutex>
#define MAX_EVENTS 1024
#define MAX_FDS 65535

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
    void setwriteevents() {
        events_ |= EPOLLOUT;
    }
    // set callback functions
    void setreadcallback(std::function<void()> cb) {
        readcallback_ = std::move(cb);
    }
    void setwritecallback(std::function<void()> cb) {
        writecallback_ = std::move(cb);
    }
    // handle events
    void handle_events() {
        if (revents_ & EPOLLIN) {
            if (readcallback_) readcallback_();
        }
        if (revents_ & EPOLLOUT) {
            if (writecallback_) writecallback_();
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

private:
    int fd_;
    uint32_t events_;
    uint32_t revents_;
    std::function<void()> readcallback_;
    std::function<void()> writecallback_;
};

class eventloop {
public:
    eventloop() {
        channels_.resize(MAX_FDS, nullptr);
        epollfd_ = epoll_create(1);
        if (epollfd_ == -1) {
            perror("epoll_create");
            exit(1);
        }
        looping_ = false;
    }
    ~eventloop() {
        close(epollfd_);
    }
    // change channel's events
    void updatechannel(channel* ch) {
        struct epoll_event ev;
        ev.events = ch->getevents();
        ev.data.ptr = ch;
        if (epoll_ctl(epollfd_, EPOLL_CTL_ADD, ch->getfd(), &ev) == -1) {
            perror("epoll_ctl_add");
            exit(1);
        }
        channels_[ch->getfd()] = ch;
    }
    void deletechannel(channel* ch) {
        if (epoll_ctl(epollfd_, EPOLL_CTL_DEL, ch->getfd(), nullptr) == -1) {
            perror("epoll_ctl_delete");
            exit(1);
        }
        channels_[ch->getfd()] = nullptr;
    }
    // loop
    void loop() {
        looping_ = true;
        struct epoll_event events[MAX_EVENTS];

        while (looping_) {
            int nready = epoll_wait(epollfd_, events, MAX_EVENTS, -1);
            if (nready == -1) {
                perror("epoll_wait");
                break;
            }

            for (int i = 0; i < nready; i++) {
                channel* ch = static_cast<channel*>(events[i].data.ptr);
                ch->setrevents(events[i].events);
                ch->handle_events();
            }
        }
    }

    void quit() {
        looping_ = false;
    }

private:
    int epollfd_;
    bool looping_;
    std::vector<channel*> channels_;
};

// 全局变量
eventloop* g_loop = nullptr;

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

// 已连接fd的读处理函数
void handle_read(channel* ch) {
    int fd = ch->getfd();
    char buf[1024];

    // 检查http连接
    auto it = g_http_requests.find(fd);
    if (it == g_http_requests.end()) {
        g_loop->deletechannel(ch);
        close(fd);
        delete ch;
        return;
    }
    Httprequest* req = it->second;

    while (1) {
        memset(buf, 0, sizeof(buf));
        int n = recv(fd, buf, sizeof(buf), 0);
        if (n == 0) {
            g_loop->deletechannel(ch);
            close(fd);
            delete ch;
            delete req;
            g_http_requests.erase(fd);
            break;
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                perror("recv");
                g_loop->deletechannel(ch);
                close(fd);
                delete ch;
                delete req;
                g_http_requests.erase(fd);
                break;
            }
        }
        bool done = req->parse(buf, n);

        while (done) {
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

            // 1.检查方法是否支持
            if (req->getmethod() != "GET") {
                std::cout << "Unsupported HTTP method: " << req->getmethod() << ", sending 405 response." << std::endl;
                response = make_error_response(405, "Method Not Allowed");
            } else {
                // url映射
                std::string file_path = url_to_path(req->getpath());
                std::cout << "Mapped URL: " << req->getpath() << " to file path: " << file_path << std::endl;
                // 2.检查非法路径
                if (file_path.empty()) {
                    response = make_error_response(403, "Forbidden");
                } else {
                    // 3.检查文件是否存在
                    std::string body;
                    if (!read_file(file_path, body)) {
                        response = make_error_response(404, "Not Found");
                        std::cout << "File not found: " << file_path << ", sending 404 response." << std::endl;
                    } else {
                        // 4.文件存在，构造200响应
                        std::string content_type = get_content_type(file_path);
                        std::cout << "Serving file: " << file_path << " with Content-Type: " << content_type << std::endl;
                        std::cout << "File size: " << body.size() << " bytes" << std::endl;
                        response += "HTTP/1.1 200 OK\r\n";
                        response += "Content-Length: " + std::to_string(body.size()) + "\r\n";
                        response += "Content-Type: " + content_type + "\r\n";
                        response += "\r\n";
                        response += body;
                    }
                }
            }

            // 添加 Connection 响应头
            auto header_end = response.find("\r\n\r\n");
            if (header_end != std::string::npos) {
                std::string conn_line = "Connection: " +
                                        std::string(keep_alive ? "keep-alive" : "close") + "\r\n";
                response.insert(header_end, conn_line);
            }
            // 发送响应
            int total_sent = 0;
            while (total_sent < (int)response.size()) {
                int n_sent = send(fd, response.c_str() + total_sent, response.size() - total_sent, 0);
                if (n_sent == -1) {
                    perror("send");
                    break;
                }
                total_sent += n_sent;
            }

            if (keep_alive) {
                req->reset();              // 重置状态，准备解析下一个请求
                done = req->parse("", 0);  // 尝试解析下一个请求（如果有粘包数据）
            } else {
                g_loop->deletechannel(ch);
                close(fd);
                delete ch;
                delete req;
                g_http_requests.erase(fd);
                break;
            }
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
    g_http_requests[connfd] = new Httprequest();
}

int main() {
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd == -1) {
        perror("socket");
        exit(1);
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(9000);

    int opt = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    if (bind(listenfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        perror("bind");
        exit(1);
    }

    if (listen(listenfd, SOMAXCONN) == -1) {
        perror("listen");
        exit(1);
    }

    g_loop = new eventloop();

    channel* listen_ch = new channel(listenfd);
    listen_ch->setreadevents();
    listen_ch->setreadcallback([listen_ch]() {
        handle_accept(listen_ch);
    });

    g_loop->updatechannel(listen_ch);

    g_loop->loop();

    delete listen_ch;
    delete g_loop;
    close(listenfd);
    return 0;
}
