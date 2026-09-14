#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <vector>

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

// 已连接fd的读处理函数
void _read(channel* ch) {
    int fd = ch->getfd();
    char buf[1024];
    while (1) {
        memset(buf, 0, sizeof(buf));
        int n = recv(fd, buf, sizeof(buf), 0);
        if (n == 0) {
            g_loop->deletechannel(ch);
            close(fd);
            delete ch;
            break;
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                perror("recv");
                g_loop->deletechannel(ch);
                close(fd);
                delete ch;
                break;
            }
        } else {
            send(fd, buf, n, 0);
            std::cout << "收到数据: " << buf << std::endl;
        }
    }
}
// listenfd的accept处理函数
void _accept(channel* ch) {
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
        _read(client_ch);
    });

    g_loop->updatechannel(client_ch);
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
        _accept(listen_ch);
    });

    g_loop->updatechannel(listen_ch);

    g_loop->loop();

    delete listen_ch;
    delete g_loop;
    close(listenfd);
    return 0;
}