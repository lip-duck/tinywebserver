#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>       //非阻塞socket
#include <netinet/in.h>  //sockaddr_in
#include <poll.h>        //poll
#include <stdio.h>       //perror
#include <sys/epoll.h>
#include <sys/select.h>
#include <sys/socket.h>  //socket
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <thread>

#define max_events 1024
int main() {
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd == -1) {
        perror("socket");
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(9000);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(listenfd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("bind");
        return 1;
    }
    if (listen(listenfd, 5) == -1) {
        perror("listen");
        return 1;
    }
    //std::cout << "epoll服务器启动，监听 9000 端口" << std::endl;

    int epfd = epoll_create(1);
    if (epfd == -1) {
        perror("epoll_create");
        return 1;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = listenfd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev);

    struct epoll_event events[max_events];

    while (1) {
        int nready = epoll_wait(epfd, events, max_events, -1);
        if (nready == -1) {
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < nready; i++) {
            if (events[i].data.fd == listenfd) {
                struct sockaddr_in client_addr;
                socklen_t len = sizeof(client_addr);

                int connfd = accept(listenfd, (struct sockaddr*)&client_addr, &len);  // 阻塞点1：accept，没有客户端连接就一直卡住
                if (connfd == -1) {
                    perror("accept");
                    continue;
                }
                int flags = fcntl(connfd, F_GETFL, 0);
                flags |= O_NONBLOCK;
                int ret = fcntl(connfd, F_SETFL, flags);
                if (ret == -1) {
                    perror("fcntl F_SETFL");
                }
                //std::cout << "新的连接" << std::endl;
                //std::cout << "客户端ip： " << inet_ntoa(client_addr.sin_addr) << std::endl;
                //std::cout << "端口： " << ntohs(client_addr.sin_port) << std::endl;

                struct epoll_event client_ev;
                client_ev.events = EPOLLIN | EPOLLET;  // 边缘触发
                client_ev.data.fd = connfd;
                epoll_ctl(epfd, EPOLL_CTL_ADD, connfd, &client_ev);
            } else if (events[i].events & EPOLLIN) {
                char buf[1024];
                while (1) {
                    memset(buf, 0, sizeof(buf));
                    int n = recv(events[i].data.fd, buf, sizeof(buf), 0);
                    if (n == 0) {
                        struct sockaddr_in peer_addr;
                        socklen_t peer_len = sizeof(peer_addr);
                        getpeername(events[i].data.fd, (struct sockaddr*)&peer_addr, &peer_len);
                        //std::cout << "断开客户端fd： " << events[i].data.fd << std::endl;
                        //std::cout << "断开客户端ip： " << inet_ntoa(peer_addr.sin_addr) << std::endl;
                        //std::cout << "断开客户端端口： " << ntohs(peer_addr.sin_port) << std::endl;
                        // 必须在 close() 之前调用
                        epoll_ctl(epfd, EPOLL_CTL_DEL, events[i].data.fd, nullptr);
                        close(events[i].data.fd);
                        break;
                    } else if (n < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            //std::cout << "读完了" << std::endl;
                            break;  // 读完了
                        }
                        perror("recv");
                        epoll_ctl(epfd, EPOLL_CTL_DEL, events[i].data.fd, nullptr);
                        close(events[i].data.fd);
                        break;
                    }
                    send(events[i].data.fd, buf, n, 0);
                    //std::cout << "fd=" << events[i].data.fd << " 收到：" << buf << std::endl;
                }
            }
        }
    }
    close(listenfd);
    close(epfd);
    return 0;
}