#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>       //非阻塞socket
#include <netinet/in.h>  //sockaddr_in
#include <poll.h>        //poll
#include <stdio.h>       //perror
#include <sys/select.h>
#include <sys/socket.h>  //socket
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <thread>

#define max_fds 1024
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
    std::cout << "poll服务器启动，监听 9000 端口" << std::endl;

    struct pollfd fds[max_fds];
    for (int i = 0; i < max_fds; i++) {
        fds[i].fd = -1;
    }
    fds[0].fd = listenfd;
    fds[0].events = POLLIN;
    int max_index = 0;

    while (1) {
        int nready = poll(fds, max_index + 1, -1);
        if (nready == -1) {
            perror("poll");
            break;
        }

        if (fds[0].revents & POLLIN) {
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
            std::cout << "新的连接" << std::endl;
            std::cout << "客户端ip： " << inet_ntoa(client_addr.sin_addr) << std::endl;
            std::cout << "端口： " << ntohs(client_addr.sin_port) << std::endl;

            int slot = -1;
            for (int j = 1; j < max_fds; j++) {
                if (fds[j].fd == -1) {
                    slot = j;
                    break;
                }
            }
            if (slot == -1) {
                std::cerr << "too many clients" << std::endl;
                close(connfd);
                continue;
            }
            fds[slot].fd = connfd;
            fds[slot].events = POLLIN;
            if (slot > max_index) max_index = slot;
        }

        for (int i = 1; i <= max_index; i++) {
            if (fds[i].revents & POLLIN) {
                char buf[1024];
                while (1) {
                    memset(buf, 0, sizeof(buf));
                    int n = recv(fds[i].fd, buf, sizeof(buf), 0);
                    if (n == 0) {
                        struct sockaddr_in peer_addr;
                        socklen_t peer_len = sizeof(peer_addr);
                        getpeername(fds[i].fd, (struct sockaddr*)&peer_addr, &peer_len);
                        std::cout << "断开客户端fd： " << fds[i].fd << std::endl;
                        std::cout << "断开客户端ip： " << inet_ntoa(peer_addr.sin_addr) << std::endl;
                        std::cout << "断开客户端端口： " << ntohs(peer_addr.sin_port) << std::endl;
                        // 必须在 close() 之前调用
                        int temp_fd = fds[i].fd;  // 保存要关闭的文件描述符
                        close(temp_fd);
                        fds[i].fd = -1;
                        break;
                    } else if (n < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            std::cout << "读完了" << std::endl;
                            break;  // 读完了
                        }
                        perror("recv");
                        int temp_fd = fds[i].fd;  // 保存要关闭的文件描述符
                        close(temp_fd);
                        fds[i].fd = -1;
                        break;
                    }
                    send(fds[i].fd, buf, n, 0);
                    std::cout << "fd=" << fds[i].fd << " 收到：" << buf << std::endl;
                }
            }
        }
    }
    close(listenfd);
    return 0;
}