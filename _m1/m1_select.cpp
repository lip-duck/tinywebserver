#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>       //非阻塞socket
#include <netinet/in.h>  //sockaddr_in
#include <stdio.h>       //perror
#include <sys/select.h>
#include <sys/socket.h>  //socket
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <thread>

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
    std::cout << "select服务器启动，监听 9000 端口" << std::endl;

    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(listenfd, &rfds);
    int maxfd = listenfd;

    while (1) {
        fd_set rset = rfds;

        int nready = select(maxfd + 1, &rset, NULL, NULL, NULL);
        if (nready == -1) {
            perror("select");
            break;
        }

        if (FD_ISSET(listenfd, &rset)) {
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
            FD_SET(connfd, &rfds);
            if (connfd > maxfd) {
                maxfd = connfd;
            }
        }

        for (int fd = listenfd + 1; fd <= maxfd; fd++) {
            if (FD_ISSET(fd, &rset)) {
                char buf[1024];
                while (1) {
                    memset(buf, 0, sizeof(buf));
                    int n = recv(fd, buf, sizeof(buf), 0);
                    if (n == 0) {
                        struct sockaddr_in peer_addr;
                        socklen_t peer_len = sizeof(peer_addr);
                        getpeername(fd, (struct sockaddr*)&peer_addr, &peer_len);
                        // 必须在 close() 之前调用
                        FD_CLR(fd, &rfds);
                        close(fd);
                        std::cout << "断开客户端fd： " << fd << std::endl;
                        std::cout << "断开客户端ip： " << inet_ntoa(peer_addr.sin_addr) << std::endl;
                        std::cout << "断开客户端端口： " << ntohs(peer_addr.sin_port) << std::endl;
                        break;
                    } else if (n < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            std::cout << "读完了" << std::endl;
                            break;  // 读完了
                        }
                        perror("recv");
                        FD_CLR(fd, &rfds);
                        close(fd);
                        break;
                    }
                    send(fd, buf, n, 0);
                    std::cout << "fd=" << fd << " 收到：" << buf << std::endl;
                }
            }
        }
    }
    close(listenfd);
    return 0;
}