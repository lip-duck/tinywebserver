#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_BUFFER 128
#define MAX_EPOLLSIZE 65536
#define MAX_PORT 1             // 【修改】只连一个端口
#define MAX_CONNECTIONS 60000  // 【修改】先测 5000 连接

#define TIME_SUB_MS(tv1, tv2) ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

int isContinue = 0;

static int ntySetNonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    flags |= O_NONBLOCK;
    return fcntl(fd, F_SETFL, flags);
}

int main(int argc, char** argv) {
    if (argc <= 2) {
        printf("Usage: %s ip port\n", argv[0]);
        exit(0);
    }

    const char* ip = argv[1];
    int port = atoi(argv[2]);
    int connections = 0;
    char buffer[128] = {0};
    int i = 0, index = 0;

    struct epoll_event events[MAX_EPOLLSIZE];
    int epoll_fd = epoll_create(MAX_EPOLLSIZE);

    strcpy(buffer, " Data From MulClient\n");

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(struct sockaddr_in));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(ip);

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    while (1) {
        if (++index >= MAX_PORT) index = 0;

        struct epoll_event ev;
        int sockfd = 0;

        if (connections < MAX_CONNECTIONS && !isContinue) {
            sockfd = socket(AF_INET, SOCK_STREAM, 0);
            if (sockfd == -1) {
                perror("socket");
                goto err;
            }

            addr.sin_port = htons(port + index);

            if (connect(sockfd, (struct sockaddr*)&addr, sizeof(struct sockaddr_in)) < 0) {
                perror("connect");
                goto err;
            }
            ntySetNonblock(sockfd);

            sprintf(buffer, "Hello Server: client --> %d\n", connections);
            send(sockfd, buffer, strlen(buffer), 0);

            ev.data.fd = sockfd;
            ev.events = EPOLLIN | EPOLLOUT;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sockfd, &ev);

            connections++;
        }

        if (connections % 1000 == 999 || connections >= MAX_CONNECTIONS) {
            struct timeval tv_cur;
            memcpy(&tv_cur, &tv_begin, sizeof(struct timeval));
            gettimeofday(&tv_begin, NULL);

            int time_used = TIME_SUB_MS(tv_begin, tv_cur);
            printf("connections: %d, sockfd:%d, time_used:%d ms\n", connections, sockfd, time_used);

            int nfds = epoll_wait(epoll_fd, events, connections, 100);
            for (i = 0; i < nfds; i++) {
                int clientfd = events[i].data.fd;

                if (events[i].events & EPOLLOUT) {
                    sprintf(buffer, "data from %d\n", clientfd);
                    send(clientfd, buffer, strlen(buffer), 0);
                } else if (events[i].events & EPOLLIN) {
                    char rBuffer[MAX_BUFFER] = {0};
                    ssize_t length = recv(clientfd, rBuffer, MAX_BUFFER, 0);
                    if (length > 0) {
                        // 收到服务器回显，不打印（太多了）
                    } else if (length == 0) {
                        printf(" Disconnect clientfd:%d\n", clientfd);
                        connections--;
                        close(clientfd);
                    } else {
                        if (errno == EINTR) continue;
                        close(clientfd);
                    }
                } else {
                    close(clientfd);
                }
            }
            if (connections >= MAX_CONNECTIONS) {
                printf("Reached max connections: %d\n", connections);
                break;
            }
        }

        usleep(500);  // 每 500 微秒建一个连接，避免瞬间打满
    }

    return 0;

err:
    printf("error : %s\n", strerror(errno));
    // usleep(5000000);
    return 0;
}
