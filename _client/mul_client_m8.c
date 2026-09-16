// mul_client_m8.c —— m8 百万连接压测客户端
//
// 用法: ./mul_client_m8 <ip> <base_port> [port_count] [total_conns] [rate_per_sec] [src_ip_count]
// 例子:
//   ./mul_client_m8 127.0.0.1 9000            默认连 9000 一个端口，60000 连接（复现第二版场景）
//   ./mul_client_m8 127.0.0.1 9000 20 1200000 连 9000~9019 共 20 个端口，目标 120 万连接
//   ./mul_client_m8 127.0.0.1 9000 1 60000 20000 3   单端口 + 3 个回环源 IP
//
// 和旧版 mul_client.c 的区别（旧版保留不动，用于第二版笔记的对照）:
//   1. 非阻塞 connect + EPOLLOUT 判定握手完成，不会因为服务器忙而卡死主流程
//   2. 支持连多个端口：单客户端 IP 对单个服务器端口最多 ~64000 连接（源端口 16 位），
//      要上百万必须让服务器开多个监听端口，客户端分散去连
//   3. 【新增】支持多个回环源 IP（127.0.0.1~127.0.0.N）：
//      TCP 五元组 = (源IP,源端口,目的IP,目的端口)，换源 IP 就能扩展源端口空间。
//      WSL 重启后 ip_local_port_range 会打回默认 32768~60999（约 2.8 万个），
//      单端口要打到 6 万就用 3 个源 IP，不依赖 sysctl
//   4. 定速建连（默认每秒 2 万个），不再 usleep(500)
//   5. 连接成功后只订阅 EPOLLIN；keepalive 按固定间隔低频发送（默认 20 秒一轮），
//      旧版把 EPOLLOUT 常驻 + 每次扫描向所有连接群发，等于自己制造流量风暴
//   6. signal(SIGPIPE, SIG_IGN)：服务器踢掉连接时 send 不会再把进程杀掉
//   7. 到达目标后不退出，保持连接并每 5 秒打印一次在线数，Ctrl+C 退出
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define MAX_EVENTS 65536
#define HELLO_MSG "Hello Server: client from mul_client_m8\n"
#define KEEPALIVE_MSG "k\n"

// IP_BIND_ADDRESS_NO_PORT：bind 只绑源 IP 不预分配端口，端口延迟到 connect 时
// 按"源IP,源端口,目的IP,目的端口"四元组分配。没有它，bind 会提前全局占用源端口，
// 同一源 IP 的端口不能跨目的地复用（8 IP × 2.8 万 ≈ 22.5 万连接就是天花板），
// 而且 bind 内核扫描随连接数变慢——这是百万连接压测的标准配置（nginx/wrk 同款）
#ifndef IP_BIND_ADDRESS_NO_PORT
#define IP_BIND_ADDRESS_NO_PORT 24
#endif

#define TIME_SUB_MS(tv1, tv2) ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

static int g_epoll_fd;
static const char* g_ip;
static int g_base_port;
static int g_port_count = 1;
static long g_total_target = 60000;
static long g_rate_per_sec = 20000;
static int g_src_ip_count = 1;  // 回环源 IP 数量：127.0.0.1 ~ 127.0.g_src_ip_count

// EADDRNOTAVAIL 熔断：连续失败太多次说明源端口耗尽，停止新建，保住现有连接
static int g_consecutive_eaddr = 0;

static long g_alive = 0;        // 已建立且未断开的连接
static long g_connecting = 0;   // 握手中的连接
static long g_created = 0;      // 累计发起的连接
static long g_failed = 0;       // 累计失败
static long g_kicked = 0;       // 被服务器关闭的连接

// 已建连的 fd 列表，keepalive 轮询用；断开的置 -1
static int* g_fdlist = NULL;
static long g_fdlist_cap = 0;
static long g_fdlist_len = 0;
static long g_ka_cursor = 0;

// fd -> 是否处于握手中（EPOLLERR/EPOLLHUP 时用它准确区分计入哪个计数器）
static unsigned char* g_conn_mark = NULL;
static long g_conn_mark_cap = 0;

// fd -> 在 fdlist 里的下标（drop_conn 用它把对应槽位标成 -1，防止 stale fd 被重复处理）
static long* g_fdslot = NULL;

static void mark_connecting(int fd, int v) {
    if (fd >= 0 && fd < g_conn_mark_cap) g_conn_mark[fd] = (unsigned char)v;
}
static int is_connecting(int fd) {
    return (fd >= 0 && fd < g_conn_mark_cap) ? g_conn_mark[fd] : 0;
}

static volatile sig_atomic_t g_stop = 0;

static void on_int(int sig) { (void)sig; g_stop = 1; }

static long now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static void set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void fdlist_push(int fd) {
    if (g_fdlist_len == g_fdlist_cap) {
        long newcap = g_fdlist_cap ? g_fdlist_cap * 2 : 65536;
        int* p = realloc(g_fdlist, newcap * sizeof(int));
        if (!p) { perror("realloc"); exit(1); }
        g_fdlist = p;
        g_fdlist_cap = newcap;
    }
    if (fd >= 0 && fd < g_conn_mark_cap) {
        g_fdslot[fd] = g_fdlist_len;  // 记下这个 fd 在列表里的位置
    }
    g_fdlist[g_fdlist_len++] = fd;
}

static void drop_conn(int fd) {
    // 幂等：同一个 fd 只处理一次（防止 stale fd 二次扣减 alive 计数）
    if (fd < 0 || fd >= g_conn_mark_cap || g_fdslot[fd] == -1) {
        return;
    }
    epoll_ctl(g_epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    long pos = g_fdslot[fd];
    if (pos >= 0 && pos < g_fdlist_len && g_fdlist[pos] == fd) {
        g_fdlist[pos] = -1;
    }
    g_fdslot[fd] = -1;
    g_alive--;
    g_kicked++;
}

// 发起一个非阻塞 connect
static void start_one_connect(void) {
    int port_index = (int)(g_created % g_port_count);
    int port = g_base_port + port_index;

    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd == -1) {
        // 一般是 fd 上限不够：ulimit -n / fs.nr_open
        if (errno == EMFILE || errno == ENFILE) {
            fprintf(stderr, "socket: %s —— fd 不够了，调大 ulimit -n 和 fs.nr_open\n", strerror(errno));
            g_stop = 1;
        }
        g_failed++;
        return;
    }

    // 多源 IP：每次绑定不同的 127.0.0.X，扩展五元组空间（不依赖 sysctl）
    if (g_src_ip_count > 1) {
        // SO_REUSEADDR：允许同一(源IP,源端口)用于不同目的端口（双方都设才生效），
        // 配合 NO_PORT 把单 IP 容量从 2.8 万提到 2.8 万 × 目的端口数
        int reuse = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        int ip_index = (int)(g_created % g_src_ip_count);
        struct sockaddr_in src;
        memset(&src, 0, sizeof(src));
        src.sin_family = AF_INET;
        src.sin_addr.s_addr = htonl(0x7F000001 + ip_index);  // 127.0.0.(1+ip_index)
        // 关键：延迟端口分配，否则同一源 IP 的端口无法跨目的端口复用（见文件头注释）
        int one = 1;
        setsockopt(fd, SOL_IP, IP_BIND_ADDRESS_NO_PORT, &one, sizeof(one));
        if (bind(fd, (struct sockaddr*)&src, sizeof(src)) == -1) {
            close(fd);
            g_failed++;
            return;
        }
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(g_ip);
    addr.sin_port = htons(port);

    int ret = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    if (ret == 0) {
        // 本机回环可能立即成功
        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, fd, &ev);
        send(fd, HELLO_MSG, strlen(HELLO_MSG), 0);
        fdlist_push(fd);
        g_alive++;
        g_created++;
        g_consecutive_eaddr = 0;
        return;
    }
    if (errno != EINPROGRESS) {
        // EADDRNOTAVAIL: 源端口耗尽；ECONNREFUSED: 服务器没开
        if (errno == EADDRNOTAVAIL) {
            g_consecutive_eaddr++;
            if (g_consecutive_eaddr == 200) {
                fprintf(stderr,
                        "源端口耗尽（Cannot assign requested address）：%d 个源 IP 的端口空间已用完。\n"
                        "对策：多端口监听 / 多源 IP / 调大 ip_local_port_range（见笔记）。\n"
                        "停止新建，保持现有连接（alive=%ld）。\n",
                        g_src_ip_count, g_alive);
                g_total_target = g_created;  // 冻结建连目标，主循环转入纯保持模式
            }
        } else {
            static long last_report = 0;
            if (now_ms() - last_report > 1000) {
                fprintf(stderr, "connect %s:%d 失败: %s（此后同类错误 1 秒只报一次）\n",
                        g_ip, port, strerror(errno));
                last_report = now_ms();
            }
        }
        close(fd);
        g_failed++;
        return;
    }

    // 握手中，等 EPOLLOUT
    struct epoll_event ev;
    ev.events = EPOLLOUT;
    ev.data.fd = fd;
    epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, fd, &ev);
    mark_connecting(fd, 1);
    g_connecting++;
    g_created++;
}

// 处理握手完成（EPOLLOUT）
static void finish_connect(int fd) {
    int err = 0;
    socklen_t len = sizeof(err);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err != 0) {
        mark_connecting(fd, 0);
        g_connecting--;
        g_failed++;
        close(fd);
        return;
    }
    // 握手完成：改订阅为 EPOLLIN，发一条问候
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    epoll_ctl(g_epoll_fd, EPOLL_CTL_MOD, fd, &ev);
    send(fd, HELLO_MSG, strlen(HELLO_MSG), 0);
    fdlist_push(fd);
    mark_connecting(fd, 0);
    g_alive++;
    g_connecting--;
}

// keepalive：给所有活连接发 "k\n"，防止被服务器空闲超时踢掉
// 100 万连接一轮要发 100 万次 send，一次发完会卡住建连，所以每轮主循环只发一批
static void keepalive_tick(int max_batch) {
    if (g_fdlist_len <= 0) return;
    long total = g_fdlist_len;
    int sent = 0;
    long scanned = 0;
    while (sent < max_batch && scanned < total) {
        // 最多扫完一整圈，避免全是 -1 的空槽时死循环
        long i = g_ka_cursor % g_fdlist_len;
        g_ka_cursor++;
        scanned++;
        int fd = g_fdlist[i];
        if (fd == -1) continue;
        ssize_t n = send(fd, KEEPALIVE_MSG, strlen(KEEPALIVE_MSG), MSG_NOSIGNAL);
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            drop_conn(fd);
        }
        sent++;
    }
}

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: %s <ip> <base_port> [port_count] [total_conns] [rate_per_sec] [src_ip_count]\n", argv[0]);
        return 0;
    }
    g_ip = argv[1];
    g_base_port = atoi(argv[2]);
    if (argc > 3) g_port_count = atoi(argv[3]);
    if (argc > 4) g_total_target = atol(argv[4]);
    if (argc > 5) g_rate_per_sec = atol(argv[5]);
    if (argc > 6) g_src_ip_count = atoi(argv[6]);
    if (g_port_count < 1) g_port_count = 1;
    if (g_src_ip_count < 1) g_src_ip_count = 1;
    if (g_src_ip_count > 250) g_src_ip_count = 250;

    signal(SIGPIPE, SIG_IGN);  // 服务器关掉的连接上 send 不再杀进程
    signal(SIGINT, on_int);

    // 自动把 fd 上限提到硬限制（旧版必须记得先在终端里 ulimit -n）
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        rlim_t need = (rlim_t)g_total_target + 1024;
        if (rl.rlim_cur < need) {
            rlim_t hard = rl.rlim_max;
            if (hard != RLIM_INFINITY && hard < need) {
                printf("[警告] 硬限制 %lu 小于需要的 %lu，请改 limits.conf 和 fs.nr_open\n",
                       (unsigned long)hard, (unsigned long)need);
            }
            rl.rlim_cur = (hard == RLIM_INFINITY || hard > need) ? need : hard;
            if (setrlimit(RLIMIT_NOFILE, &rl) == 0) {
                printf("RLIMIT_NOFILE 提到 %lu\n", (unsigned long)rl.rlim_cur);
            } else {
                perror("setrlimit");
            }
        }
    }

    g_epoll_fd = epoll_create1(0);
    if (g_epoll_fd == -1) {
        perror("epoll_create");
        return 1;
    }

    // 握手标记数组：按 fd 直接索引
    g_conn_mark_cap = g_total_target + 8192;
    g_conn_mark = calloc(g_conn_mark_cap, 1);
    if (!g_conn_mark) {
        perror("calloc");
        return 1;
    }
    // fd -> fdlist 下标数组，初始化为 -1（0xFF）
    g_fdslot = malloc(g_conn_mark_cap * sizeof(long));
    if (!g_fdslot) {
        perror("malloc");
        return 1;
    }
    memset(g_fdslot, 0xFF, g_conn_mark_cap * sizeof(long));

    printf("目标: %s 端口 %d~%d, 共 %ld 连接, 速率 %ld/秒\n",
           g_ip, g_base_port, g_base_port + g_port_count - 1, g_total_target, g_rate_per_sec);

    long last_progress = 0;      // 上次打印进度时的 alive 数
    long keepalive_every_ms = 10000;  // keepalive 间隔；大批量时每轮只发一部分，间隔要留足余量（服务器超时 60s）
    long next_keepalive_ms = now_ms() + 1000;
    long next_status_ms = now_ms() + 5000;
    long rate_window_start = now_ms();
    long rate_budget = 0;
    long build_start = now_ms();

    struct epoll_event events[MAX_EVENTS];

    while (!g_stop) {
        int nfds = epoll_wait(g_epoll_fd, events, MAX_EVENTS, 100);
        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;
            uint32_t re = events[i].events;
            if (re & (EPOLLERR | EPOLLHUP)) {
                if (is_connecting(fd)) {
                    epoll_ctl(g_epoll_fd, EPOLL_CTL_DEL, fd, NULL);
                    mark_connecting(fd, 0);
                    close(fd);
                    g_connecting--;
                    g_failed++;
                } else {
                    drop_conn(fd);
                }
                continue;
            }
            if (re & EPOLLOUT) {
                finish_connect(fd);
            }
            if (re & EPOLLIN) {
                // 服务器一般不回数据；收到 0 说明被关了
                char buf[64];
                ssize_t n = recv(fd, buf, sizeof(buf), 0);
                if (n == 0) {
                    drop_conn(fd);
                } else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                    drop_conn(fd);
                }
            }
        }

        // 定速建连
        long now = now_ms();
        long elapsed = now - rate_window_start;
        if (elapsed >= 100) {
            rate_budget += g_rate_per_sec * elapsed / 1000;
            rate_window_start = now;
            long cap = g_rate_per_sec / 2;  // 预算最多攒半秒的量，防止突发
            if (rate_budget > cap) rate_budget = cap;
        }
        while (g_created < g_total_target && rate_budget > 0) {
            start_one_connect();
            rate_budget--;
        }

        // keepalive（一批一批发，不阻塞建连）
        if (now >= next_keepalive_ms) {
            keepalive_tick(200000);
            next_keepalive_ms = now + keepalive_every_ms;
        }

        // 进度打印：每 1000 个连接一行（保持和旧版一样的格式）
        if (g_alive - last_progress >= 1000) {
            long used = now - build_start;
            printf("connections: %ld, connecting:%ld, failed:%ld, elapsed:%ld.%ld s\n",
                   g_alive, g_connecting, g_failed, used / 1000, (used % 1000) / 100);
            last_progress = g_alive;
        }

        // 状态打印：每 5 秒
        if (now >= next_status_ms) {
            printf("[status] alive:%ld connecting:%ld failed:%ld kicked:%ld\n",
                   g_alive, g_connecting, g_failed, g_kicked);
            next_status_ms = now + 5000;
        }
    }

    printf("\n最终: alive:%ld failed:%ld kicked:%ld\n", g_alive, g_failed, g_kicked);
    printf("连接保持中可 Ctrl+C 退出；用 ss -nat | wc -l 或 ss -s 在服务器侧核对\n");
    return 0;
}
