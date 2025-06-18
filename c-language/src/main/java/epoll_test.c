#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/epoll.h>

#define PORT 9090
#define MAX_EVENTS 10
#define BUFFER_SIZE 1024

// 设置文件描述符为非阻塞
int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int main() {
    int listen_fd, conn_fd, epoll_fd, nfds;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);
    struct epoll_event ev, events[MAX_EVENTS];
    char buffer[BUFFER_SIZE];

    // 1. 创建 socket
    if ((listen_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket error");
        exit(1);
    }

    // 设置为非阻塞
    set_nonblocking(listen_fd);

    // 2. 绑定地址
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind error");
        exit(1);
    }

    // 3. 监听
    if (listen(listen_fd, SOMAXCONN) < 0) {
        perror("listen error");
        exit(1);
    }

    // 4. 创建 epoll
    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1 error");
        exit(1);
    }

    // 5. 注册监听 socket 到 epoll
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev);

    printf("Epoll TCP Server running on port %d...\n", PORT);

    // 6. epoll 循环
    while (1) {
        nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (nfds < 0) {
            perror("epoll_wait error");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == listen_fd) {
                // 新连接
                conn_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);
                if (conn_fd < 0) {
                    perror("accept error");
                    continue;
                }
                printf("Accepted new connection: fd %d\n", conn_fd);
                set_nonblocking(conn_fd);
                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = conn_fd;
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_fd, &ev);
            } else {
                // 可读事件
                int client_fd = events[i].data.fd;
                int n = read(client_fd, buffer, BUFFER_SIZE);
                if (n <= 0) {
                    // 连接关闭或错误
                    close(client_fd);
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, NULL);
                    printf("Closed connection: fd %d\n", client_fd);
                } else {
                    // 回显
                    write(client_fd, buffer, n);
                }
            }
        }
    }

    close(listen_fd);
    close(epoll_fd);
    return 0;
}
