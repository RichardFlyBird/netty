#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT 9090
#define BUFFER_SIZE 1024

int main()
{
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    char buffer[BUFFER_SIZE];
    socklen_t client_len = sizeof(client_addr);

    // 1. 创建 socket（IPv4 + TCP）
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // 2. 设置服务器地址结构体
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;         // IPv4
    server_addr.sin_addr.s_addr = INADDR_ANY; // 本机任意 IP
    server_addr.sin_port = htons(PORT);       // 端口转换为网络字节序

    // 3. 绑定 socket 到 IP 和端口
    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        perror("bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // 4. 监听端口
    if (listen(server_fd, 5) < 0)
    {
        perror("listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    printf("Blocking TCP Server is listening on port %d...\n", PORT);

    // 5. 阻塞等待客户端连接
    while (1)
    {
        client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0)
        {
            perror("accept failed");
            continue;
        }

        printf("Client connected: %s:%d\n",
               inet_ntoa(client_addr.sin_addr),
               ntohs(client_addr.sin_port));

        // 6. 接收和响应客户端数据（阻塞读取）
        ssize_t n;
        while ((n = read(client_fd, buffer, BUFFER_SIZE)) > 0)
        {
            buffer[n] = '\0'; // 添加字符串结束符
            printf("Received: %s\n", buffer);

            // 回显给客户端
            write(client_fd, buffer, n);
        }

        if (n == 0)
        {
            printf("Client disconnected.\n");
        }
        else
        {
            perror("read failed");
        }

        close(client_fd);
    }

    close(server_fd);
    return 0;
}
