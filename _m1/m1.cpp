#include<iostream>              
#include<cstring>
#include <unistd.h>
#include<stdio.h>               //perror
#include<sys/socket.h>          //socket
#include<netinet/in.h>          //sockaddr_in
#include<arpa/inet.h>            

int main(){
int listenfd=socket(AF_INET,SOCK_STREAM,0);
if(listenfd==-1){
    perror("socket");
    return -1;
}

struct sockaddr_in addr;
memset(&addr,0,sizeof(addr));
addr.sin_family=AF_INET;
addr.sin_port=htons(9000);
addr.sin_addr.s_addr=htonl(INADDR_ANY);

if(bind(listenfd,(struct sockaddr*)&addr,sizeof(addr))==-1){
    perror("bind");
    return 1;
}
if(listen(listenfd,5)==-1){
    perror("lsiten");
    return 1;
}
std::cout<<"阻塞服务器启动，监听 9000 端口"<<std::endl;

while(1){
struct sockaddr_in client_addr;
socklen_t len=sizeof(client_addr);

int connfd=accept(listenfd,(struct sockaddr*)&client_addr,&len);//阻塞点1：accept，没有客户端连接就一直卡住
if (connfd == -1) { perror("accept"); continue; }
std::cout << "客户端ip： " << inet_ntoa(client_addr.sin_addr) <<std::endl;
std::cout << "端口： " << ntohs(client_addr.sin_port) <<std::endl;

char buf[1024];
while (true) {
            memset(buf, 0, sizeof(buf));
            // 阻塞点2：recv，客户端不发数据就一直卡住
            int n = recv(connfd, buf, sizeof(buf), 0);
            if (n <= 0) break;
            // 阻塞点3：send，发送缓冲区满时可能阻塞
            send(connfd, buf, n, 0);
        }
       close(connfd);
       std::cout << "连接关闭" << std::endl;

}
close(listenfd);
return 0;
}