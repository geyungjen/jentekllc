/*
This is a socket client program, design to test socket server to ensure its data are correctly received. The socket server is designed to
generate streaming data for test needed on Spark streaming application.


George Jen, Jen Tek LLC



*/


#include<stdio.h> 
#include <stdlib.h>
#include<string.h>    
#include<sys/socket.h>    
#include<arpa/inet.h> 

int *sock_client(char* server_ipaddr , int server_port)
{
    

    int sock, i;
    struct sockaddr_in server;
    char  server_reply[2000];
    char ip_addr[300];
    int port=29999;
    int ret[] = {0, 1, 2};
    int *ret0 = &ret[1];
    int *ret1 = &ret[2];   
    int *ret2 = &ret[3];   

    printf("ipaddress %s\n",server_ipaddr);
    printf("port %d\n",server_port);

    sock = socket(AF_INET , SOCK_STREAM , 0);
    if (sock == -1)
    {
        printf("Could not create socket");
        return ret1;
    }

    server.sin_addr.s_addr = inet_addr(server_ipaddr);
    server.sin_family = AF_INET;
    server.sin_port = htons(server_port);

    //Connect to remote server
    if (connect(sock , (struct sockaddr *)&server , sizeof(server)) < 0)
    {
        perror("connect failed. Error");
        return ret1;

    }

    while (1)
   {

        //Receive a reply from the server
        if( recv(sock , server_reply , 2000 , 0) < 0)
        {
            puts("recv failed");
            return ret1;
        }
     char *reply = malloc(sizeof(server_reply));  // allocate memory from the heap
     memcpy(reply, server_reply, sizeof(server_reply));
     printf("%s\n",reply);
     free(reply);
     sleep(1);
}

    close(sock);
    return ret0;

}


