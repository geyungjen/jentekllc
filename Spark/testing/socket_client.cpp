/*
This is a socket client program, design to test socket server to ensure its data are correctly received. The socket server is designed to 
generate streaming data for test needed on Spark streaming application.


George Jen, Jen Tek LLC



*/


#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>




using namespace std;

extern "C" int *sock_client(char s[], int i);

int  main(int argc, char** argv)
{
       int *reply;
       char* ipaddr;
       char *p;

       if (argc < 3)
         {
            cout << "Syntax: sock_client <hostname> <port>"<<endl;
            return 1;
         }

       long port = strtol(argv[2], &p, 10);

       struct hostent *host_entry = gethostbyname(argv[1]);
       ipaddr = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0]));
       if (host_entry)
       {
            cout << host_entry->h_name << endl;
            cout << ipaddr << endl;
        }
        else
            herror("gethostbyname");
       reply=sock_client(ipaddr,port);
       return *reply;
}


