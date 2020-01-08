/*
This is a socket client program, design to test socket server to ensure its data are correctly received. The socket server is designed to 
generate streaming data for test needed on Spark streaming application.


George Jen, Jen Tek LLC



*/


#include <iostream>
#include <stdio.h>
#include <stdlib.h>
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
       ipaddr=argv[1];
       reply=sock_client(ipaddr,port);
       return *reply;
}


