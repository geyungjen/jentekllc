'''
This is a socket server program that generates non stop random CSV lines, intended a streaming testing source with Spark streaming
syntax:

python3 sock_server.py <port>

you should choose a port that is free, consider a free port > 20000


George Jen,  Jen Tek LLC

'''

import socket
import sys
import time
import datetime
import random
import argparse

def get_message():
#    datetimestamp=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#    value=random.randint(1,10000000)
#    string="".join(sorted([chr(i) for i in range(ord('a'),ord('a')+26)],key=lambda x: random.randint(1,1000)))
#    return str(datetimestamp)+","+string+","+str(value)+"\n"
#    For testing with Spark streaming word count, use below
     popular_word_list=['a','about','all','also','and','as','at','be','because','but','by','can','come','could','day','do','even','find','first','for','from','get','give','go','have','he','her','here','him','his','how','I','if','in','into','it','its','just','know','like','look','make','man','many','me','more','my','new','no','not','now','of','on','one','only','or','other','our','out','people','say','see','she','so','some','take','tell','than','that','the','their','them','then','there','these','they','thing','think','this','those','time','to','two','up','use','very','want','way','we','well','what','when','which','who','will','with','would','year','you','your']
     return ",".join(random.choices(popular_word_list,k=20))+"\n"


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Streaming generation socket server")
    parser.add_argument("port", type=int, help="port")
    args = parser.parse_args()

    port=args.port

    print(port)

    HOST=""
    PORT=port

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print ('Socket created')

    #Bind socket to local host and port
    try:
      	sock.bind((HOST, PORT))
    except socket.error as msg:
    #    print ('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        print ('Bind failed. Error Code : ' + str(msg))
        sys.exit()
	
    print ('Socket bind complete')

    #Start listening on socket
    sock.listen(10)
    print ('Listening on {}'.format(PORT))

    #now keep sending data to the client
    while True:
    #    #wait to accept a connection - blocking call
        try:
            conn, addr = sock.accept()
            print ('Connected with ' + addr[0] + ':' + str(addr[1]))
        except KeyboardInterrupt:
            print("bye")
            sock.close()
            sys.exit()
        except:
            print("Error when accept connection")
            sock.close()
            sys.exit() 

        while True:
            try:
                conn.send(get_message().encode())
                time.sleep(1)
            except KeyboardInterrupt:
                print("bye")
                break
            except:
                print("Client disconnect")
                break
    sock.close()
print("Done")

