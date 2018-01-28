import time
import os
import random
import socket
from thread import *
import threading
import sys




theDict={}
names={}
my_pid=0
client_counter=1

reply_counter=0
polled = False
my_port = 3021
my_ip = socket.gethostbyname(socket.gethostname())

my_name=''
Queue=[]
receive_counter={}
channel={}
flag={}
saved_money={}

conn_counter=0
recv_counter=0
disconnected = []
ports = 3020
connected = []
BallotNum = [0,os.getpid()]
AcceptNum = [0,0]
AcceptVal = None
MyVal = None
conn_sockets={}
recv_sockets={}
Server = 0
Majority = len(disconnected)/2
ack_counter = 0
ack_queue = {}
Log = []
accept_counter = 0
Leader_ip = None
client_data = ''
Num_of_tickets = 100
log_data = ''
Logs_update=0
is_server = 0



def config():
    global my_pid
    global my_port
    global client_counter
    global my_name
    print "Configuration phase"
    f=open("config.json","r")
    line=''
    ip=''
    port=''
    for line in f:
        ip,port=line.strip().split(' ')
        disconnected.append(ip)
        client_counter=client_counter+1
    f.close()

    return (ip,port)




def tcp_wait():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((my_ip,my_port))
    sock.listen(1)
    global recv_counter
    global theDict
    global recv_sockets
    while True:
        conn, addr = sock.accept()
        t=threading.Thread(target=clientthread,args=(conn,addr,recv_counter))
        t.start()

        recv_counter=recv_counter+1

    return




def clientthread(conn,addr,conn_counter):
    global theDict
    global recv_sockets
    conn.send('I got a connection')
    recv_sockets[addr[0]]=conn
    data=conn.recv(1024)

    return



def tcp_connect(ip,port):
    global theDict
    global conn_sockets
    global conn_counter
    global connected
    global disconnected
    sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.connect((ip,int(port)))
    data=sock.recv(1024)
    connected.append(ip)
    disconnected.remove(ip)
    conn_sockets[ip]=sock
    conn_counter=conn_counter+1
    Majority = len(connected)/2
    print 'Connected to ',ip,' : ',port
    sock.send('I am connecting to you')
    return



def try_connections():
    global my_port
    global disconnected
    global ports
    
    while True:
        for i in range(len(disconnected)):
            try:
                if(my_ip == disconnected[i]):
                    disconnected.pop(i)
                else:
                    try:
                        time.sleep(2)
                        tcp_connect(disconnected[i],my_port)
                    except:
                        pass
            except:
                pass



def tcp_send(msg,socket_num):
    if socket_num=='all':
        for i in range(len(connected)):
            conn_sockets[connected[i]].send(msg)
    else:
            conn_sockets[connected[socket_num]].send(msg)
    return


def tcp_recv():
    global recv_sockets
    global disconnected
    global Queue
    global BallotNum
    global AcceptNum
    global AcceptVal
    global data
    global Queue
    global bal
    global val
    global ack_counter
    global ack_queue
    global Log
    global accept_counter
    global Leader_ip
    global client_data
    global Majority
    global Num_of_tickets
    global log_data
    global Logs_update
    while True:
        try:
            for i in range(len(recv_sockets)):
                try:
                    recv_sockets[connected[i]].settimeout(0)
                    data = recv_sockets[connected[i]].recv(1024)
                    if not data:
                        print connected[i],' DISCONNECTED'
                        disconnected.append(connected.pop(i))
                        Majority = len(connected)/2
                    print data
                    keyword = data.split(':')[0]
                    
                    if('heartbeat' in keyword):
                        try:
                            Leader_ip = i
                            Queue.append(keyword)
                            if(Server == 0 and Logs_update == 0):
                                ask_leader_logs()
                                Logs_update=1
                        except Exception as e:
                            pass
                        
                    elif('Client' in keyword):
                        try:
                            client_data = data.split(':')[1]
                            if(client_message is not ''):
                                clie= threading.Thread(target=client_message)
                                #clie.daemon=True
                                clie.start()
                        except Exception as e:
                            pass
                    elif('prepare' in keyword):
                        try:
                            Queue.append('heartbeat')
                            bal = eval(data.split(':')[1])
                            if(bal[0]>=BallotNum[0]):
                                BallotNum[0]=bal[0]
                                tcp_send("ack:"+str(bal)+':'+str(AcceptNum)+':'+str(AcceptVal),Leader_ip)
                                print 'Sent ack to leader ,',connected[Leader_ip]
                                
                        except Exception as e:
                            pass
                    elif('ack' in keyword):
                        try:
                            ack_counter = ack_counter + 1
                        except Exception as e:
                            pass
                                
                    elif('accept1' in keyword):
                        try:
                            bal = eval(data.split(':')[1])
                            if(bal[0]>=BallotNum[0]):
                                AcceptNum = eval(data.split(':')[1])
                                AcceptVal = data.split(':')[2]
                                tcp_send("accept_client:"+str(AcceptNum)+':'+ str(AcceptVal),i)
                        except Exception as e:
                            pass

                        
                    elif('accept_final' in keyword):
                        Queue.append('heartbeat')
                        Log.append(data.split(':')[2])
                        fd = open('Logs.txt','a+')
                        fd.write(data.split(':')[2]+'\n')
                        fd.close()
                        print 'LOG ::: ',Log
                        if(data.split(':')[2].isdigit()):
                            Num_of_tickets = int(data.split(':')[3])
                            print '-------------------------------------------'
                            print 'NUMBER OF TICKETS TO BUY: ', Num_of_tickets
                            if(int(Num_of_tickets) > int(data.split(':')[2])):
                                print 'SOLD TICKETS:' + data.split(':')[2] 
                                print 'NUMBER OF TICKETS LEFT:' ,str(Num_of_tickets)
                                print '-------------------------------------------'
                                print ''
                            else:
                                print '-------------------------------------------'
                                print 'SHORTAGE OF TICKETS, TRY A SMALLER NUMBER'
                                print '-------------------------------------------'
                                print ''
                                
                    elif('accept_client' in keyword):
                        try:
                            accept_counter = accept_counter + 1
                        except Exception as e:
                            pass
                    elif('send_logs' in keyword):
                        try:
                            fd = open('Logs.txt','a+')
                            count = int(data.split(':')[1])
                            temp=1
                            log_data_new = ''
                            for line in fd:
                                if(temp < count):
                                    temp = temp + 1
                                    continue
                                else:
                                    log_data_new = log_data_new+line
                            tcp_send('append_logs:'+log_data_new,i)
                        except Exception as e:
                            pass
                    elif('append_logs' in keyword):
                        try:
                            log_data = data.split(':')[1]
                            append_logs()
                        except Exception as e:
                            pass

                except Exception as e:
                    pass
                    
        except Exception as e:
            pass


def send_heartbeat():
    while True:
        if(Server == 1):
            time.sleep(1)
            tcp_send('heartbeat:'+str(Num_of_tickets),'all')

def start_election():
    global BallotNum
    BallotNum[0]=BallotNum[0]+1
    BallotNum[1]=os.getpid()
    tcp_send('prepare:'+str(BallotNum),'all')


def ask_leader_logs():
    fd=open('Logs.txt','a+')
    line_count = 0
    for line in fd:
        line_count = line_count + 1
    tcp_send('send_logs:'+str(line_count+1),Leader_ip)
    fd.close()

def append_logs():
    global log_data
    global Log
    fd=open('Logs.txt','a+')
    fd.write(log_data)
    fd.seek(0)
    for line in fd:
        Log.append(line.strip('\n'))
    print 'LOG ::: ',Log
    fd.close()


def client_message():
    global client_data
    global Leader_ip
    global Server
    global Queue
    global max_val
    global max_ballot
    global ack_queue
    global Log
    global ack_counter
    global accept_counter
    global BallotNum
    global Majority
    global Num_of_tickets
    try:
        max_val = None
        max_ballot = None
        BallotNum[0] = BallotNum[0]+1
        message = 'prepare:' + str(BallotNum)
        tcp_send(message,'all')
        while(ack_counter<=Majority):
            pass
        if(ack_counter>Majority):
            tcp_send('accept1:'+str(BallotNum)+':'+str(client_data),'all')
            ack_counter = 0
            ack_queue = {}
            while(accept_counter<=Majority):
                pass
            accept_counter = 0
            Log.append(client_data)
            fd = open('Logs.txt','a+')
            fd.write(client_data+'\n')
            fd.close()
            print '-------------------------------------------'
            print 'LOG ::: ',Log
            print '-------------------------------------------'
            if(client_data.isdigit()):
                if(Num_of_tickets > 0):
                    if(Num_of_tickets-int(client_data)>0):
                        Num_of_tickets = Num_of_tickets-int(client_data)
                        print '-------------------------------------------'
                        print 'SOLD TICKETS ' ,client_data
                        print 'NUMBER OF TICKETS LEFT:' ,str(Num_of_tickets)
                        print '-------------------------------------------'
                        print ''
                    else:
                        print '-------------------------------------------'
                        print 'SHORTAGE OF TICKETS, TRY A SMALLER NUMBER'
                        print '-------------------------------------------'
                        print ''
            tcp_send('accept_final:'+str(BallotNum)+':'+str(client_data)+':'+ str(Num_of_tickets),'all')
    except Exception as e:
        pass

def check_timeout_new():
    global Server
    global Queue
    global max_val
    global max_ballot
    global ack_queue
    global Log
    global is_server
    while True:
        time.sleep(5+random.randint(5,7))
        if(Server == 0):
            if(len(Queue) == 0):
                for i in range(len(connected)):
                    if my_ip > connected[i]:
                        print "I am the leader"
                        Server = 1
        Queue = []





config()
s=threading.Thread(target = try_connections)
s.daemon = True
s.start()

c=threading.Thread(target = tcp_wait)
c.daemon = True
c.start()

r=threading.Thread(target = tcp_recv)  
r.daemon = True
r.start()

to= threading.Thread(target = check_timeout_new)
to.daemon = True
to.start()

heart= threading.Thread(target = send_heartbeat)
heart.daemon = True
heart.start()

def get_input():
    print '-------------------------------------------'
    print 'ENTER THE NUMBER OF TICKETS TO BUY'
    client_data = raw_input()
    print ''
    print ''
    if len(client_data) > 0:
        if(Server == 0 ):
            tcp_send("Client:" + client_data,Leader_ip)
        else:
            client_message()

    return


                

while True:
    try:
        time.sleep(1)
        get_input()
    except KeyboardInterrupt:
        sys.exit(0)
    except:
        print 'SOMETHING WENT WRONG, PLEASE TRY AGAIN'
        print ''


