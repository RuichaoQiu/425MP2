import threading
from threading import Thread
import time
import socket, select, string, sys

Bit = 8
CoordinatorPort = 3000
InitPeerPort = 3001

'''for experiment use'''
MsgCnt = 0

class CoordinatorThread:
    def __init__(self,filename):
        global InitPeerPort
        self.filename = filename
        self.Peers = []
        self.CurPort = InitPeerPort
        self.ActionComplete = False

        self.sthread = Thread(target=self.runserver)
        self.sthread.start()
        self.joinPeerThread(0)

        self.cmdqueue=[]
        self.ethread = Thread(target=self.execommand)
        self.ethread.start()

        self.rthread = Thread(target=self.readcommand)
        self.rthread.start()

        self.showallcomplete = True
        self.showallcounter = 0

        self.firstwrite = True

    def execommand(self):
        while 1:
            if self.cmdqueue and self.ActionComplete and self.showallcomplete:
                cmd = self.cmdqueue[0]
                self.cmdqueue.pop(0)
                if cmd.strip()[:4] == "join":
                    self.joinPeerThread(int(cmd.split()[1]))
                if cmd.strip()[:4] == "show":
                    if cmd.split()[1] == "all":
                        self.showall()
                    else:
                        self.ActionComplete = False
                        self.show(int(cmd.split()[1]))
                if cmd.strip()[:5] == "leave":
                    self.leavePeer(int(cmd.split()[1]))
                if cmd.strip()[:4] == "find":
                    self.ActionComplete = False
                    self.findkey(int(cmd.split()[1]),int(cmd.split()[2]))
            time.sleep(0.1)

    def readcommand(self):
        cmd = None
        while 1:
            socket_list = [sys.stdin]
            read_sockets, write_sockets, error_sockets = select.select(socket_list, [], [])
            for sock in read_sockets:
                cmd = sys.stdin.readline()
                self.cmdqueue.append(cmd)
                #print "cached cmd ", cmd
                #print self.cmdqueue

    def joinPeerThread(self, key):
        self.ActionComplete = False
        for item in self.Peers:
            if item[1] == key:
                print "Node %d already exists!" % (key)
                self.ActionComplete = True
                return
        # Create New PeerThread
        PT = PeerThread(key, self.CurPort)
        time.sleep(0.02)
        st = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        st.settimeout(2)
        st.connect(("localhost", self.CurPort))
        self.Peers.append([PT, key, st])
        self.CurPort += 1
        st.send("join")

    def show(self, key):
        for item in self.Peers:
            if item[1] == key:
                item[2].send("show")
                return

    def showall(self):
        self.showallcomplete = False
        self.showallcounter = 0
        self.Peers.sort(key=lambda x:x[1])
        for item in self.Peers:
            while not self.ActionComplete:
                pass
            self.ActionComplete = False
            item[2].send("show")

    def leavePeer(self, key):
        self.ActionComplete = False
        for item in self.Peers:
            if item[1] == key:
                item[2].send("leave")
                self.Peers.remove(item)
                return
        print "Node %d does not exist!" % (key)
        self.ActionComplete = True

    def findkey(self,p,k):
        for item in self.Peers:
            if item[1] == p:
                item[2].send("find %d" % (k))
                return
        print "Node {key} does not exist!".format(key=p)
        self.ActionComplete = True

    def runserver(self):
        global CoordinatorPort
        CONNECTION_LIST = []
        RECV_BUFFER = 4096
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("0.0.0.0", CoordinatorPort))
        server_socket.listen(10)
        CONNECTION_LIST.append(server_socket)
        while 1:
            read_sockets,write_sockets,error_sockets = select.select(CONNECTION_LIST,[],[])
            for sock in read_sockets:
                if sock == server_socket:
                    sockfd, addr = server_socket.accept()
                    CONNECTION_LIST.append(sockfd)
                else:
                    try:
                        data = sock.recv(RECV_BUFFER)
                        #print "Receive "+data
                        if data == "join":
                            print "Join Complete!"
                            #print "Msg cnt: ", MsgCnt
                            self.ActionComplete = True
                        elif data == "leave":
                            print "Leave Complete!"
                            self.ActionComplete = True
                        elif data[:4] == "show":
                            print data[5:]
                            if self.filename:
                                mode = 'a'
                                if self.firstwrite:
                                    mode = 'w'
                                    self.firstwrite = False
                                with open(self.filename, mode) as OUTPUT:
                                    OUTPUT.write("{show_result}\n".format(show_result=data[5:]))
                            self.ActionComplete = True
                            if not self.showallcomplete:
                                self.showallcounter += 1
                                if self.showallcounter == len(self.Peers):
                                    self.showallcomplete = True

                        elif data[:4] == "find":
                            print "Find the key at Node %d" % (int(data.split()[1]))
                            self.ActionComplete = True
                            #print "Msg cnt: ", MsgCnt
                    except:
                        sock.close()
                        CONNECTION_LIST.remove(sock)

        server_socket.close()


class PeerThread:
    def __init__(self,key,port):
        self.stop = False
        self.KeyLocation = key
        self.PORT = port
        self.EventId = 0
        self.EventList = []
        self.mutex = True

        # Set up connection with coordinator server
        global CoordinatorPort
        self.pst = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.pst.settimeout(2)
        self.pst.connect(("localhost", CoordinatorPort))

        global Bit
        # In Finger array, first element represents start key, second element represents port number of first node >= finger[k].start
        # Third element represents key location of the second element
        self.finger = [[0,0,0] for i in xrange(Bit+1)]
        self.predecessor = 0
        self.predLocation = 0
        # Store the keys stored in PeerThread
        self.keys = []

        self.messagequeue = []
        self.sthread = Thread(target=self.runserver)
        self.ethread = Thread(target=self.runexec)
        self.sthread.start()
        self.ethread.start()

    # Receive incoming message and store in messagequeue
    def runserver(self):
        CONNECTION_LIST = []
        RECV_BUFFER = 4096
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("0.0.0.0", self.PORT))
        server_socket.listen(10)
        CONNECTION_LIST.append(server_socket)
        while 1:
            read_sockets,write_sockets,error_sockets = select.select(CONNECTION_LIST,[],[])
            for sock in read_sockets:
                if sock == server_socket:
                    sockfd, addr = server_socket.accept()
                    CONNECTION_LIST.append(sockfd)
                else:

                    try:
                        data = sock.recv(RECV_BUFFER)
                        #print "receive "+data
                        lmsg = data.split()
                        if lmsg[0] == "ack":
                            item = self.EventList[int(lmsg[-1])]
                            item[1] = int(lmsg[1])
                            if len(lmsg) > 3:
                                item[2] = int(lmsg[2])
                            item[0] = True
                        elif lmsg[0] == "succ":
                            nt = Thread(target=self.ThreadForFind_Successor,args=(lmsg,))
                            nt.start()
                        elif lmsg[0] == "pred":
                            tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            tmpconn.settimeout(2)
                            tmpconn.connect(("localhost", int(lmsg[1])))
                            tmpconn.send("ack %d %d %d" % (self.predecessor,self.predLocation,int(lmsg[-1])))
                        elif lmsg[0] == "upda":
                            self.predecessor = int(lmsg[1])
                            self.predLocation = int(lmsg[2])
                            tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            tmpconn.settimeout(2)
                            tmpconn.connect(("localhost", int(lmsg[3])))
                            tmpconn.send("ack %d %d" % (0,int(lmsg[-1])))
                        elif lmsg[0] == "updatepred":
                            self.finger[1][1] = int(lmsg[1])
                            self.finger[1][2] = int(lmsg[2])
                            tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            tmpconn.settimeout(2)
                            tmpconn.connect(("localhost", int(lmsg[3])))
                            tmpconn.send("ack %d %d" % (0,int(lmsg[-1])))
                        elif lmsg[0] == "fing":
                            nt = Thread(target=self.ThreadForUpdate_Finger_Table,args=(lmsg,))
                            nt.start()
                        elif lmsg[0] == "getsuc":
                            tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            tmpconn.settimeout(2)
                            tmpconn.connect(("localhost", int(lmsg[1])))
                            tmpconn.send("ack %d %d %d" % (self.finger[1][1],self.finger[1][2],int(lmsg[-1])))

                        elif lmsg[0] == "closest":
                            nt = Thread(target=self.ThreadForClosest_Preceding_Finger,args=(lmsg,))
                            nt.start()
                        elif lmsg[0] == "recover":
                            nt = Thread(target=self.ThreadForRecover_Finger_Table,args=(lmsg,))
                            nt.start()
                        else:
                            self.messagequeue.append(data)
                    except:
                        sock.close()
                        CONNECTION_LIST.remove(sock)

        server_socket.close()

    # Pop out one message from message queue once a time and execute the message
    def runexec(self):
        while 1:
            if self.messagequeue:
                msg = self.messagequeue.pop(0)
                #print "exec "+msg
                lmsg = msg.split()
                if lmsg[0] == "join":
                    self.Node_Join()
                if lmsg[0] == "show":
                    self.showkey()
                if lmsg[0] == "leave":
                    self.leavenode()
                if lmsg[0] == "find":
                    self.findkey(int(lmsg[1]))
            #time.sleep(0.01)

    def ThreadForFind_Successor(self, lmsg):
        p1,p2 = self.Find_Successor(int(lmsg[1]))
        tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmpconn.settimeout(2)
        tmpconn.connect(("localhost", int(lmsg[2])))
        tmpconn.send("ack %d %d %d" % (p1,p2,int(lmsg[-1])))
        global MsgCnt
        MsgCnt += 1
        #print "ThreadForFind_Successor ack + 1"

    def ThreadForUpdate_Finger_Table(self, lmsg):
        self.Update_Finger_Table(int(lmsg[1]),int(lmsg[2]),int(lmsg[3]))
        tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmpconn.settimeout(2)
        tmpconn.connect(("localhost", int(lmsg[4])))
        tmpconn.send("ack %d %d" % (0,int(lmsg[-1])))
        global MsgCnt
        MsgCnt += 1
        #print "ThreadForUpdate_Finger_Table ack + 1"

    def ThreadForClosest_Preceding_Finger(self, lmsg):
        p1,p2 = self.Closest_Preceding_Finger(int(lmsg[1]))
        tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmpconn.settimeout(2)
        tmpconn.connect(("localhost", int(lmsg[2])))
        tmpconn.send("ack %d %d %d" % (p1,p2,int(lmsg[-1])))
        global MsgCnt
        MsgCnt += 1
        #print "ThreadForClosest_Preceding_Finger ack + 1"

    def ThreadForRecover_Finger_Table(self, lmsg):
        self.Recover_Finger_Table(int(lmsg[1]),int(lmsg[2]),int(lmsg[3]),int(lmsg[4]))
        tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmpconn.settimeout(2)
        tmpconn.connect(("localhost", int(lmsg[5])))
        tmpconn.send("ack %d %d" % (0,int(lmsg[-1])))
        global MsgCnt
        MsgCnt += 1
        #print "ThreadForRecover_Finger_Table ack + 1"

    def Node_Join(self):
        global Bit
        for i in xrange(1,Bit+1):
            self.finger[i][0] = (self.KeyLocation + (1 << (i-1))) % (1 << Bit)
        if self.KeyLocation == 0:
            for i in xrange(1,Bit+1):
                self.finger[i][1] = self.PORT
                self.finger[i][2] = self.KeyLocation
            self.predecessor = self.PORT
            self.predLocation = self.KeyLocation
        else:
            self.Init_Finger_Table()
            self.Update_Others()
        self.pst.send("join")


    def Init_Finger_Table(self):
        global InitPeerPort
        # update finger[1].node
        tsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tsoc.settimeout(2)
        tsoc.connect(("localhost", InitPeerPort))
        tmpid = self.RemoteCall()
        tsoc.send("succ %d %d %d" % (self.finger[1][0],self.PORT,tmpid,))
        global MsgCnt
        MsgCnt += 1
        #print "update finger[1].node + 1"
        self.WaitForResponse(tmpid)
        self.finger[1][1] = self.EventList[tmpid][1]
        self.finger[1][2] = self.EventList[tmpid][2]


        # set predecessor
        nsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        nsoc.settimeout(2)
        nsoc.connect(("localhost", self.finger[1][1]))
        tmpid = self.RemoteCall()
        nsoc.send("pred %d %d" % (self.PORT,tmpid,))
        MsgCnt += 1
        #print "set predecessor + 1"
        self.WaitForResponse(tmpid)
        self.predecessor = self.EventList[tmpid][1]
        self.predLocation = self.EventList[tmpid][2]

        # update successor.predecessor
        nsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        nsoc.settimeout(2)
        nsoc.connect(("localhost", self.finger[1][1]))
        tmpid = self.RemoteCall()
        nsoc.send("upda %d %d %d %d" % (self.PORT,self.KeyLocation,self.PORT,tmpid,))
        MsgCnt += 1
        #print "update successor.predecessor + 1"
        self.WaitForResponse(tmpid)

        # update predecessor.successor
        nsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        nsoc.settimeout(2)
        nsoc.connect(("localhost", self.predecessor))
        tmpid = self.RemoteCall()
        nsoc.send("updatepred %d %d %d %d" % (self.PORT,self.KeyLocation,self.PORT,tmpid,))
        MsgCnt += 1
        #print "update predecessor.successor + 1"
        self.WaitForResponse(tmpid)

        # update its own finger table
        for i in xrange(1,Bit):
            if self.inrange(self.finger[i+1][0],self.KeyLocation,(self.finger[i][2]-1+(1<<Bit))%(1<<Bit)):
                self.finger[i+1][1] = self.finger[i][1]
                self.finger[i+1][2] = self.finger[i][2]
            else:
                tmpid = self.RemoteCall()
                tsoc.send("succ %d %d %d" % (self.finger[i+1][0],self.PORT,tmpid,))
                #print "get succ + 1"
                MsgCnt += 1
                self.WaitForResponse(tmpid)
                self.finger[i+1][1] = self.EventList[tmpid][1]
                self.finger[i+1][2] = self.EventList[tmpid][2]

    def Update_Others(self):
        global Bit
        #print "update others!"
        for i in xrange(1, Bit+1):
            #print i, self.KeyLocation, (self.KeyLocation-(1<<(i-1))+(1<<Bit) + 1) % (1<<Bit)
            p = self.Find_Predecessor((self.KeyLocation-(1<<(i-1))+(1<<Bit) + 1) % (1<<Bit))
            #print "p ", p
            if p == self.PORT:
                #print "return here!"
                continue
            tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmpconn.settimeout(2)
            tmpconn.connect(("localhost", p))
            tmpid = self.RemoteCall()
            tmpconn.send("fing %d %d %d %d %d" % (self.PORT,self.KeyLocation,i,self.PORT,tmpid,))
            global MsgCnt
            MsgCnt += 1
            #print "update others"
            self.WaitForResponse(tmpid)


    def Update_Finger_Table(self,sPort,sLoc,i):
        if sLoc == self.KeyLocation:
            return
        if self.inrange(sLoc,self.KeyLocation,(self.finger[i][2]-1+(1<<Bit))%(1<<Bit)):
            self.finger[i][1] = sPort
            self.finger[i][2] = sLoc
            tmpp = self.predecessor
            tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmpconn.settimeout(2)
            tmpconn.connect(("localhost", tmpp))
            tmpid = self.RemoteCall()
            tmpconn.send("fing %d %d %d %d %d" % (sPort,sLoc,i,self.PORT,tmpid,))
            global MsgCnt
            MsgCnt += 1
            #print "finger + 1"
            self.WaitForResponse(tmpid)

    # Wait for Remote Procedure Call completed
    def WaitForResponse(self,tid):
        while not self.EventList[tid][0]:
            pass

    # Judge whether x is in [y,z]
    def inrange(self,x,y,z):
        global Bit
        if z < y:
            z += (1 << Bit)
        if x < y:
            x += (1 << Bit)
        if x >= y and x <= z:
            return True
        else:
            return False

    # return successor of index, format: [portnumber, keylocation]
    def Find_Successor(self,index):
        portnumber = self.Find_Predecessor(index)
        return self.getsucc(portnumber)

    # return port number of predecessor
    def Find_Predecessor(self,index):
        global Bit
        cur = [self.PORT,self.KeyLocation]
        succ = self.getsucc(cur[0])
        while not self.inrange(index,(cur[1]+1) % (1<<Bit),succ[1]):
            tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmpconn.settimeout(2)
            tmpconn.connect(("localhost", cur[0]))
            tmpid = self.RemoteCall()
            tmpconn.send("closest %d %d %d" % (index,self.PORT,tmpid,))
            global MsgCnt
            MsgCnt += 1
            #print "closest + 1"
            self.WaitForResponse(tmpid)
            cur = [self.EventList[tmpid][1], self.EventList[tmpid][2]]
            succ = self.getsucc(cur[0])
        return cur[0]

    # Return closest preceding node [portnumber, keylocation]
    def Closest_Preceding_Finger(self,index):
        global Bit
        for i in xrange(Bit,0,-1):
            if self.inrange(self.finger[i][2],(self.KeyLocation+1)%(1<<Bit),(index-1+(1<<Bit))%(1<<Bit)):
                #print self.finger[i][2],(self.KeyLocation+1)%(1<<Bit),(index-1+(1<<Bit))%(1<<Bit)
                return [self.finger[i][1],self.finger[i][2]]
        return [self.PORT,self.KeyLocation]


    # return pn.successor [portnumber, keylocation]
    def getsucc(self,pn):
        tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmpconn.settimeout(2)
        tmpconn.connect(("localhost", pn))
        tmpid = self.RemoteCall()
        tmpconn.send("getsuc %d %d" % (self.PORT,tmpid,))
        #print "getsuc + 1"
        self.WaitForResponse(tmpid)
        return [self.EventList[tmpid][1], self.EventList[tmpid][2]]


    def showkey(self):
        global Bit
        cur = (self.predLocation+1) % (1 << Bit)
        self.keys = []
        while cur != self.KeyLocation:
            self.keys.append(cur)
            cur = (cur+1) % (1 << Bit)
        self.keys.append(self.KeyLocation)
        st = str(self.KeyLocation)
        for item in self.keys:
            st += " " + str(item)
        """
        # For test
        print self.KeyLocation
        for item in self.finger:
            print item
        st = "123"
        """
        self.pst.send("show "+st)

    def RemoteCall(self):
        while 1:
            if self.mutex:
                self.mutex = False
                self.EventList.append([False,0,0])
                tid = len(self.EventList)-1
                self.mutex = True
                return tid
        return -1

    def leavenode(self):
        # update successor.predecessor
        nsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        nsoc.settimeout(2)
        nsoc.connect(("localhost", self.finger[1][1]))
        tmpid = self.RemoteCall()
        nsoc.send("upda %d %d %d %d" % (self.predecessor,self.predLocation,self.PORT,tmpid,))
        self.WaitForResponse(tmpid)

        # update others' finger table
        global Bit
        for i in xrange(1, Bit+1):
            p = self.Find_Predecessor((self.KeyLocation-(1<<(i-1))+(1<<Bit)+1) % (1<<Bit))
            #print (self.KeyLocation-(1<<(i-1))+(1<<Bit)+1) % (1<<Bit), i, p
            if p == self.PORT:
                continue
            tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmpconn.settimeout(2)
            tmpconn.connect(("localhost", p))
            tmpid = self.RemoteCall()
            tmpconn.send("recover %d %d %d %d %d %d" % (self.finger[1][1],self.finger[1][2],i,self.PORT,self.PORT,tmpid,))
            self.WaitForResponse(tmpid)
        self.pst.send("leave")

    def Recover_Finger_Table(self,sPort,sLoc,i,oPort):
        #print self.finger[i][1], sPort, sLoc, oPort
        if self.PORT == oPort:
            return
        if self.finger[i][1] == oPort:
            self.finger[i][1] = sPort
            self.finger[i][2] = sLoc
            tmpp = self.predecessor
            tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmpconn.settimeout(2)
            tmpconn.connect(("localhost", tmpp))
            tmpid = self.RemoteCall()
            tmpconn.send("recover %d %d %d %d %d %d" % (sPort,sLoc,i,oPort,self.PORT,tmpid,))
            self.WaitForResponse(tmpid)

    def findkey(self,k):
        curkey = self.Find_Successor(k)[1]
        self.pst.send("find %d" % (curkey))
