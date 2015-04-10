import threading
from threading import Thread
import time
import socket, select, string, sys

Bit = 8
CoordinatorPort = 3000
InitPeerPort = 3001


class CoordinatorThread:
    def __init__(self,fileHandle):
        global InitPeerPort
        self.fileHandle = fileHandle
        self.Peers = []
        self.CurPort = InitPeerPort
        self.ActionComplete = False
        self.sthread = Thread(target=self.runserver)
        self.sthread.start()
        self.joinPeerThread(0)
        self.rthread = Thread(target=self.readcommand)
        self.rthread.start()
        self.showallcomplete = True

    def readcommand(self):
        cmd = None
        if self.fileHandle:
            cmd = self.fileHandle.readline()
        while 1:
            while cmd and self.ActionComplete and self.showallcomplete:
                cmd = cmd.strip()
                if cmd.strip()[:4] == "join":
                    self.ActionComplete = False
                    self.joinPeerThread(int(cmd.split()[1]))
                if cmd.strip()[:4] == "show":
                    if cmd.split()[1] == "all":
                        self.showall()
                    else:
                        self.ActionComplete = False
                        self.show(int(cmd.split()[1]))
                if cmd.strip()[:5] == "leave":
                    self.ActionComplete = False
                    self.leavePeer(int(cmd.split()[1]))
                if cmd.strip()[:4] == "find":
                    self.ActionComplete = False
                    self.findkey(int(cmd.split()[1]),int(cmd.split()[2]))
                cmd = self.fileHandle.readline()


    def joinPeerThread(self, key):
        # Create New PeerThread
        PT = PeerThread(key, self.CurPort)
        time.sleep(0.02)
        st = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        st.settimeout(2)
        st.connect(("localhost", self.CurPort))
        self.Peers.append([PT, key, st])
        self.CurPort += 1
        if key == 0:
            st.send("join 3001 0")
        else:
            miniloc = 10000
            miniport = 0
            for item in self.Peers:
                tmp = item[1]
                if tmp == key:
                    continue;
                if tmp < key:
                    tmp += 256
                if tmp < miniloc:
                    miniloc = tmp
                    miniport = item[0].PORT
            st.send("join %d %d" % (miniport,miniloc%256))

    def show(self, key):
        for item in self.Peers:
            if item[1] == key:
                item[2].send("show")
                return

    def showall(self):
        self.showallcomplete = False
        self.Peers.sort(key=lambda x:x[1])
        for item in self.Peers:
            while not self.ActionComplete:
                pass
            self.ActionComplete = False
            item[2].send("show")

    def leavePeer(self, key):
        for item in self.Peers:
            if item[1] == key:
                item[2].send("leave")
                self.Peers.remove(item)
                return

    def findkey(self,p,k):
        for item in self.Peers:
            if item[1] == p:
                item[2].send("find %d" % (k))
                return
        print "oh"
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
                            self.ActionComplete = True
                        elif data == "leave":
                            print "Leave Complete!"
                            self.ActionComplete = True
                        elif data[:4] == "show":
                            print data[5:]
                            self.ActionComplete = True
                        elif data[:4] == "find":
                            print "Find the key at Node %d" % (int(data.split()[1]))
                            self.ActionComplete = True
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
                    self.Node_Join(int(lmsg[1]),int(lmsg[2]))
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

    def ThreadForUpdate_Finger_Table(self, lmsg):
        self.Update_Finger_Table(int(lmsg[1]),int(lmsg[2]),int(lmsg[3]))
        tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmpconn.settimeout(2)
        tmpconn.connect(("localhost", int(lmsg[4])))
        tmpconn.send("ack %d %d" % (0,int(lmsg[-1])))

    def ThreadForClosest_Preceding_Finger(self, lmsg):
        p1,p2 = self.Closest_Preceding_Finger(int(lmsg[1]))
        tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmpconn.settimeout(2)
        tmpconn.connect(("localhost", int(lmsg[2])))
        tmpconn.send("ack %d %d %d" % (p1,p2,int(lmsg[-1])))

    def ThreadForRecover_Finger_Table(self, lmsg):
        self.Recover_Finger_Table(int(lmsg[1]),int(lmsg[2]),int(lmsg[3]),int(lmsg[4]))
        tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmpconn.settimeout(2)
        tmpconn.connect(("localhost", int(lmsg[5])))
        tmpconn.send("ack %d %d" % (0,int(lmsg[-1])))

    def Node_Join(self,p1,p2):
        global Bit
        for i in xrange(1,Bit+1):
            self.finger[i][0] = (self.KeyLocation + (1 << (i-1))) % (1 << Bit)
        #self.finger[1][1] = p1
        #self.finger[1][2] = p2
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
        self.WaitForResponse(tmpid)
        self.finger[1][1] = self.EventList[tmpid][1]
        self.finger[1][2] = self.EventList[tmpid][2]


        # set predecessor
        nsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        nsoc.settimeout(2)
        nsoc.connect(("localhost", self.finger[1][1]))
        tmpid = self.RemoteCall()
        nsoc.send("pred %d %d" % (self.PORT,tmpid,))
        self.WaitForResponse(tmpid)
        self.predecessor = self.EventList[tmpid][1]
        self.predLocation = self.EventList[tmpid][2]

        # update successor.predecessor
        nsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        nsoc.settimeout(2)
        nsoc.connect(("localhost", self.finger[1][1]))
        tmpid = self.RemoteCall()
        nsoc.send("upda %d %d %d %d" % (self.PORT,self.KeyLocation,self.PORT,tmpid,))
        self.WaitForResponse(tmpid)

        # update predecessor.successor
        nsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        nsoc.settimeout(2)
        nsoc.connect(("localhost", self.predecessor))
        tmpid = self.RemoteCall()
        nsoc.send("updatepred %d %d %d %d" % (self.PORT,self.KeyLocation,self.PORT,tmpid,))
        self.WaitForResponse(tmpid)

        # update its own finger table
        for i in xrange(1,Bit):
            if self.inrange(self.finger[i+1][0],self.KeyLocation,(self.finger[i][2]-1+(1<<Bit))%(1<<Bit)):
                self.finger[i+1][1] = self.finger[i][1]
                self.finger[i+1][2] = self.finger[i][2]
            else:
                tmpid = self.RemoteCall()
                tsoc.send("succ %d %d %d" % (self.finger[i+1][0],self.PORT,tmpid,))
                self.WaitForResponse(tmpid)
                self.finger[i+1][1] = self.EventList[tmpid][1]
                self.finger[i+1][2] = self.EventList[tmpid][2]

    def Update_Others(self):
        global Bit
        for i in xrange(1, Bit+1):
            #print (self.KeyLocation-(1<<(i-1))+(1<<Bit)+1) % (1<<Bit), i
            p = self.Find_Predecessor((self.KeyLocation-(1<<(i-1))+(1<<Bit)+1) % (1<<Bit))
            if p == self.PORT:
                continue
            tmpconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmpconn.settimeout(2)
            tmpconn.connect(("localhost", p))
            tmpid = self.RemoteCall()
            tmpconn.send("fing %d %d %d %d %d" % (self.PORT,self.KeyLocation,i,self.PORT,tmpid,))
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
            self.WaitForResponse(tmpid)

    # Wait for Remote Procedure Call completed
    def WaitForResponse(self,tid):
        while not self.EventList[tid][0]:
            pass

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



def main():

    #fileHandle = None
    #if sys.argv[1] and sys.argv[1] == "-g":
    #    fileHandle = open(sys.argv[2],"r")
    fileHandle = open("input.txt","r")
    CThread = CoordinatorThread(fileHandle)

if __name__ == '__main__':
    main()




