import threading
import time
import socket, select, string, sys

Bit = 8
CoordinatorPort = 4000
ActionComplete = True

class CoordinatorThread(threading.Thread):
    def __init__(self,fileHandle):
        threading.Thread.__init__(self)
        self.fileHandle = fileHandle
        self.Peers = []
        self.CurPort = 4001
        self.joinPeerThread(0)

    def run(self):
        while 1:
            cmd = None
            if self.fileHandle:
                cmd = self.fileHandle.readline()
            while cmd and ActionComplete:
                cmd = cmd.strip()
                if cmd.strip()[:4] == "join":
                    self.joinPeerThread(int(cmd.split()[1]))
                cmd = self.fileHandle.readline()

    def joinPeerThread(self, key):
        global ActionComplete
        # Create New PeerThread
        PT = PeerThread(key, self.CurPort)
        PT.start()
        st = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        st.settimeout(2)
        st.connect(("localhost", self.CurPort))
        self.Peers.append([PT, key, st])
        self.CurPort += 1
        ActionComplete = False
        st.send("join")

class CoordinatorServerThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        global CoordinatorPort
        global ActionComplete
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
                        if data == "join":
                            print "Join Complete!"
                            ActionComplete = True
                        elif data == "leave":
                            print "Leave Complete!"
                            ActionComplete = True
                    except:
                        sock.close()
                        CONNECTION_LIST.remove(sock)

        server_socket.close()


class PeerThread(threading.Thread):
    def __init__(self,key,port):
        threading.Thread.__init__(self)
        self.stop = False
        self.KeyLocation = key
        self.PORT = port

        # Set up connection with coordinator server
        global CoordinatorPort
        self.pst = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.pst.settimeout(2)
        self.pst.connect(("localhost", CoordinatorPort))

        global Bit
        # In Finger array, first element represents start key, second element represents first node > finger[k].start
        self.finger = [[0,0] for i in xrange(Bit+1)]

    def run(self):
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
                        if data == "join":
                            self.Node_Join()
                    except:
                        sock.close()
                        CONNECTION_LIST.remove(sock)

        server_socket.close()

    def Node_Join(self):
        self.pst.send("join")

    def Init_Finger_Table(self):
        pass

    def Update_Others(self):
        pass

    def Update_Finger_Table(self):
        pass





def main():
    fileHandle = None
    if sys.argv[1] and sys.argv[1] == "-g":
        fileHandle = open(sys.argv[2],"r")
    CServerThread = CoordinatorServerThread()
    CServerThread.start()
    CThread = CoordinatorThread(fileHandle)
    CThread.start()

if __name__ == '__main__':
    main()




