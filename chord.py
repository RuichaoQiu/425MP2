import threading
import time
import socket, select, string, sys

Bit = 8

class CoordinatorThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.Peers = []
        self.Peers.append(PeerThread(0))
        self.Peers[0].start()

    def run(self):
        self.update()

    def update(self):
        pass

class PeerThread(threading.Thread):
    def __init__(self,key):
        threading.Thread.__init__(self)
        self.stop = False
        self.KeyLocation = key

        global Bit
        # In Finger array, first element represents start key, second element represents first node > finger[k].start
        self.finger = [[0,0] for i in xrange(Bit+1)]

    def run(self):
        pass

    def FindSuccessor(self):
        return self.finger[1][1]

    def FindPredecessor(self):
        pass



def main():
    CThread = CoordinatorThread()
    CThread.start()

if __name__ == '__main__':
    main()




