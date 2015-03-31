import threading
import time
import socket, select, string, sys

Peers = []

class CoordinatorThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        self.update()

    def update(self):
        pass

class PeerThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop = False

    def run(self):
        print "start"
        count = 0
        while not self.stop:
            count += 1
        print "ends"



def main():
    Peers.append(PeerThread())
    Peers[0].start()
    time.sleep(5)
    Peers[0].stop = True
    del Peers[0]
    Peers.append(PeerThread())
    print len(Peers)
    Peers[0].start()

if __name__ == '__main__':
    main()




