from threading import Thread
import socket
from time import sleep


class MDClient(Thread):
    fTerminate = False
    fSocket = None
    fCallBack = None
    fPort = None
    fServer = None
    fSkipCount = 0
    fSubscribeList = []
    fSubscribeListMD = []

    def __init__(self, aCallBack, aServer='192.168.1.101', aPort=2016):
        Thread.__init__(self)
        self.fCallBack = aCallBack
        self.fPort = aPort
        self.fServer = aServer

    def run(self):

        self.reconnect()

        while not self.fTerminate:
            data = self.fSocket.recv(65536)
            if len(data) == 0 and not self.fTerminate:
                self.reconnect()
                continue

            self.fCallBack(data)
            #print(data)
            self.fSkipCount = 0

        self.fSocket.close()

    def stop(self):
        self.fTerminate = True

    def timer_callback(self):
        self.fSkipCount += 1
        if self.fSkipCount > 30 and not self.fSocket._closed:
            self.fSocket.close()
            print("Connect lost!")

    def reconnect(self):
        print("Reconnecting...")
        if self.fSocket is not None and not self.fSocket._closed:
            self.fSocket.close()

        while not self.fTerminate:
            lConnected = False
            try:
                self.fSocket = socket.socket()
                self.fSocket.settimeout(60)
                self.fSocket.connect((self.fServer, self.fPort))
                lConnected = True
            except socket.error as msg:
                print(msg)
                sleep(1)

            if lConnected:
                break

        self.fSkipCount = 0

        print("Resubscribing...")
        for lTosend in self.fSubscribeList:
            self.fSocket.send(lTosend.encode())

        for lTosend in self.fSubscribeListMD:
            self.fSocket.send(lTosend.encode())

        print("Connected")

    def subscribe(self, aTicker):
        if aTicker is not None:
            lTosend = '<'+aTicker+';TRADES>'
            self.fSocket.send(lTosend.encode())
            print("sent: " + lTosend)
            self.fSubscribeList.append(lTosend)

    def subscribeMD(self, aTicker):
        if aTicker is not None:
            lTosend = '<'+aTicker+'>'  # ;ASK_BID
            self.fSocket.send(lTosend.encode())
            print("sent: " + lTosend)
            self.fSubscribeListMD.append(lTosend)
