from threading import Thread
from time import sleep

gTimerEvent = None

class TimerEvent(Thread):
    fCallbacks = []
    fTerminate = False
    fWait = 1

    def __init__(self, aWait=1):
        Thread.__init__(self)
        self.fWait = aWait

    def run(self):
        while not self.fTerminate:
            sleep(self.fWait)
            for cb in self.fCallbacks:
                cb()

    def addListener(self, callback):
        self.fCallbacks.append(callback)

    def stop(self):
        self.fTerminate = True
        self.fCallbacks.clear()
        self.join()

def init_timer():
    global gTimerEvent
    if gTimerEvent is None:
        gTimerEvent = TimerEvent()
        gTimerEvent.start()


def stop_timer():
    global gTimerEvent
    if gTimerEvent is not None:
        gTimerEvent.stop()
        gTimerEvent.join()


def add_timer_listener(callback):
    global gTimerEvent
    if gTimerEvent is not None:
        gTimerEvent.addListener(callback)
