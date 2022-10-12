import re
from threading import Thread
from queue import *
from bar import Bar
from bar_ab import Bar_AB
import datetime


def roundTime(dt=None, roundTo=60):
   """Round a datetime object to any time lapse in seconds
   dt : datetime.datetime object, default now.
   roundTo : Closest number of seconds to round to, default 1 minute.
   """
   if dt == None :
       dt = datetime.datetime.now()
   seconds = (dt.replace(tzinfo=None) - dt.min).seconds
   # rounding = (seconds+roundTo/2) // roundTo * roundTo
   rounding = (seconds // roundTo)*roundTo
   return dt + datetime.timedelta(0, rounding-seconds, -dt.microsecond)


class BarProcessor(Thread):
    fSQLExecutor = None
    fQueue = Queue()
    fTerminate = False
    fBarCollection = {}
    fBarCollectionMD = {}
    fRoundTime = None
    fTime = None
    fCanUpdate = False
    fNewTick = False

                            #<ETHUSDTb;1;213.82000000;110>
    fParser = re.compile(r"\<([^;>]+);(\d);(\d*\.?\d*);(\d+)\>")
    fParserMD = re.compile(r"\<([^;>]+);(\d*\.?\d*);(\d*\.?\d*)\>")

    def __init__(self, aSQLConnector, aRoundTime=60):
        Thread.__init__(self)
        self.fSQLExecutor = aSQLConnector
        self.fRoundTime = aRoundTime
        self.fTime = roundTime(roundTo=self.fRoundTime)

    def timer_callback(self):
        self.fQueue.put(None)
        self.fNewTick = True
        #print("timer")
        for key in self.fBarCollectionMD.keys():
            #print(key)
            self.fBarCollectionMD[key].check_md()

    def add_tick(self, aTick):
        self.fQueue.put(aTick)
        self.fCanUpdate = self.fNewTick
        self.fNewTick = False

    def stop(self):
        self.fTerminate = True

    def run(self):
        while not self.fTerminate:
            #b'<ETHUSDTb;1;213.82000000;110>\n<ETHUSDTb;1;213.82000000;562>\n<ETHUSDTb;1;213.82000000;56>\n'
            lTick = self.fQueue.get()
            self.fQueue.task_done()

            if lTick is not None:
                data = self.fParser.findall(lTick.decode())
                for ticker, side, price, qty in data:
                    #print(ticker + " " + side + " " + price + " " + qty)
                    if self.fBarCollection.get(ticker) is None:
                        self.fBarCollection[ticker] = Bar()
                    self.fBarCollection[ticker].add_tick(float(price), float(qty))

                data = self.fParserMD.findall(lTick.decode())
                for ticker, ask, bid in data:
                    #print("new md {} {} {}".format(ticker, ask, bid))
                    if self.fBarCollectionMD.get(ticker) is None:
                        self.fBarCollectionMD[ticker] = Bar_AB(ticker)
                        #print("ab {} created".format(ticker))
                    self.fBarCollectionMD[ticker].add_tick(float(ask), float(bid))

            lTime = roundTime(roundTo=self.fRoundTime)
            lPrintBar = False
            if self.fTime != lTime:
                lPrintBar = True


            if self.fCanUpdate or lPrintBar:
                for lkey in self.fBarCollection.keys():
                    lBar = self.fBarCollection[lkey]

                    if lBar is None or lBar.open is None:
                        continue

                    lTablename = 'bar_' + lkey.lower()
                    lSQL = "INSERT INTO \"" + lTablename + "\" (datetime, open, high, low, close, volume, trades, vmp) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT(datetime) DO UPDATE SET open=$2, high=$3, low=$4, close=$5, volume=$6, trades=$7, vmp=$8;"
                    #print(lSQL)
                    insert = self.fSQLExecutor.prepare(lSQL)
                    insert(self.fTime, lBar.open, lBar.high, lBar.low, lBar.close, lBar.volume, lBar.trades, lBar.VMP/lBar.volume);

                    if lPrintBar:
                        print("\t"+lkey + "\t" + str(lBar))
                        self.fBarCollection[lkey].reset()

                for lkey in self.fBarCollectionMD.keys():
                    lBarMD = self.fBarCollectionMD[lkey]
                    lTablename = 'ab_bar_' + lkey.lower()
                    lSQL = "INSERT INTO \"" + lTablename + "\" (datetime, ask, bid) VALUES ($1, $2, $3) ON CONFLICT(datetime) DO UPDATE SET ask=$2, bid=$3;"
                    #print(lSQL)
                    insert = self.fSQLExecutor.prepare(lSQL)
                    ask, bid, lasks, lbids = lBarMD.get_means()
                    #print(lkey + " ab " + str(ask) + "\t" + str(bid) + "\t" + str(lasks) + "\t" + str(lbids))
                    if lasks == 0 or lbids == 0:
                        continue

                    insert(self.fTime, ask/lasks, bid/lbids)

                    if lPrintBar:
                        print(str(lBarMD))


                self.fCanUpdate = False
                #print(self.fQueue.qsize())

            if self.fTime != lTime:
                print(self.fTime)
                self.fTime = lTime
