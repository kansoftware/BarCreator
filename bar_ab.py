import datetime

class Bar_AB:

    def __init__(self, aTicker, aCounter=60):
        self.__counter = aCounter
        self.ticker = aTicker
        self.__asks = []
        self.__bids = []
        self.__last_ask = None
        self.__last_bid = None
        self.dt = datetime.datetime.now()

    def reset(self):
        self.__asks = []
        self.__bids = []
        self.__last_ask = None
        self.__last_bid = None

    def add_tick(self, aAsk, aBid):
        if (aAsk > aBid) and (aBid != 0) and ((aAsk / aBid - 1.0) < 0.05):
            self.__last_ask = aAsk
            self.__last_bid = aBid
            self.dt = datetime.datetime.now()
        else:
            self.__last_ask = None
            self.__last_bid = None

    def check_md(self):
        last_up = (datetime.datetime.now() - self.dt).seconds
        if last_up > self.__counter * 2:
           self.reset()
           print("reset: " + self.ticker)
           return

        if self.__last_ask is not None:
           self.__asks.append(self.__last_ask)
           while len(self.__asks) > self.__counter:
              self.__asks.pop(1)
        else:
            self.reset()

        if self.__last_bid is not None:
           self.__bids.append(self.__last_bid)
           while len(self.__bids) > self.__counter:
              self.__bids.pop(1)
        else:
            self.reset()

    def get_means(self):
        lask = sum(self.__asks)
        lbid = sum(self.__bids)
        return (lask, lbid, len(self.__asks), len(self.__bids))

    def __str__(self) -> str:
        if self.__counter == 0 or len(self.__asks) == 0 or len(self.__bids) == 0:
            return None
        else:
            ask, bid, lask, lbid = self.get_means()
            return "AB_Bar: {}\t{:.6f} {:.6f}".format(self.ticker, ask/lask, bid/lbid)
