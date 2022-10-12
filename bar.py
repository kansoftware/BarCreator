class Bar:
    high = None
    open = None
    close = None
    low = None
    volume = 0
    trades = 0
    VMP = None

    def reset(self):
        self.high = None
        self.open = None
        self.close = None
        self.low = None
        self.volume = 0
        self.trades = 0
        self.VMP = None

    def add_tick(self, aPrice, aQty):
        lQty = aQty if aQty > 0 else 0.25
        self.volume += lQty
        self.trades = self.trades + 1

        if self.open is None:
            self.high = aPrice
            self.open = aPrice
            self.close = aPrice
            self.low = aPrice
            self.VMP = aPrice * lQty
        else:
            self.high = max(aPrice, self.high)
            self.close = aPrice
            self.low = min(aPrice, self.low)
            self.VMP = self.VMP + aPrice * lQty

    def __str__(self) -> str:

        if self.open is None:
            return None
        else:
            # return str("Bar: " + str(self.open) + " " + str(self.high) + " " + str(self.low) + " " +
            #         str(self.close) + " " + str(self.volume) + " " +
            #         str(self.trades) + " " + str(self.VMP / self.volume))
            return "Bar: {:.6f} {:.6f} {:.6f} {:.6f} {:.6f} {:d} {:.6f}".format(
                self.open,
                self.high,
                self.low,
                self.close,
                self.volume,
                self.trades,
                self.VMP / self.volume)
