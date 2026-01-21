from kubera_sdk.base import KuberaStrategy, MarketEvent, BarPayload

class MomentumStrategy(KuberaStrategy):
    def __init__(self, name: str, threshold: float):
        super().__init__(name)
        self.threshold = threshold
        self.last_price = None

    def on_tick(self, event: MarketEvent):
        price = event.payload.price
        if self.last_price is not None:
            returns = (price - self.last_price) / self.last_price
            if returns > self.threshold:
                print(f"[{self.name}] BULLISH SIGNAL: returns over {self.threshold}")
            elif returns < -self.threshold:
                print(f"[{self.name}] BEARISH SIGNAL: returns under -{self.threshold}")
        
        self.last_price = price

    def on_bar(self, symbol: str, bar: BarPayload):
        pass
