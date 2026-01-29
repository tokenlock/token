import pandas as pd
from abc import ABC, abstractmethod
from events import MarketEvent, SignalEvent
from utils import MACDCalculator, MAFSCalculator

class Strategy(ABC):
    @abstractmethod
    def on_market_event(self, event):
        pass

class SpreadStrategy(Strategy):
    def __init__(self, event_bus, spread_thresh=0.002, strategy_id="spread"):
        self.event_bus = event_bus
        self.spread_thresh = spread_thresh
        self.strategy_id = strategy_id

    async def on_market_event(self, event: MarketEvent):
        data = event.data
        for bar in data:
            symbol = bar["symbol"]
            bid_price = bar["bid_price"]
            ask_price = bar["ask_price"]
            last_price = bar["last_price"]
            timestamp = bar["timestamp"]
            buy_spread = last_price - bid_price
            sell_spread = ask_price - last_price
            if buy_spread <= self.spread_thresh:
                print(f"[{timestamp}] STRATEGY: Buy signal for {symbol}")
                print("bid_price:", bid_price, "ask_price", ask_price, "last_price:", last_price)
                await self.event_bus.publish(SignalEvent(symbol, timestamp, 'BUY', meta={"strategy_id": self.strategy_id}))
            elif sell_spread <= self.spread_thresh:
                print(f"[{timestamp}] STRATEGY: Sell signal for {symbol}")
                print("bid_price:", bid_price, "ask_price", ask_price, "last_price:", last_price)
                await self.event_bus.publish(SignalEvent(symbol, timestamp, 'SELL', meta={"strategy_id": self.strategy_id}))

class RTStrategy(Strategy):
    def __init__(self, event_bus, samples=10, scale_ratio=1.0, max_depth=1, strategy_id="macdfs"):
        self.event_bus = event_bus
        self.macd_cal = MACDCalculator()
        self.mafs_cal = MAFSCalculator()
        self.macds = []
        self.samples = samples
        self.scale_ratio = scale_ratio
        self.max_depth = max_depth # 最多可以连续买入的次数
        self.strategy_id = strategy_id
        self.event_bus.register('MarketEvent', self.on_market_event)

    def on_market_event(self, event: MarketEvent):
        signal = 0
        symbol = event.symbol
        timestamp = event.timestamp
        bar = event.data
    
        # Update technical indicators
        diff, dea, macd = self.macd_cal.update(bar["close"])
        mafs_price = self.mafs_cal.update(bar["turnover"] * 10000, bar["volume"] * 100)
        # Store MACD values with a longer history
        self.macds.append(macd)
        
        # Require more samples for more stable signals
        required_samples = max(10, self.samples)  # Use at least 10 samples
        
        if len(self.macds) >= required_samples:
            # Calculate derivatives over a longer window
            window_size = 8  # Number of points to consider for trend analysis
            recent_macds = self.macds[-window_size:]
            
            # Calculate first derivatives (slopes)
            first_derivatives = [recent_macds[i+1] - recent_macds[i] 
                            for i in range(len(recent_macds)-1)]
            
            # Calculate second derivatives (changes in slope)
            second_derivatives = [first_derivatives[i+1] - first_derivatives[i] 
                                for i in range(len(first_derivatives)-1)]
            
            first_deriv_mean = sum(first_derivatives) / len(first_derivatives)
            current_trend = 1 if first_deriv_mean >= 0 else -1
            # Signal generation with more confirmation
            if (current_trend == 1 and  # Upward trend
                bar["close"] > mafs_price and  # Price below MAFS
                all(d <= 0 for d in first_derivatives[-2:])):  # Strong recent Downward momentum
                print(f"[{timestamp}] STRATEGY: Buy signal for {symbol}")
                self.event_bus.dispatch(SignalEvent(symbol, timestamp, 'BUY', meta={"strategy_id": self.strategy_id, "scale_ratio": self.scale_ratio, "max_depth": self.max_depth}))    
                
            elif (current_trend == -1 and  # Downward trend
                bar["close"] < mafs_price and  # Price above MAFS
                all(d >= 0 for d in first_derivatives[-2:])):  # Strong recent Upward momentum
                print(f"[{timestamp}] STRATEGY: Sell signal for {symbol}")
                self.event_bus.dispatch(SignalEvent(symbol, timestamp, 'SELL', meta={"strategy_id": self.strategy_id, "scale_ratio": self.scale_ratio, "max_depth": self.max_depth}))    
        
    
class GridStrategy(Strategy):
    # 低于3000点开始买进 金字塔建仓 然后进行网格交易
    def __init__(self, event_bus, init_price=3000, levels=[1.00, 0.95, 0.91, 0.88, 0.86, 0.84, 0.82], scale_ratio=1.0, strategy_id="grid"):
        self.event_bus = event_bus
        self.init_price = init_price
        self.scale_ratio = scale_ratio
        self.strategy_id = strategy_id
        self.price_levels = [init_price * level for level in levels]
        self.prev_level = 0  # 用于记录上一个阶段level 默认给最高层
        self.event_bus.register('MarketEvent', self.on_market_event)
    
    def get_level(self, current_price):
        for level, price_level in enumerate(self.price_levels):
            if current_price > price_level:
                return level
        return len(self.price_levels)

    def on_market_event(self, event: MarketEvent):
        symbol = event.symbol
        timestamp = event.timestamp
        bar = event.data
        current_level = self.get_level(bar['close'])
        if self.prev_level is not None:
            if current_level > self.prev_level:
                print(f"[{timestamp}] STRATEGY: Buy signal for {symbol}")
                self.event_bus.dispatch(SignalEvent(symbol, timestamp, 'BUY',  meta={"current_level": current_level, "strategy_id": self.strategy_id, "scale_ratio": self.scale_ratio}))
            elif current_level < self.prev_level:
                print(f"[{timestamp}] STRATEGY: Sell signal for {symbol}")
                self.event_bus.dispatch(SignalEvent(symbol, timestamp, 'SELL', meta={"current_level": current_level, "strategy_id": self.strategy_id, "scale_ratio": self.scale_ratio}))
        self.prev_level = current_level

class MovingAverageCrossStrategy(Strategy):
    """
    一个简单的移动平均线交叉策略。
    """
    def __init__(self, event_bus, symbols, short_window=10, long_window=30):
        self.event_bus = event_bus
        self.symbols = symbols
        self.short_window = short_window
        self.long_window = long_window
        
        self.bars = {s: pd.DataFrame() for s in self.symbols}
        self.bought = {s: False for s in self.symbols}

        self.event_bus.register('MarketEvent', self.on_market_event)

    def on_market_event(self, event: MarketEvent):
        symbol = event.symbol
        timestamp = event.timestamp
        
        # 追加新数据
        new_bar = pd.DataFrame([event.data], index=[timestamp])
        self.bars[symbol] = pd.concat([self.bars[symbol], new_bar])
        
        # 需要足够的数据来计算长均线
        if len(self.bars[symbol]) < self.long_window:
            return

        # 计算均线
        short_ma = self.bars[symbol]['close'].rolling(window=self.short_window).mean().iloc[-1]
        long_ma = self.bars[symbol]['close'].rolling(window=self.long_window).mean().iloc[-1]

        # 产生信号
        if short_ma > long_ma and not self.bought[symbol]:
            print(f"[{timestamp}] STRATEGY: Long signal for {symbol}")
            self.event_bus.dispatch(SignalEvent(symbol, timestamp, 'LONG'))
            self.bought[symbol] = True
        elif short_ma < long_ma and self.bought[symbol]:
            print(f"[{timestamp}] STRATEGY: Exit signal for {symbol}")
            self.event_bus.dispatch(SignalEvent(symbol, timestamp, 'EXIT'))
            self.bought[symbol] = False