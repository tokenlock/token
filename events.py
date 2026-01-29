import asyncio
from collections import defaultdict

class EventBus:
    """
    事件总线，负责管理事件订阅者和发布事件
    """
    def __init__(self):
        # 保留原有结构，仅变量名微调（listeners → subscribers，更贴合订阅语义）
        self.subscribers = defaultdict(list)

    def subscribe(self, event_type, subscriber):
        """
        订阅一个事件（替代原register）
        Args:
            event_type (str): 事件类型名称
            subscriber (callable): 订阅者回调函数
        """
        self.subscribers[event_type].append(subscriber)
        # 日志中也同步修改命名，保持一致
        print(f"Subscriber {subscriber.__qualname__} subscribed for event '{event_type}'")

    def unsubscribe(self, event_type, subscriber):
        """
        取消订阅一个事件（替代原unregister）
        """
        if subscriber in self.subscribers[event_type]:
            self.subscribers[event_type].remove(subscriber)

    async def publish(self, event):
        """
        发布一个事件，通知所有相关的订阅者（替代原dispatch）
        Args:
            event (Event): 要发布的事件对象
        """
        event_type = event.type
        if event_type in self.subscribers:
            for subscriber in self.subscribers[event_type]:
                # 如果 subscriber 是异步函数，用 create_task 异步调度
                if asyncio.iscoroutinefunction(subscriber):
                    # 加到事件循环调度 其他空闲了就执行该event 不会阻塞
                    task = asyncio.create_task(subscriber(event))  # 加到事件循环调度
                    print(task)
                else:
                    # 普通同步函数直接调用
                    subscriber(event)

class Event:
    """
    所有事件类的基类
    """
    @property
    def type(self):
        return self.__class__.__name__

class TokenEvent(Event):
    def __init__(self, access_token, meta={}):
        self.access_token = access_token
        self.meta = meta

class MarketEvent(Event):
    """
    当有新的市场数据（如K线）产生时，触发此事件
    """
    def __init__(self, data, meta={}):
        self.data = data  # K线数据 (通常是一个Series或dict)
        self.meta = meta

class SignalEvent(Event):
    """
    当策略产生交易信号时，触发此事件
    """
    def __init__(self, symbol, timestamp, signal, strength=1.0, meta={}):
        self.symbol = symbol
        self.timestamp = timestamp
        self.signal = signal
        self.strength = strength # 信号强度
        self.meta = meta # 额外信息

class OrderEvent(Event):
    """
    当投资组合决定下单时，触发此事件
    """
    def __init__(self, symbol, order_type, quantity, signal, meta={}):
        self.symbol = symbol
        self.order_type = order_type  # 'MARKET' (市价), 'LIMIT' (限价)
        self.quantity = quantity      # 数量 (正数)
        self.signal = signal    # 'BUY' or 'SELL'
        self.meta = meta


class FillEvent(Event):
    """
    当订单在通过风控模块后，触发此事件
    """
    def __init__(self, symbol, timestamp, quantity, signal, fill_price, commission, meta={}):
        self.symbol = symbol
        self.timestamp = timestamp
        self.quantity = quantity
        self.signal = signal
        self.fill_price = fill_price
        self.commission = commission
        self.meta = meta
