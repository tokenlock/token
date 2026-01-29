import os
import asyncio
import requests
from tokenx import TokenEngine
from stream import DataStream
from strategy import SpreadStrategy
from sizers import SpreadSizer
from portfolio import Portfolio
from events import EventBus, TokenEvent, SignalEvent, OrderEvent, FillEvent

class TradeEngine:
    def __init__(self, event_bus, portfolio, sizer, sandbox=True):
        self.event_bus = event_bus
        self.portfolio = portfolio
        self.sizer = sizer
        self.sandbox = sandbox
    
    # 获取到最新access_token, 事件驱动可以发布到多个订阅共同接收
    async def on_token_event(self, event: TokenEvent):
        self.access_token = event.access_token

    async def on_signal_event(self, event: SignalEvent):
        symbol = event.symbol
        timestamp = event.timestamp
        signal = event.signal
        strength = event.strength # 信号强度
        quantity = self.sizer.calculate_quantity(signal)
        if quantity > 0:
            await self.event_bus.publish(OrderEvent(symbol, "MARKET", quantity, signal))
            
    async def on_order_event(self, event: OrderEvent) -> None:
        """
        处理订单事件并构造订单请求参数
        
        Args:
            event: 订单事件对象，包含订单相关信息
        """
        symbol = event.symbol
        order_type = event.order_type
        quantity = event.quantity
        signal = event.signal
        # 构造订单请求的payload（字典格式）
        payload = {
            "orderType": order_type,
            "session": "NORMAL",
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": signal,
                    "quantity": quantity,
                    "instrument": {
                        "symbol": symbol,
                        "assetType": "EQUITY"
                    }
                }
            ]
        }
        
        order_id = self.place_order(payload)

    def place_order(self, payload):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "accept": "application/json"
        }

        try:
            if self.sandbox:
                preview_url = f"https://api.schwabapi.com/trader/v1/accounts/{self.portfolio.account_number}/previewOrder"
                preview_resp = requests.post(preview_url, headers=headers, json=payload, timeout=2)
                # preview有返回
                print(preview_resp.json())
                self.sizer.prev_signal = payload["orderLegCollection"][0]["instruction"]
            else:
                order_url = f"https://api.schwabapi.com/trader/v1/accounts/{self.portfolio.account_number}/orders"
                # response = requests.post(order_url, headers=headers, json=payload, timeout=2)
                # if response.status_code == 201:
                #     # 获取 Location header
                #     location = response.headers.get("Location")
                #     print("Order created, location:", location)
                #     order_id = location.split('/')[-1]
                #     return order_id
            
                # response.raise_for_status()

        except requests.exceptions.Timeout:
            print("错误：Order请求超时，超过2秒未响应")
        except requests.exceptions.ConnectionError:
            print("错误：无法连接到嘉信API服务器，请检查网络")
        except requests.exceptions.RequestException as e:
            print(f"Order请求失败：{str(e)}")


    def get_order_status(self, order_id):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "accept": "application/json"
        }

        order_status_url = f"https://api.schwabapi.com/trader/v1/accounts/{self.portfolio.account_number}/orders/{order_id}"
        response = requests.get(order_status_url, headers=headers)
        status = response.json()["status"]
        return status

    async def on_fill_event(self, event: FillEvent):
        symbol = event.symbol
        timestamp = event.timestamp
        quantity = event.quantity
        signal = event.signal
        fill_price = event.fill_price
        commission = event.commission
        meta = event.meta

        self.sizer.prev_signal = signal



async def main():
    client_id = os.getenv("APP_KEY")
    client_secret = os.getenv("APP_SECRET")
    auth_code_secret = os.getenv("STREAM_SECRET")
    event_bus = EventBus()
    symbols = ["SGOV"]
    
    token_engine = TokenEngine(event_bus, client_id, client_secret, auth_code_secret)
    data_stream = DataStream(event_bus)
    # data_stream订阅token 事件触发connect stream
    event_bus.subscribe("TokenEvent", data_stream.on_token_event)

    strategy = SpreadStrategy(event_bus)
    # strategy订阅stream market data
    event_bus.subscribe('MarketEvent', strategy.on_market_event)

    portfolio = Portfolio()
    # portfolio订阅token
    event_bus.subscribe('TokenEvent', portfolio.on_token_event)

    # init sizer
    sizer = SpreadSizer()
    sandbox = True
    trade_engine = TradeEngine(event_bus, portfolio, sizer, sandbox)
    event_bus.subscribe('TokenEvent', trade_engine.on_token_event)
    event_bus.subscribe('SignalEvent', trade_engine.on_signal_event)
    event_bus.subscribe('OrderEvent', trade_engine.on_order_event)
    event_bus.subscribe('FillEvent', trade_engine.on_fill_event)

    token_engine.start()
    # 创建symbol data task
    # 这里不用start() 是因为on_token_event会因为前面的订阅TokenEvent自动触发
    # 同理 strategy为什么不需要create task 因为已经订阅MarketEvent自动触发
    data_task = asyncio.create_task(data_stream.sub_levelone_equities(symbols))
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())

    
    
