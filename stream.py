import os
import json
import asyncio
import requests
import websockets
from datetime import datetime
from events import EventBus, TokenEvent, MarketEvent, FillEvent

class DataStream:
    def __init__(self, event_bus):
        self.event_bus = event_bus
        self.ws = None
        self._ws_lock = asyncio.Lock()
        self.recv_task = None
        self.schwab_streamer_config_url = "https://api.schwabapi.com/trader/v1/userPreference"
        self.streamer_config = None
        self.connected = asyncio.Event()
        self.access_token = None
        self.levelone_cache = {}

        self.levelone_equities_map = {
            '1': 'bid_price',
            '2': 'ask_price',
            '3': 'last_price',
            '4': 'bid_size',
            '5': 'ask_size',
            '8': 'total_volume',
            '10': 'high_price',
            '11': 'low_price',
            '12': 'close_price',
        }

    @property
    def current_streamer_config(self):
        return self.get_streamer_config()

    async def on_token_event(self, event: TokenEvent):
        self.access_token = event.access_token
        # ws连上后不会因为token过期而主动断开 
        if self.ws is None or not self.ws.open:
            self.streamer_config = self.current_streamer_config
            # 异步触发重连，不阻塞事件循环
            task = asyncio.create_task(self.connect_streamer())
            print(task)

    def get_streamer_config(self):
        if not self.access_token:
            raise RuntimeError("No access token available for streamer config")
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "accept": "application/json",
        }
        try:
            response = requests.get(
                self.schwab_streamer_config_url,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            config_json = response.json()
        except requests.RequestException as e:
            raise RuntimeError(f"Streamer config request failed: {e}") from e
        except ValueError as e:
            raise RuntimeError(f"Streamer config not JSON: {response.text}") from e

        info = config_json['streamerInfo'][0]
        print(f"✅ GET streamer config success")
        return {
            "streamerSocketUrl": info['streamerSocketUrl'],
            "schwabClientCustomerId": info['schwabClientCustomerId'],
            "schwabClientCorrelId": info['schwabClientCorrelId'],
            "schwabClientChannel": info['schwabClientChannel'],
            "schwabClientFunctionId": info['schwabClientFunctionId'],
            "Authorization": self.access_token
        }

    async def connect_streamer(self):
        async with self._ws_lock:
            # 关闭旧连接
            if self.ws and self.ws.open:
                await self.ws.close()

            # 连接新 ws
            self.ws = await websockets.connect(
                self.streamer_config["streamerSocketUrl"],
                ping_interval=50,
                ping_timeout=25,
                open_timeout=30
            )

            # 停掉旧 recv_task
            if self.recv_task and not self.recv_task.done():
                self.recv_task.cancel()

            # 后台启动 recv_streamer
            self.recv_task = asyncio.create_task(self.recv_streamer())
            print(self.recv_task)
            # 发送 login
            login_payload = {
                "service": "ADMIN",
                "command": "LOGIN",
                "requestid": 0,
                "SchwabClientCustomerId": self.streamer_config["schwabClientCustomerId"],
                "SchwabClientCorrelId": self.streamer_config["schwabClientCorrelId"],
                "parameters": {
                    "Authorization": self.streamer_config["Authorization"],
                    "SchwabClientChannel": self.streamer_config["schwabClientChannel"],
                    "SchwabClientFunctionId": self.streamer_config["schwabClientFunctionId"]
                }
            }
            await self.ws.send(json.dumps(login_payload))
            # 默认account activity task加入异步事件循环调度
            activity_task = asyncio.create_task(self.sub_account_activity())
            print(activity_task)

    async def sub_levelone_equities(self, symbols=None):
        symbols = symbols
        symbols = ",".join(symbols)
        await self.connected.wait()
        payload = {
            "service": "LEVELONE_EQUITIES",
            "command": "SUBS",
            "requestid": 0,
            "SchwabClientCustomerId": self.streamer_config["schwabClientCustomerId"],
            "SchwabClientCorrelId": self.streamer_config["schwabClientCorrelId"],
            "parameters": {
                "keys": symbols,
                "fields": "0,1,2,3,4,5,8,10,11,12"
            }
        }
        await self.ws.send(json.dumps(payload))

    def parse_levelone_equities_entry(self, entry):
        parsed = []
        timestamp = entry.get("timestamp")
        for content in entry.get("content", []):
            symbol = content.get("key")
            if symbol not in self.levelone_cache:
                self.levelone_cache[symbol] = {"symbol": symbol}
            stock = self.levelone_cache[symbol]

            # 基本信息
            for field in ["delayed", "assetMainType", "assetSubType", "cusip"]:
                if field not in stock or stock[field] is None:
                    stock[field] = content.get(field)
            
            stock["timestamp"] = timestamp

            # 映射字段
            for k, v in self.levelone_equities_map.items():
                val = content.get(k)
                if val is not None:
                    stock[v] = val

            parsed.append(stock.copy())
        return parsed

    async def recv_streamer(self):
        try:
            async for msg in self.ws:
                raw_data = json.loads(msg)
                # print(raw_data)
                # === response 类型消息（登录、订阅确认等） ===
                for r in raw_data.get("response", []):
                    service = r.get("service")
                    command = r.get("command")
                    content = r.get("content", {})

                    # 登录成功
                    if service == "ADMIN" and command == "LOGIN" and content.get("code") == 0:
                        print("✅ Streamer LOGIN success")
                        self.connected.set()

                    # SUBS 成功
                    elif command == "SUBS" and content.get("code") == 0:
                        print(f"✅ {service} SUBS command success")

                # === data 类型消息（LevelOne / Account Activity 推送） ===
                for entry in raw_data.get("data", []):
                    service = entry.get("service")
                    
                    if service == "LEVELONE_EQUITIES":
                        data = self.parse_levelone_equities_entry(entry)
                        if data:
                            # print(data)
                            await self.event_bus.publish(MarketEvent(data=data))

                    elif service == "ACCT_ACTIVITY":
                        print(entry)

        except asyncio.CancelledError:
            print("🔹 recv_streamer task cancelled")
        except websockets.exceptions.ConnectionClosed:
            print("⚠️ WS 连接断开")

    async def sub_account_activity(self):
        await self.connected.wait()
        payload = {
            "service": "ACCT_ACTIVITY",
            "command": "SUBS",
            "requestid": 0,
            "SchwabClientCustomerId": self.streamer_config["schwabClientCustomerId"],
            "SchwabClientCorrelId": self.streamer_config["schwabClientCorrelId"],
            "parameters": {
                "keys": "Account Activity",
                "fields": "0,1,2,3"
            }
        }
        await self.ws.send(json.dumps(payload))

    async def parse_account_activity_entry(self, entry):
        timestamp = entry.get("timestamp")
        for content in entry.get("content", []):
            order_status = content.get("2")
            if order_status == "OrderFillCompleted":
                order_fill_detail = content.get("3")
                order_id = order_fill_detail.get("SchwabOrderID")
                base_event = order_fill_detail.get("BaseEvent", {})
                order_leg_info = base_event.get("OrderFillCompletedEventOrderLegQuantityInfo", {})
                order_post_info = order_leg_info.get("OrderInfoForTransactionPosting", {})
                symbol = order_post_info.get("Symbol")
                signal = order_post_info.get("BuySellCode")
                order_type = order_post_info.get("OrderTypeCode")

                exec_info = order_leg_info.get("ExecutionInfo", {})
                # 成交数量（换算：lo / 10^signScale × 10^6）
                exec_qty = exec_info.get("ExecutionQuantity", {})
                fill_quantity = int(exec_qty.get("lo", 0)) / (10 ** int(exec_qty.get("signScale", 0))) * 10**6
                # 成交价格
                exec_price = exec_info.get("ExecutionPrice", {})
                fill_price = int(exec_price.get("lo", 0)) / (10 ** int(exec_price.get("signScale", 0))) * 10**6                
                # 实际手续费（无lo值则为0）
                exec_commission = exec_info.get("ActualChargedCommissionAmount", {})
                commission = int(exec_commission.get("lo", 0)) / (10 ** int(exec_commission.get("signScale", 0))) * 10**6
                                
                await self.event_bus.publish(FillEvent(symbol, timestamp, fill_quantity, signal, fill_price, commission, meta={"order_id": order_id, "order_type": order_type}))
            


async def main():
    access_token = "I0.b2F1dGgyLmJkYy5zY2h3YWIuY29t.zXqDvVQrYDa69ZUlMApCvkI5uR7dNEJ8KGcQVBdaiK4@"
    event_bus = EventBus()
    token_event = TokenEvent(access_token=access_token)
    data_stream = DataStream(event_bus)
    symbols = ["SGOV"]
    await data_stream.on_token_event(token_event)
    await data_stream.sub_levelone_equities(symbols)
    await asyncio.Event().wait()  # 永久阻塞

if __name__ == "__main__":
    asyncio.run(main())
