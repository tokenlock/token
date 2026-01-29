import os
import requests
from events import EventBus, TokenEvent

class Portfolio:
    def __init__(self, schwab_account_base_url="https://api.schwabapi.com/trader/v1/accounts"):
        self.schwab_account_base_url = schwab_account_base_url
        # 在类外subscribe 避免事件总线强绑定某个类
        # 该类无需publish 故不需要传入eventbus
        # self.event_bus.subscribe("TokenEvent", self.on_token_event)
        self.access_token = None
        self.account_number = None
        self.account_balance = None
        self.account_positions = None

    async def on_token_event(self, event: TokenEvent):
        self.access_token = event.access_token
        # account_number只获取一次
        if not self.account_number:
            self.account_number = self.get_account_number()

    def get_account_detail(self):
        self.account_balance, self.account_positions = self.get_balance_position()

    def request_schwab_account(self, schwab_account_url, params=None):
        headers = {
            "Authorization": f"Bearer {self.access_token}",  # Bearer+空格+token
            "accept": "application/json",
        }
        
        try:
            if params:
                response = requests.get(
                    schwab_account_url, 
                    headers=headers,
                    params=params, 
                    timeout=30
                )
            else:
                response = requests.get(
                    schwab_account_url, 
                    headers=headers,
                    timeout=30
                )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"嘉信Account接口请求失败: {str(e)}") from e
        # 解析响应并校验
        try:
            response = response.json()
            # print(response)
        except ValueError as e:
            raise ValueError(f"嘉信Account接口返回非JSON格式响应: {response.text}") from e
        return response
    
    def get_account_number(self):
        schwab_account_number_url = self.schwab_account_base_url + "/accountNumbers"
        response = self.request_schwab_account(schwab_account_number_url)
        account_number = response[0]["hashValue"]
        return account_number

    # 具体下单购买力检查由previewOrder去处理
    def get_balance_position(self):
        schwab_account_balance_position_url =  self.schwab_account_base_url + "/" + self.account_number
        # 返回balance 同时返回positions
        params = {
            "fields": "positions"
        }
        response = self.request_schwab_account(schwab_account_balance_position_url, params)
        balance, positions = self.parse_account_snapshot(response)
        return balance, positions

    def parse_account_snapshot(self, data):
        account = data["securitiesAccount"]

        # balance
        balance = account["currentBalances"]["availableFunds"]

        # positions
        positions = {}

        for pos in account.get("positions", []):
            instrument = pos["instrument"]
            symbol = instrument["symbol"]

            long_qty = pos.get("longQuantity", 0.0)
            short_qty = pos.get("shortQuantity", 0.0)
            net_qty = long_qty - short_qty

            positions[symbol] = {
                "assetType": instrument.get("assetType"),
                "longQty": long_qty,
                "shortQty": short_qty,
                "netQty": net_qty,
                "avgPrice": pos.get("averagePrice"),
                "marketValue": pos.get("marketValue"),
                "unrealizedPnL": (
                    pos.get("longOpenProfitLoss", 0.0)
                    + pos.get("shortOpenProfitLoss", 0.0)
                ),
                "extra": instrument
            }

        return balance, positions

    def on_fill_event(self, event):
        # 维护一个离线的balance加快查询速度
        pass


if __name__ == "__main__":
    # event_bus = EventBus()
    schwab_account_base_url = "https://api.schwabapi.com/trader/v1/accounts"
    portfolio = Portfolio(schwab_account_base_url)
    # event_bus.subscribe("TokenEvent", portfolio.on_token_event)
    portfolio.access_token = "I0.b2F1dGgyLmJkYy5zY2h3YWIuY29t.5jVykVPV5PONDsWreSJOpbZDcw_4fAVB6RmNaaWCR-E@"
    # print(portfolio.get_account_number())
    portfolio.account_number = "76202B2D1AA8092A5D32F69F527109D8C873E5210A3A5E81B3D239C7A32EFFC7"
    print(portfolio.get_balance_position())
