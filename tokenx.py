import os
import time
import json
import base64
import asyncio
import requests
import websockets
from aiohttp import web
from datetime import datetime
from events import EventBus, TokenEvent

class TokenEngine:
    def __init__(self, event_bus, client_id, client_secret, auth_code_secret):
        self.event_bus = event_bus

        # 重定向callback
        self.redirect_uri = "https://tokenlock.cn/oauth/schwab/callback"
        # 连接cloudflare worker获取auth code
        self.auth_code_secret = auth_code_secret

        self.auth_code_url = "wss://tokenlock.cn/oauth/schwab/stream"
        
        self.client_id = client_id
        self.client_secret = client_secret
        self.schwab_token_url = "https://api.schwabapi.com/v1/oauth/token" 
        self.access_token = None
        self.refresh_token = None
        self.token_expires_in = None
        self.token_ready = asyncio.Event()
    
    async def get_auth_code(self):
        headers = {
                "X-WS-Password": self.auth_code_secret,  # 自定义头名（避免和默认头冲突）
                "Upgrade": "websocket",  # 显式声明WebSocket升级（可选，部分服务端需要）
                "Connection": "Upgrade"
            }
        
        max_retry = 100
        retry_count = 0
        retry_delay = 5
        prev_auth_code = None

        while retry_count<=max_retry:
            try:
                async with websockets.connect(self.auth_code_url, additional_headers=headers, ping_interval=50, ping_timeout=25) as ws:
                    print("✅ 已连接原生WebSocket")
                    retry_count = 0  # 连接成功，重置重试计数
                    retry_delay = 5  # 连接成功，重置重试延迟
                    async for msg in ws:
                        data = json.loads(msg)
                        print(f"🎉 收到code: {data['code']}")
                        current_auth_code = data['code']
                        if current_auth_code!=prev_auth_code:
                            prev_auth_code = current_auth_code
                            await self.get_token(current_auth_code)
                            # 先启动get_auth_code 再启动refresh
                            self.token_ready.set()

            except websockets.exceptions.ConnectionClosedError:
                print("⚠️ WebSocket连接被关闭，准备重连")
            except websockets.exceptions.InvalidURI:
                print(f"❌ WebSocket地址无效: {self.auth_code_url}")
                break  # 地址错误无需重试
            except Exception as e:
                print(f"❌ WebSocket连接失败: {str(e)}")
            
            # 重连前等待，避免高频重试
            retry_count += 1
            if retry_count <= max_retry:
                print(datetime.now(), f"🔄 第{retry_count}次重连，等待{retry_delay}秒...")
                await asyncio.sleep(retry_delay)
                # 每次重试增加一点延迟（指数退避），避免被服务器限制
                retry_delay = min(retry_delay * 1.5, 60)  # 最大延迟30秒
            else:
                print("❌ 达到最大重试次数，停止重连")
            

    def _get_basic_auth_headers(self):
        """
        生成嘉信API所需的Basic Auth请求头（私有辅助函数）
        :return: 包含Authorization和Content-Type的请求头字典
        """
        credentials = f"{self.client_id}:{self.client_secret}"
        base64_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
        return {
            "Authorization": f"Basic {base64_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def _request_schwab_token(self, payload):
        """
        通用的嘉信令牌请求函数（核心封装函数）
        负责向嘉信Token接口发送POST请求，处理响应并返回令牌字典
        :param payload: 令牌请求的核心参数（grant_type、code等）
        :return: 嘉信返回的令牌字典（包含access_token/refresh_token等）
        :raises: requests.exceptions.RequestException: 请求失败时抛出
        :raises: ValueError: 响应非JSON或缺少核心令牌字段时抛出
        """
        # 获取通用请求头
        headers = self._get_basic_auth_headers()
        timestamp = time.time()
        # 发送POST请求（增加超时和异常捕获）
        try:
            response = requests.post(
                url=self.schwab_token_url,
                headers=headers,
                data=payload,
                timeout=30  # 增加超时，避免无限等待
            )
            response.raise_for_status()  # 触发HTTP状态码异常（如401/500）
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"嘉信Token接口请求失败: {str(e)}") from e
        
        # 解析响应并校验
        try:
            token_dict = response.json()
        except ValueError as e:
            raise ValueError(f"嘉信Token接口返回非JSON格式响应: {response.text}") from e
        
        # 校验核心字段
        required_fields = ["access_token", "refresh_token"]
        missing_fields = [f for f in required_fields if f not in token_dict]
        if missing_fields:
            raise ValueError(f"嘉信Token响应缺少核心字段: {', '.join(missing_fields)}，响应内容：{token_dict}")
        
        # 记录Token过期时间
        self.token_expires_in = token_dict["expires_in"]
        # 用当前时间减去发起更新前时间算出来的时间差
        # 比如发起请求到返回用了4s 服务端更新时间小于该间隔 说明有效期留出了安全余量
        self.token_expires_in -=  time.time() - timestamp
        return token_dict

    async def get_token(self, auth_code):
        """
        通过授权码获取嘉信API令牌（首次获取Token）
        :param auth_code: 嘉信授权流程返回的authorization_code
        """
        payload = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": self.redirect_uri,
        }
        token_dict = self._request_schwab_token(payload)
        
        # 更新实例属性
        self.refresh_token = token_dict["refresh_token"]
        self.access_token = token_dict["access_token"]
        print(datetime.now(), "获取Token成功: 已获取新access_token")
        print(token_dict["access_token"])
        
        # 异步开启事件链
        await self.event_bus.publish(TokenEvent(access_token=self.access_token))

    async def async_refresh_token(self):
        """
        通过刷新令牌更新嘉信API令牌（Token过期时调用）
        :raises: RuntimeError: 未初始化refresh_token时抛出
        """
        await self.token_ready.wait()
        
        while True:
            await asyncio.sleep(self.token_expires_in)

            if not self.refresh_token:
                raise RuntimeError("刷新Token失败：refresh_token未初始化，请先调用get_token_by_auth_code")
                
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            }
            token_dict = self._request_schwab_token(payload)
            
            self.refresh_token = token_dict["refresh_token"]
            self.access_token = token_dict["access_token"]
            print(datetime.now(), "刷新Token成功: 已获取新access_token")
            print(token_dict["access_token"])
            await self.event_bus.publish(TokenEvent(access_token=self.access_token))

    # 最小化 HTTP server handler
    async def start_http_server(self):
        async def handle(request):
            # 自动等待 token ready
            await self.token_ready.wait()
            return web.json_response({
                "access_token": self.access_token
            })

        # web.Application 可直接加 routes
        app = web.Application()
        app.add_routes([web.get('/', handle)])

        # 监听本地内网
        web_runner = web.AppRunner(app)
        await web_runner.setup()
        site = web.TCPSite(web_runner, '127.0.0.1', 8080)
        await site.start()
        print("🔑 token HTTP server started at 127.0.0.1:8080")
        
    def start(self):
        # 定义要执行的异步任务列表
        tasks = [
            self.get_auth_code(),
            self.async_refresh_token(),
            self.start_http_server()
        ]
        
        # 逐个创建后台任务，加入事件循环
        for coro in tasks:
            task = asyncio.create_task(coro)
            print(task)

async def main():
    client_id = os.getenv("APP_KEY")
    client_secret = os.getenv("APP_SECRET")
    auth_code_secret = os.getenv("STREAM_SECRET")
    event_bus = EventBus()
    token_engine = TokenEngine(event_bus, client_id, client_secret, auth_code_secret)
    # start这里是同步函数 create task瞬间执行完成 不会阻塞
    token_engine.start()
    # 永久阻塞 避免create task后直接结束
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
    