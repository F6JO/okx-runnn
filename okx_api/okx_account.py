import ccxt
import json
import asyncio
import threading
import functools
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Callable, List, Tuple
from datetime import datetime
import time
import copy
import socket
from urllib.parse import urlparse

import socks
from websockets.legacy.client import connect as ws_connect

from lib.logger import okx_logger
from lib.globalVar import getVar, setVar


@dataclass
class _WebSocketState:
    path: str
    url: str
    ws: Optional[Any] = None
    is_connected: bool = False
    loop: Optional[asyncio.AbstractEventLoop] = None
    thread: Optional[threading.Thread] = None
    subscription_registry: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    subscription_lock: threading.Lock = field(default_factory=threading.Lock)
    pending_acks: Dict[str, threading.Event] = field(default_factory=dict)
    ack_results: Dict[str, bool] = field(default_factory=dict)
    ack_messages: Dict[str, str] = field(default_factory=dict)
    ack_lock: threading.Lock = field(default_factory=threading.Lock)
    should_run: bool = True

class OkxAccount:
    """
    统一的OKX账户管理类
    支持REST API和WebSocket两种模式
    
    使用方式：
    1. 只使用REST API（默认）：enable_websocket=False
    2. 同时使用REST + WebSocket：enable_websocket=True
    """
    def update_from(self, new_account: "OkxAccount") -> None:
        """
        用另一个 OkxAccount 实例的状态覆盖当前实例。

        参数:
            new_account: 已初始化好的 OkxAccount。
        """
        if not isinstance(new_account, OkxAccount):
            raise TypeError("update_from 仅支持 OkxAccount 实例")

        # 同步代理配置
        new_proxy = getattr(new_account, "proxy", None)
        self._apply_proxy(new_proxy)

        fields_to_copy = [
            "market_type",
            "api_key",
            "api_secret",
            "password",
            "testnet",
            "enable_websocket",
            "exchange",
            "data_cache",
            "callbacks",
            "_okx_price_instances",
            "_ws_states",
        ]

        for field in fields_to_copy:
            if hasattr(new_account, field):
                setattr(self, field, getattr(new_account, field))

    def _apply_proxy(self, proxy: Optional[str]) -> None:
        normalized = (proxy or "").strip() or None
        if normalized and normalized.lower().startswith("socks5://"):
            normalized = "socks5h://" + normalized[9:]
        self.proxy = normalized
        setVar("HTTPS_PROXY", normalized)

    def _ws_address_for(self, url: str) -> Tuple[str, int, bool]:
        parsed = urlparse(url)
        host = parsed.hostname or "ws.okx.com"
        secure = parsed.scheme.lower() == "wss"
        port = parsed.port or (443 if secure else 80)
        return host, port, secure

    def _parse_proxy_config(self) -> Optional[Dict[str, Any]]:
        if not self.proxy:
            return None

        parsed = urlparse(self.proxy)
        scheme = (parsed.scheme or "").lower()
        host = parsed.hostname
        port = parsed.port
        username = parsed.username
        password = parsed.password

        if not host:
            okx_logger.error(f"代理配置无效: {self.proxy}")
            return None

        if scheme in {"http", "https"}:
            proxy_type = socks.HTTP
            port = port or 8080
        elif scheme in {"socks5", "socks5h"}:
            proxy_type = socks.SOCKS5
            port = port or 1080
        elif scheme in {"socks4", "socks4a"}:
            proxy_type = socks.SOCKS4
            port = port or 1080
        else:
            okx_logger.error(f"不支持的代理协议: {scheme or '未指定'}")
            return None

        return {
            "type": proxy_type,
            "host": host,
            "port": port,
            "username": username,
            "password": password,
        }

    def _create_proxied_socket(self, url: str) -> socket.socket:
        proxy_cfg = self._parse_proxy_config()
        if proxy_cfg is None:
            raise ValueError("代理配置无效，无法建立连接")

        host, port, _ = self._ws_address_for(url)

        sock = socks.socksocket()
        sock.set_proxy(
            proxy_cfg["type"],
            proxy_cfg["host"],
            proxy_cfg["port"],
            username=proxy_cfg["username"],
            password=proxy_cfg["password"],
        )
        sock.settimeout(10)
        sock.connect((host, port))
        sock.setblocking(False)
        return sock

    async def _build_ws_connect_kwargs(self, url: str) -> Dict[str, Any]:
        if not self.proxy:
            return {}

        loop = asyncio.get_running_loop()
        try:
            sock = await loop.run_in_executor(
                None, functools.partial(self._create_proxied_socket, url)
            )
        except Exception as exc:
            okx_logger.warning(f"创建代理连接失败: {exc}")
            raise

        host, _, secure = self._ws_address_for(url)
        kwargs: Dict[str, Any] = {"sock": sock}
        if secure:
            kwargs.setdefault("server_hostname", host)
        return kwargs

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        password: str,
        testnet: bool = False,
        market_type: str = "spot",
        proxy: Optional[str] = None,
        enable_websocket: bool = False,
    ):
        """
        初始化账户管理

        参数:
            api_key: API密钥
            api_secret: API秘钥
            password: API密码短语
            testnet: 是否使用测试网
            market_type: 市场类型 ('spot', 'swap', 'futures', 'margin')
            proxy: 代理设置
            enable_websocket: 是否启用WebSocket（默认False，只使用REST）
        """
        self.market_type = market_type
        self.api_key = api_key
        self.api_secret = api_secret
        self.password = password
        self.testnet = testnet
        self.enable_websocket = enable_websocket
        self._okx_price_instances = {}
        retries_cfg = getVar("API_MAX_RETRIES", 3)
        try:
            self._max_retries = max(int(retries_cfg), 1)
        except (TypeError, ValueError):
            self._max_retries = 3
        self._ws_states: Dict[str, _WebSocketState] = {}
        self.ws_ready_event = threading.Event()

        proxy_value = proxy if proxy not in (None, "") else getVar("HTTPS_PROXY")
        self._apply_proxy(proxy_value)

        exchange_config = {
            "apiKey": api_key,
            "secret": api_secret,
            "password": password,
            "enableRateLimit": True,
        }
        if self.proxy:
            exchange_config["proxies"] = {"http": self.proxy, "https": self.proxy}

        self.exchange = ccxt.okx(exchange_config)

        if testnet:
            self.exchange.set_sandbox_mode(True)

        # WebSocket相关（仅在启用时初始化）
        self.data_cache = None
        self.callbacks = None

        if enable_websocket:
            self._init_websocket()
        else:
            self.ws_ready_event.set()
    
    def _init_websocket(self):
        """初始化WebSocket相关组件"""
        # 数据缓存
        self.data_cache = {
            'tickers': {},
            'orderbooks': {},
            'trades': {},
            'candles': {},
        }
        
        # 回调函数
        self.callbacks = {
            'ticker': [],
            'orderbook': [],
            'trades': [],
            'candles': [],
        }

    def _normalize_ws_path(self, ws_path: Optional[str]) -> str:
        path = (ws_path or "/ws/v5/public").strip()
        if not path.startswith("/"):
            path = f"/{path}"
        return path

    def _base_ws_origin(self) -> str:
        return "wss://wspap.okx.com:8443" if self.testnet else "wss://ws.okx.com:8443"

    def _build_ws_url(self, ws_path: str) -> str:
        return f"{self._base_ws_origin()}{ws_path}"

    def _ensure_ws_state(self, ws_path: Optional[str]) -> Optional[_WebSocketState]:
        if not self.enable_websocket:
            okx_logger.warning("WebSocket未启用，无法建立连接状态")
            return None

        normalized_path = self._normalize_ws_path(ws_path)
        state = self._ws_states.get(normalized_path)
        if state is None:
            url = self._build_ws_url(normalized_path)
            state = _WebSocketState(path=normalized_path, url=url)
            self._ws_states[normalized_path] = state
        self._start_websocket(state)
        return state

    @property
    def is_connected(self) -> bool:
        return any(state.is_connected for state in self._ws_states.values())
    
    def _start_websocket(self, state: _WebSocketState) -> None:
        """启动指定路径的 WebSocket 连接"""
        if not self.enable_websocket:
            okx_logger.warning("WebSocket未启用，请在初始化时设置enable_websocket=True")
            return

        if state.thread and state.thread.is_alive():
            return

        state.should_run = True
        state.thread = threading.Thread(
            target=self._run_async_loop,
            args=(state,),
            daemon=True,
        )
        state.thread.start()

        timeout = 10
        start_time = time.time()
        while state.should_run and not state.is_connected and (time.time() - start_time) < timeout:
            time.sleep(0.1)

        if not state.is_connected:
            okx_logger.warning(
                f"WebSocket路径 {state.path} 在规定时间内未连接成功"
            )

    def send_ws_message(
        self,
        payload: Dict[str, Any],
        *,
        ws_path: Optional[str] = None,
        track: bool = False,
        wait_ack: bool = False,
        ack_timeout: float = 5.0,
    ) -> bool:
        """
        发送 WebSocket 消息，保持账户层只负责连接和消息传输

        参数:
            payload: 需要发送的消息体
            ws_path: 指定要使用的 WebSocket 路径（默认 public）
            track: 是否记录订阅状态，以便断线重连后自动恢复

        返回:
            bool: 消息是否成功发送
        """
        if not self.enable_websocket:
            okx_logger.warning("WebSocket未启用，无法发送消息")
            return False

        state = self._ensure_ws_state(ws_path)
        if state is None:
            return False

        if not state.is_connected or not state.loop or not state.ws:
            okx_logger.warning(
                f"WebSocket路径 {state.path} 未就绪，无法发送消息"
            )
            return False

        ack_key = None
        ack_event = None
        if wait_ack:
            ack_key = self._make_subscription_key(payload)
            if ack_key is not None:
                ack_event = threading.Event()
                with state.ack_lock:
                    state.pending_acks[ack_key] = ack_event
                    state.ack_results.pop(ack_key, None)
                    state.ack_messages.pop(ack_key, None)

        try:
            asyncio.run_coroutine_threadsafe(
                state.ws.send(json.dumps(payload)),
                state.loop,
            )
            if track and not wait_ack:
                self._track_subscription(state, payload)
        except Exception as e:
            if ack_key is not None:
                with state.ack_lock:
                    state.pending_acks.pop(ack_key, None)
                    state.ack_results.pop(ack_key, None)
                    state.ack_messages.pop(ack_key, None)
            okx_logger.error(f"发送WebSocket消息失败: {e}")
            return False

        if not wait_ack or ack_event is None:
            return True

        if not ack_event.wait(timeout=ack_timeout):
            with state.ack_lock:
                state.pending_acks.pop(ack_key, None)
                state.ack_results.pop(ack_key, None)
                state.ack_messages.pop(ack_key, None)
            okx_logger.warning(
                f"订阅确认超时 (路径 {state.path}) payload={payload}"
            )
            return False

        with state.ack_lock:
            success = state.ack_results.pop(ack_key, False)
            message = state.ack_messages.pop(ack_key, "")
            state.pending_acks.pop(ack_key, None)

        if success and track:
            self._track_subscription(state, payload)
        elif not success and message:
            okx_logger.warning(f"订阅确认失败: {message}")

        return success

    def _run_async_loop(self, state: _WebSocketState) -> None:
        """运行异步事件循环"""
        state.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(state.loop)
        state.loop.run_until_complete(self._connect(state))
    
    async def _connect(self, state: _WebSocketState) -> None:
        """建立指定路径的 WebSocket 连接"""
        backoff_base = 5
        while self.enable_websocket and state.should_run:
            self.ws_ready_event.clear()
            for attempt in range(1, self._max_retries + 1):
                sock_ref = None
                try:
                    connect_kwargs: Dict[str, Any] = {}
                    if self.proxy:
                        connect_kwargs = await self._build_ws_connect_kwargs(state.url)
                        sock_ref = connect_kwargs.get("sock")

                    state.ws = await ws_connect(state.url, **connect_kwargs)
                    state.is_connected = True
                    self.ws_ready_event.set()
                    okx_logger.info(
                        f"WebSocket连接成功 (路径 {state.path}, 尝试 {attempt}/{self._max_retries})"
                    )

                    await self._resubscribe_all(state)

                    await asyncio.gather(
                        self._handle_messages(state),
                        self._heartbeat(state),
                    )
                    break  # 连接正常结束，准备重连或退出
                except Exception as e:
                    state.is_connected = False
                    self.ws_ready_event.clear()
                    okx_logger.warning(
                        f"WebSocket连接失败 (路径 {state.path}, 尝试 {attempt}/{self._max_retries}): {e}"
                    )
                    if sock_ref:
                        try:
                            sock_ref.close()
                        except Exception:
                            pass
                    if attempt < self._max_retries:
                        await asyncio.sleep(min(backoff_base * attempt, 30))
                    else:
                        okx_logger.error("WebSocket连接达到最大重试次数，停止尝试")
                        self.ws_ready_event.clear()
                        return

            if not self.enable_websocket or not state.should_run:
                break

            if state.ws:
                try:
                    await state.ws.close()
                except Exception:
                    pass
                finally:
                    state.ws = None
            self.ws_ready_event.clear()

            state.is_connected = False
            okx_logger.warning(
                f"WebSocket连接中断 (路径 {state.path})，10 秒后尝试重新连接"
            )
            await asyncio.sleep(10)

        state.is_connected = False
        self.ws_ready_event.clear()
    
    async def _handle_messages(self, state: _WebSocketState) -> None:
        """处理WebSocket消息"""
        try:
            async for message in state.ws:
                if message.strip() and message != 'pong':
                    data = json.loads(message)
                    await self._process_message(state, data)
        except json.JSONDecodeError as e:
            okx_logger.error(f"JSON解析错误: {e}")
            raise
        except Exception as e:
            okx_logger.warning(f"WebSocket连接错误(路径 {state.path}): {e}")
            raise
    
    async def _process_message(self, state: _WebSocketState, data):
        """处理收到的消息"""
        event = data.get('event')
        if event in {'subscribe', 'unsubscribe', 'error'}:
            self._resolve_subscription_ack(state, data)
            if event == 'subscribe':
                okx_logger.info(f"订阅成功: {data}")
            elif event == 'unsubscribe':
                okx_logger.info(f"取消订阅成功: {data}")
            else:
                okx_logger.warning(f"订阅错误: {data}")
            return
            
        # 检查是否有数据
        if 'data' not in data:
            okx_logger.debug(f"收到非数据消息: {data}")
            return
            
        # 处理数据消息
        arg = data.get('arg', {})
        channel = arg.get('channel')
        
        # okx_logger.debug(f"收到数据推送 - 频道: {channel}, 数据量: {len(data['data'])}")
        
        for item in data['data']:
            if channel == 'tickers':
                self._update_ticker_cache(item)
            elif channel.startswith('books'):  # 处理所有订单簿频道
                self._update_orderbook_cache(item)
            elif channel == 'trades':
                self._update_trades_cache(item)
            elif channel.startswith('candle') or channel.startswith('mark-price-candle'):
                self._update_candle_cache(arg, channel, item)
    
    def _update_ticker_cache(self, data):
        """更新价格缓存"""
        okx_symbol = data.get('instId')  # OKX格式：SOL-USDT-SWAP
        if okx_symbol:
            # 转换回标准格式用作key
            if '-SWAP' in okx_symbol:
                # SOL-USDT-SWAP -> SOL/USDT:USDT
                base_quote = okx_symbol.replace('-SWAP', '').replace('-', '/')
                standard_symbol = f"{base_quote}:USDT"
            else:
                # SOL-USDT -> SOL/USDT
                standard_symbol = okx_symbol.replace('-', '/')
            
            ticker_data = {
                'symbol': standard_symbol,
                'last': float(data.get('last', 0)),
                'bid': float(data.get('bidPx', 0)),
                'ask': float(data.get('askPx', 0)),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 使用标准格式作为key存储
            self.data_cache['tickers'][standard_symbol] = ticker_data
            okx_logger.debug(f"更新价格缓存: {standard_symbol} = {ticker_data['last']}")
            
            # 触发回调
            for callback in self.callbacks.get('ticker', []):
                callback(ticker_data)
    
    def _update_orderbook_cache(self, data):
        """更新订单簿缓存"""
        okx_symbol = data.get('instId')  # OKX格式：SOL-USDT-SWAP
        if okx_symbol:
            # 转换回标准格式用作key
            if '-SWAP' in okx_symbol:
                # SOL-USDT-SWAP -> SOL/USDT:USDT
                base_quote = okx_symbol.replace('-SWAP', '').replace('-', '/')
                standard_symbol = f"{base_quote}:USDT"
            else:
                # SOL-USDT -> SOL/USDT
                standard_symbol = okx_symbol.replace('-', '/')
            
            self.data_cache['orderbooks'][standard_symbol] = {
                'bids': data.get('bids', []),
                'asks': data.get('asks', []),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 触发回调
            for callback in self.callbacks.get('orderbook', []):
                callback(self.data_cache['orderbooks'][standard_symbol])
    
    def _update_trades_cache(self, data):
        """更新成交数据缓存"""
        okx_symbol = data.get('instId')  # OKX格式：SOL-USDT-SWAP
        if okx_symbol:
            # 转换回标准格式用作key
            if '-SWAP' in okx_symbol:
                # SOL-USDT-SWAP -> SOL/USDT:USDT
                base_quote = okx_symbol.replace('-SWAP', '').replace('-', '/')
                standard_symbol = f"{base_quote}:USDT"
            else:
                # SOL-USDT -> SOL/USDT
                standard_symbol = okx_symbol.replace('-', '/')
            
            if standard_symbol not in self.data_cache['trades']:
                self.data_cache['trades'][standard_symbol] = []
                
            trade_data = {
                'price': float(data.get('px', 0)),
                'volume': float(data.get('sz', 0)),
                'side': data.get('side'),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 保持最近1000条记录
            self.data_cache['trades'][standard_symbol].append(trade_data)
            if len(self.data_cache['trades'][standard_symbol]) > 1000:
                self.data_cache['trades'][standard_symbol] = self.data_cache['trades'][standard_symbol][-1000:]
                
            # 触发回调
            for callback in self.callbacks.get('trades', []):
                callback(trade_data)

    def _update_candle_cache(self, arg: Dict[str, Any], channel: str, item: Any) -> None:
        """更新 K 线缓存"""
        inst_id = arg.get('instId') or arg.get('uly')
        if not inst_id:
            return

        if '-SWAP' in inst_id:
            base_quote = inst_id.replace('-SWAP', '').replace('-', '/')
            standard_symbol = f"{base_quote}:USDT"
        else:
            standard_symbol = inst_id.replace('-', '/')

        # OKX 返回的数据可能是嵌套列表或直接列表
        raw_values = item
        if isinstance(item, dict):
            raw_values = (
                item.get('candle')
                or item.get('candles')
                or item.get('markPriceCandle')
                or item.get('markPriceCandles')
                or item.get('data')
                or item
            )

        if isinstance(raw_values, list) and raw_values and isinstance(raw_values[0], list):
            latest_point = raw_values[0]
        else:
            latest_point = raw_values

        prefix = 'mark-price-candle' if channel.startswith('mark-price-candle') else 'candle'
        timeframe = channel[len(prefix):] if len(channel) > len(prefix) else ''

        cache_key = f"{channel}:{inst_id}"
        candle_entry = {
            'symbol': standard_symbol,
            'instId': inst_id,
            'channel': channel,
            'timeframe': timeframe,
            'is_mark_price': prefix == 'mark-price-candle',
            'values': latest_point,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

        self.data_cache['candles'][cache_key] = candle_entry
        for callback in self.callbacks.get('candles', []):
            callback(candle_entry)

    async def _heartbeat(self, state: _WebSocketState) -> None:
        """心跳保持连接"""
        while self.enable_websocket and state.should_run and state.ws:
            try:
                await state.ws.send('ping')
                await asyncio.sleep(30)
            except Exception as exc:
                okx_logger.warning(
                    f"WebSocket心跳失败(路径 {state.path}): {exc}"
                )
                raise
    
    # ========== WebSocket订阅方法（仅在enable_websocket=True时可用） ==========
    
    # ========== REST API方法（始终可用） ==========
    
    def get_balance(self) -> Dict[str, Any]:
        """
        获取账户余额
        
        返回:
            包含账户余额信息的字典
        """
        return self.exchange.fetch_balance()

    def _normalize_symbol_for_price(self, raw_symbol: str, market_type: str) -> str:
        """
        将用户输入的 symbol 统一转换为标准格式，用于实例复用
        """
        if raw_symbol is None or not str(raw_symbol).strip():
            raise ValueError("symbol 不能为空")
        
        symbol = str(raw_symbol).strip().upper()
        if '/' in symbol or ':' in symbol or symbol.endswith('-SWAP'):
            return symbol
        
        quote_asset = 'USDT'
        if market_type in {'swap', 'future', 'futures'}:
            return f"{symbol}/{quote_asset}:USDT"
        return f"{symbol}/{quote_asset}"

    def get_OkxCoin(self, 
                     symbol: str, 
                     market_type: Optional[str] = None, 
                     use_cache: bool = True) -> "OkxCoin":
        """
        创建或获取 OkxCoin 实例
        
        参数:
            symbol: 币种或交易对符号，支持 'BTC' / 'BTC/USDT' 等格式
            market_type: 交易类型，默认沿用当前账户设置
            use_cache: 是否复用已有实例，默认开启
            
        返回:
            OkxCoin 实例
        """
        resolved_market_type = market_type or self.market_type
        cache_key = None
        if use_cache:
            if not hasattr(self, '_okx_price_instances'):
                self._okx_price_instances = {}
            normalized_symbol = self._normalize_symbol_for_price(symbol, resolved_market_type)
            cache_key = (normalized_symbol, resolved_market_type)
            cached_instance = self._okx_price_instances.get(cache_key)
            if cached_instance:
                return cached_instance
        
        # 避免循环导入，将引用放在方法内部
        from okx_api.okx_coin import OkxCoin
        
        price_instance = OkxCoin(
            symbol=symbol,
            market_type=resolved_market_type
        )
        
        if use_cache and cache_key is not None:
            self._okx_price_instances[cache_key] = price_instance
        
        return price_instance

    # ========== WebSocket数据缓存访问（仅在enable_websocket=True时可用） ==========
    
    def get_cached_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取缓存的价格数据"""
        if not self.enable_websocket or not self.data_cache:
            return None
        return self.data_cache['tickers'].get(symbol)
        
    def get_cached_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取缓存的订单簿数据"""
        if not self.enable_websocket or not self.data_cache:
            return None
        return self.data_cache['orderbooks'].get(symbol)
        
    def get_cached_trades(self, symbol: str) -> Optional[List[Dict[str, Any]]]:
        """获取缓存的成交数据"""
        if not self.enable_websocket or not self.data_cache:
            return []
        return self.data_cache['trades'].get(symbol, [])

    def get_cached_candles(
        self,
        symbol: str,
        *,
        channel_prefix: str = 'candle',
        timeframe: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取缓存的 K 线数据"""
        if not self.enable_websocket or not self.data_cache:
            return []

        results: List[Dict[str, Any]] = []
        target_prefix = channel_prefix
        target_tf = (timeframe or '').lower()

        for entry in self.data_cache['candles'].values():
            entry_symbol = entry.get('symbol')
            if entry_symbol != symbol:
                continue
            entry_prefix = 'mark-price-candle' if entry.get('is_mark_price') else 'candle'
            if entry_prefix != target_prefix:
                continue
            entry_tf = (entry.get('timeframe') or '').lower()
            if target_tf and entry_tf != target_tf:
                continue
            results.append(entry)
        return results
    
    def add_callback(self, data_type: str, callback: Callable):
        """添加数据更新回调函数"""
        if not self.enable_websocket or not self.callbacks:
            okx_logger.warning("WebSocket未启用，无法添加回调")
            return
        if data_type not in self.callbacks:
            self.callbacks[data_type] = []
        self.callbacks[data_type].append(callback)
    
    def close(self):
        """关闭所有 WebSocket 连接"""
        for state in self._ws_states.values():
            state.should_run = False
            if state.ws and state.loop:
                asyncio.run_coroutine_threadsafe(state.ws.close(), state.loop)

    def _make_subscription_key(self, payload: Dict[str, Any]) -> Optional[str]:
        args = payload.get("args")
        return self._make_subscription_key_from_args(args)

    def _make_subscription_key_from_args(
        self,
        args: Optional[List[Dict[str, Any]]],
    ) -> Optional[str]:
        if not args:
            return None
        try:
            normalized = json.dumps(args, sort_keys=True, separators=(",", ":"))
        except TypeError:
            return None
        return normalized

    def _resolve_subscription_ack(
        self,
        state: _WebSocketState,
        data: Dict[str, Any],
    ) -> None:
        arg = data.get("arg")
        if not isinstance(arg, dict):
            return

        key = self._make_subscription_key_from_args([arg])
        if key is None:
            return

        event_name = data.get("event")
        success = event_name in {"subscribe", "unsubscribe"}
        message = ""
        if not success:
            code = data.get("code")
            msg = data.get("msg")
            parts = [part for part in (code, msg) if part not in (None, "")]
            message = " ".join(str(part) for part in parts)

        with state.ack_lock:
            event = state.pending_acks.get(key)
            if event is None:
                return
            state.ack_results[key] = success
            state.ack_messages[key] = message
            event.set()

    def _track_subscription(self, state: _WebSocketState, payload: Dict[str, Any]) -> None:
        op = payload.get("op")
        key = self._make_subscription_key(payload)
        if key is None:
            return
        lock = getattr(state, "subscription_lock", None)
        target = state.subscription_registry
        if lock:
            lock.acquire()
        try:
            if op == "subscribe":
                target[key] = copy.deepcopy(
                    {
                        "op": "subscribe",
                        "args": payload.get("args", []),
                    }
                )
            elif op == "unsubscribe":
                target.pop(key, None)
        finally:
            if lock:
                lock.release()

    async def _resubscribe_all(self, state: _WebSocketState) -> None:
        if not state.subscription_registry or not state.ws:
            return
        okx_logger.info(
            f"WebSocket重连：恢复订阅 {len(state.subscription_registry)} 项 (路径 {state.path})"
        )
        lock = getattr(state, "subscription_lock", None)
        if lock:
            lock.acquire()
        try:
            registry = list(state.subscription_registry.values())
        finally:
            if lock:
                lock.release()
        for payload in registry:
            try:
                await state.ws.send(json.dumps(payload))
                await asyncio.sleep(0.05)
            except Exception as exc:
                okx_logger.error(f"重放订阅失败: {exc}")


if __name__ == "__main__":
    # 测试代码已移除，请使用独立的测试脚本
    pass
