from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

from lib.globalVar import getVar
from lib.logger import okx_logger

if TYPE_CHECKING:
    from okx_api.okx_account import OkxAccount


class OkxMarketWatch:
    """负责订阅、监控与分析实时市场行为。"""

    CHANNEL_REGISTRY: Dict[str, Dict[str, Any]] = {
        "ticker": {
            "ws_path": "/ws/v5/public",
            "builder": "_build_ticker_args",
            "defaults": {},
        },
        "orderbook": {
            "ws_path": "/ws/v5/public",
            "builder": "_build_orderbook_args",
            "defaults": {"depth": "books"},
        },
        "trades": {
            "ws_path": "/ws/v5/public",
            "builder": "_build_trades_args",
            "defaults": {},
        },
        "candle": {
            "ws_path": "/ws/v5/business",
            "builder": "_build_candle_args",
            "defaults": {"timeframe": "1m"},
        },
    }

    def __init__(self, symbol: str, market_type: str):
        self.okx_account = self._resolve_account()
        self.symbol = symbol
        self.market_type = market_type
        self.subscribed_orderbook_depth: Optional[str] = None
        self._active_subscriptions: Dict[str, Dict[str, Any]] = {}

    def _resolve_account(self) -> "OkxAccount":
        account = getVar("OKX_ACCOUNT")
        if account is None:
            raise ValueError("全局 OKX 账户未设置，无法构建 OkxMarketWatch")
        return account

    def _prepare_channel_payload(
        self, key: str, params: Dict[str, Any]
    ) -> Optional[Tuple[Dict[str, Any], str, Dict[str, Any]]]:
        config = self.CHANNEL_REGISTRY.get(key)
        if not config:
            okx_logger.error(f"未知的订阅键: {key}")
            return None

        merged = {**config.get("defaults", {}), **params}
        builder_name = config.get("builder")
        builder = getattr(self, builder_name, None)
        if builder is None:
            okx_logger.error(f"订阅 {key} 缺少构造函数 {builder_name}")
            return None

        try:
            args = builder(merged)
        except Exception as exc:
            okx_logger.error(f"构造订阅参数失败 ({key}): {exc}")
            return None

        ws_path = config.get("ws_path", "/ws/v5/public")
        return args, ws_path, merged

    def _build_ticker_args(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "channel": "tickers",
            "instId": self._format_symbol_for_ws(),
        }

    def _build_orderbook_args(self, params: Dict[str, Any]) -> Dict[str, Any]:
        depth = params.get("depth")
        if not depth:
            raise ValueError("订单簿订阅需要指定 depth")
        return {
            "channel": depth,
            "instId": self._format_symbol_for_ws(),
        }

    def _build_trades_args(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "channel": "trades",
            "instId": self._format_symbol_for_ws(),
        }

    def _normalize_ws_timeframe(self, timeframe: str) -> str:
        normalized = (timeframe or "").strip()
        if not normalized:
            raise ValueError("K线订阅需要提供 timeframe 参数")
        mapping = getattr(self.okx_account.exchange, "timeframes", {}) or {}
        return mapping.get(normalized, normalized)

    def _build_candle_args(self, params: Dict[str, Any]) -> Dict[str, Any]:
        timeframe = self._normalize_ws_timeframe(params.get("timeframe"))
        return {
            "channel": f"candle{timeframe}",
            "instId": self._format_symbol_for_ws(),
        }

    # ========== 订阅/取消订阅 ==========

    def subscribe_channel(self, key: str, **params: Any) -> bool:
        if not self.okx_account.enable_websocket:
            okx_logger.warning("WebSocket 未启用，无法发起订阅")
            return False

        prepared = self._prepare_channel_payload(key, params)
        if not prepared:
            return False

        args, ws_path, merged = prepared
        payload = {"op": "subscribe", "args": [args]}

        okx_logger.debug(f"订阅频道 {key}: {payload}")
        success = self.okx_account.send_ws_message(
            payload,
            ws_path=ws_path,
            track=True,
            wait_ack=True,
        )
        if success:
            self._active_subscriptions[key] = merged
            if key == "orderbook":
                self.subscribed_orderbook_depth = merged.get("depth", "books")
        return success

    def unsubscribe_channel(self, key: str, **params: Any) -> bool:
        if not self.okx_account.enable_websocket:
            return False

        if params:
            effective_params = dict(params)
        else:
            effective_params = dict(self._active_subscriptions.get(key, {}))

        prepared = self._prepare_channel_payload(key, effective_params)
        if not prepared:
            return False

        args, ws_path, _ = prepared
        payload = {"op": "unsubscribe", "args": [args]}

        okx_logger.debug(f"取消订阅频道 {key}: {payload}")
        success = self.okx_account.send_ws_message(
            payload,
            ws_path=ws_path,
            track=True,
            wait_ack=True,
        )
        if success:
            self._active_subscriptions.pop(key, None)
            if key == "orderbook":
                self.subscribed_orderbook_depth = None
        return success

    def subscribe_ticker(self) -> bool:
        """订阅价格数据流。"""
        success = self.subscribe_channel("ticker")
        if success:
            okx_logger.info(f"已订阅 {self.symbol} 的价格数据流")
        return success

    def subscribe_orderbook(self, depth: str = "books") -> bool:
        """订阅订单簿数据流。"""
        success = self.subscribe_channel("orderbook", depth=depth)
        if success:
            okx_logger.info(
                f"已订阅 {self.symbol} 的订单簿数据流 (深度: {depth})"
            )
        return success

    def subscribe_trades(self) -> bool:
        """订阅成交数据流。"""
        success = self.subscribe_channel("trades")
        if success:
            okx_logger.info(f"已订阅 {self.symbol} 的成交数据流")
        return success

    def subscribe_kline(self, timeframe: str = "1m") -> bool:
        """订阅 K 线数据流。"""
        success = self.subscribe_channel("candle", timeframe=timeframe)
        if success:
            okx_logger.info(f"已订阅 {self.symbol} 的K线数据流 (周期: {timeframe})")
        return success

    def subscribe_all_basic_data(self, orderbook_depth: str = "books") -> Dict[str, bool]:
        """一键订阅价格、订单簿、成交三类基础数据。"""
        results = {
            "ticker": self.subscribe_ticker(),
            "orderbook": self.subscribe_orderbook(orderbook_depth),
            "trades": self.subscribe_trades(),
        }
        okx_logger.info(
            f"基础数据流订阅完成: {sum(results.values())}/3 成功"
        )
        return results

    def unsubscribe_ticker(self) -> bool:
        """取消订阅价格数据流。"""
        success = self.unsubscribe_channel("ticker")
        if success:
            okx_logger.info(f"已取消订阅 {self.symbol} 的价格数据流")
        return success

    def unsubscribe_orderbook(self) -> bool:
        """取消订阅订单簿数据流。"""
        success = self.unsubscribe_channel("orderbook")
        if success:
            okx_logger.info(f"已取消订阅 {self.symbol} 的订单簿数据流")
        return success

    def unsubscribe_trades(self) -> bool:
        """取消订阅成交数据流。"""
        success = self.unsubscribe_channel("trades")
        if success:
            okx_logger.info(f"已取消订阅 {self.symbol} 的成交数据流")
        return success

    def unsubscribe_all(self) -> Dict[str, bool]:
        """取消订阅全部基础数据。"""
        results = {
            "ticker": self.unsubscribe_ticker(),
            "orderbook": self.unsubscribe_orderbook(),
            "trades": self.unsubscribe_trades(),
        }
        okx_logger.info(f"取消订阅完成: {sum(results.values())}/3 成功")
        return results

    # ========== 等待数据 ==========

    def wait_for_data(
        self, data_types: List[str] = None, timeout: int = 10
    ) -> bool:
        """
        等待指定类型的数据到达缓存。
        """
        if not self.okx_account.enable_websocket:
            okx_logger.warning("WebSocket 未启用，无法等待实时数据")
            return False

        data_types = data_types or ["ticker", "orderbook"]
        okx_logger.info(
            f"等待数据到达: {', '.join(data_types)} (超时: {timeout}秒)"
        )

        start_time = time.time()
        while (time.time() - start_time) < timeout:
            all_ready = True
            for data_type in data_types:
                if data_type == "ticker":
                    if not self.okx_account.get_cached_ticker(self.symbol):
                        all_ready = False
                        break
                elif data_type == "orderbook":
                    if not self.okx_account.get_cached_orderbook(self.symbol):
                        all_ready = False
                        break
                elif data_type == "trades":
                    if not self.okx_account.get_cached_trades(self.symbol):
                        all_ready = False
                        break

            if all_ready:
                okx_logger.info("所有数据已就绪")
                return True

            time.sleep(0.1)

        okx_logger.warning("等待数据超时")
        return False

    # ========== 实时监控 ==========

    def start_real_time_price_monitoring(
        self, callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        """启动实时价格回调。"""
        if not self.okx_account.enable_websocket:
            okx_logger.warning("WebSocket 未启用，无法启动实时价格监控")
            return

        self.okx_account.add_callback("ticker", callback)

    def start_real_time_orderbook_monitoring(
        self, callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        """启动实时订单簿回调。"""
        if not self.okx_account.enable_websocket:
            okx_logger.warning("WebSocket 未启用，无法启动实时订单簿监控")
            return

        self.okx_account.add_callback("orderbook", callback)

    def start_real_time_trades_monitoring(
        self, callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        """启动实时成交回调。"""
        if not self.okx_account.enable_websocket:
            okx_logger.warning("WebSocket 未启用，无法启动实时成交监控")
            return

        self.okx_account.add_callback("trades", callback)

    def start_real_time_candle_monitoring(
        self,
        callback: Callable[[Dict[str, Any]], None],
    ) -> None:
        """启动实时 K 线回调。"""
        if not self.okx_account.enable_websocket:
            okx_logger.warning("WebSocket 未启用，无法启动 K 线监控")
            return

        self.okx_account.add_callback("candles", callback)

    def get_real_time_trades(self, count: int = 100) -> List[Dict[str, Any]]:
        """获取最近的实时成交记录。"""
        if not self.okx_account.enable_websocket:
            okx_logger.warning("WebSocket 未启用，无法获取实时成交数据")
            return []

        trades = self.okx_account.get_cached_trades(self.symbol)
        return trades[-count:] if trades else []

    def get_real_time_candles(
        self,
        *,
        timeframe: str,
        mark_price: bool = False,
    ) -> List[Dict[str, Any]]:
        """获取缓存的实时 K 线数据。"""
        if not self.okx_account.enable_websocket:
            okx_logger.warning("WebSocket 未启用，无法获取实时 K 线数据")
            return []

        prefix = 'mark-price-candle' if mark_price else 'candle'
        return self.okx_account.get_cached_candles(
            self.symbol,
            channel_prefix=prefix,
            timeframe=timeframe,
        )

    # ========== 智能监控与分析 ==========

    def monitor_price_alerts(
        self,
        upper_threshold: Optional[float] = None,
        lower_threshold: Optional[float] = None,
        callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> None:
        """设置价格上下限预警。"""
        if not self.okx_account.enable_websocket:
            okx_logger.warning("WebSocket 未启用，无法启动价格预警")
            return

        def price_alert_handler(price_data: Dict[str, Any]) -> None:
            current_price = price_data.get("last", 0)
            if upper_threshold and current_price >= upper_threshold:
                if callback:
                    callback("UPPER_ALERT", price_data)
                else:
                    okx_logger.warning(
                        f"价格预警: {self.symbol} 价格达到上限 {current_price}"
                    )
            elif lower_threshold and current_price <= lower_threshold:
                if callback:
                    callback("LOWER_ALERT", price_data)
                else:
                    okx_logger.warning(
                        f"价格预警: {self.symbol} 价格达到下限 {current_price}"
                    )

        self.start_real_time_price_monitoring(price_alert_handler)

    def monitor_large_orders(
        self,
        threshold_amount: float,
        callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> None:
        """监控订单簿大单。"""
        if not self.okx_account.enable_websocket:
            okx_logger.warning("WebSocket 未启用，无法启动大单监控")
            return

        def large_order_handler(orderbook_data: Dict[str, Any]) -> None:
            for bid in orderbook_data.get("bids", [])[:10]:
                if len(bid) >= 2 and float(bid[1]) >= threshold_amount:
                    if callback:
                        callback(
                            "LARGE_BID",
                            {"price": bid[0], "amount": bid[1], "orderbook": orderbook_data},
                        )
                    else:
                        okx_logger.info(f"发现大买单: 价格 {bid[0]}, 数量 {bid[1]}")

            for ask in orderbook_data.get("asks", [])[:10]:
                if len(ask) >= 2 and float(ask[1]) >= threshold_amount:
                    if callback:
                        callback(
                            "LARGE_ASK",
                            {"price": ask[0], "amount": ask[1], "orderbook": orderbook_data},
                        )
                    else:
                        okx_logger.info(f"发现大卖单: 价格 {ask[0]}, 数量 {ask[1]}")

        self.start_real_time_orderbook_monitoring(large_order_handler)

    def get_real_time_volume_analysis(self) -> Dict[str, Any]:
        """输出实时成交量分析结果。"""
        if not self.okx_account.enable_websocket:
            okx_logger.warning("WebSocket 未启用，无法进行实时成交量分析")
            return {}

        trades = self.get_real_time_trades(count=1000)
        if not trades:
            return {}

        buy_volume = sum(float(t["volume"]) for t in trades if t.get("side") == "buy")
        sell_volume = sum(float(t["volume"]) for t in trades if t.get("side") == "sell")
        total_volume = buy_volume + sell_volume

        recent_trades = [
            t
            for t in trades
            if (
                datetime.now()
                - datetime.strptime(t["timestamp"], "%Y-%m-%d %H:%M:%S")
            ).seconds
            < 300
        ]

        return {
            "total_volume": total_volume,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "buy_sell_ratio": buy_volume / sell_volume if sell_volume > 0 else float("inf"),
            "recent_trades_count": len(recent_trades),
            "trades_per_minute": len(recent_trades) / 5 if recent_trades else 0,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    # ========== 工具 ==========

    def _format_symbol_for_ws(self) -> str:
        """将统一格式的 symbol 转换为 OKX WebSocket 所需的 instId。"""
        if ":" in self.symbol:
            base_quote = self.symbol.split(":")[0].replace("/", "-")
            return f"{base_quote}-SWAP"
        return self.symbol.replace("/", "-")
