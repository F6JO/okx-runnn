from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from lib.globalVar import getVar
from okx_api.tools.okx_format import OkxFormat
from lib.logger import okx_logger

if TYPE_CHECKING:
    from okx_api.okx_account import OkxAccount


class OkxPriceSnapshot:
    """负责输出交易对的行情快照与基础统计。"""

    def __init__(self, symbol: str, market_type: str):
        self.okx_account = self._resolve_account()
        self.exchange = self.okx_account.exchange
        self.symbol = symbol
        self.market_type = market_type

    def _resolve_account(self) -> "OkxAccount":
        account = getVar("OKX_ACCOUNT")
        if account is None:
            raise ValueError("全局 OKX 账户未设置，无法构建 OkxPriceSnapshot")
        return account

    def get_latest_price(self) -> Dict[str, Any]:
        """
        获取最新实时价格（优先使用 WebSocket 缓存，降级到 REST）。
        """
        if self.okx_account.enable_websocket:
            cached_ticker = self.okx_account.get_cached_ticker(self.symbol)
            if cached_ticker:
                return cached_ticker

        ticker = self.exchange.fetch_ticker(self.symbol)
        return {
            "symbol": ticker["symbol"],
            "last": ticker["last"],
            "bid": ticker["bid"],
            "ask": ticker["ask"],
            "timestamp": datetime.fromtimestamp(ticker["timestamp"] / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }

    def get_order_book(self, limit: int = 20) -> Dict[str, Any]:
        """
        获取订单簿信息（优先使用 WebSocket 缓存，降级到 REST）。
        """
        if self.okx_account.enable_websocket:
            cached_orderbook = self.okx_account.get_cached_orderbook(self.symbol)
            if cached_orderbook:
                bids = (
                    cached_orderbook["bids"][:limit]
                    if len(cached_orderbook["bids"]) > limit
                    else cached_orderbook["bids"]
                )
                asks = (
                    cached_orderbook["asks"][:limit]
                    if len(cached_orderbook["asks"]) > limit
                    else cached_orderbook["asks"]
                )
                return {
                    "bids": bids,
                    "asks": asks,
                    "timestamp": cached_orderbook["timestamp"],
                    "datetime": cached_orderbook["timestamp"],
                }

        order_book = self.exchange.fetch_order_book(
            self.symbol, limit=7000 if limit > 100 else limit
        )
        return {
            "bids": order_book["bids"],
            "asks": order_book["asks"],
            "timestamp": datetime.fromtimestamp(
                order_book["timestamp"] / 1000
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "datetime": order_book["datetime"],
        }

    def get_klines(
        self, timeframe: str = "1h", limit: int = 100, since: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        获取 K 线历史数据（使用 REST API）。
        """
        klines = self.exchange.fetch_ohlcv(
            symbol=self.symbol, timeframe=timeframe, limit=limit, since=since
        )

        formatted_klines = []
        for k in klines:
            formatted_klines.append(
                {
                    "timestamp": k[0],
                    "open": k[1],
                    "high": k[2],
                    "low": k[3],
                    "close": k[4],
                    "volume": k[5],
                }
            )

        return formatted_klines

    def get_order_book_analysis(self, limit: int = 100) -> Dict[str, Any]:
        """
        获取订单簿深度的分析结果。
        """
        order_book = self.get_order_book(limit=limit)
        order_book = OkxFormat.format_order_book(order_book, accuracy=0.5)
        current_price = self.get_latest_price()["last"]

        bids_threshold = self._get_large_order_threshold(order_book["bids"])
        asks_threshold = self._get_large_order_threshold(order_book["asks"])

        return {
            "raw_data": {
                "bids": order_book["bids"],
                "asks": order_book["asks"],
            },
            "analysis": {
                "buy_pressure_zones": self._analyze_pressure_zones(
                    order_book["bids"], current_price, is_bids=True
                ),
                "sell_pressure_zones": self._analyze_pressure_zones(
                    order_book["asks"], current_price, is_bids=False
                ),
                "large_orders": {
                    "bids": [
                        bid
                        for bid in order_book["bids"]
                        if float(bid[1]) >= bids_threshold
                    ],
                    "asks": [
                        ask
                        for ask in order_book["asks"]
                        if float(ask[1]) >= asks_threshold
                    ],
                    "thresholds": {
                        "bids": bids_threshold,
                        "asks": asks_threshold,
                    },
                },
            },
            "timestamp": order_book["timestamp"],
        }

    def get_trading_fee(self) -> Dict[str, Any]:
        """
        获取交易对手续费信息。
        """
        fee = self.exchange.fetch_trading_fee(self.symbol)

        result = {
            "symbol": self.symbol,
            "market_type": self.market_type,
            "maker": fee.get("maker"),
            "taker": fee.get("taker"),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        if self.market_type in ["swap", "futures"]:
            funding_info = self.exchange.fetch_funding_rate(self.symbol)
            funding_rate = funding_info.get("fundingRate")
            funding_rate_percent = (
                f"{funding_rate * 100:.4f}%" if funding_rate is not None else None
            )
            result.update(
                {
                    "funding_rate": funding_rate_percent,
                    "next_funding_time": datetime.fromtimestamp(
                        funding_info.get("nextFundingTime", 0) / 1000
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    if funding_info.get("nextFundingTime")
                    else None,
                    "funding_interval": "8h",
                }
            )

        return result

    def get_open_interest(self) -> Dict[str, Any]:
        """
        获取交易对未平仓量信息。
        """
        if self.market_type not in ["swap", "futures"]:
            raise ValueError("Open interest is only available for swap and futures markets")

        result = self.exchange.fetch_open_interest(self.symbol)
        if not isinstance(result, dict):
            raise ValueError("No data returned from API")

        open_interest = float(result.get("openInterest", result.get("oi", 0)))
        ts_raw = result.get("timestamp") or result.get("ts")
        if not ts_raw:
            raise ValueError("Open interest timestamp missing from API response")

        current_price = self.get_latest_price()["last"]
        timestamp = int(ts_raw)

        return {
            "symbol": self.symbol,
            "market_type": self.market_type,
            "open_interest": open_interest,
            "open_interest_value": open_interest * current_price,
            "timestamp": datetime.fromtimestamp(timestamp / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }

    def _get_large_order_threshold(self, orders: List[List[float]]) -> float:
        amounts = [float(order[1]) for order in orders]

        if not amounts:
            return 0

        mean_amount = sum(amounts) / len(amounts)
        std_amount = (sum((x - mean_amount) ** 2 for x in amounts) / len(amounts)) ** 0.5
        return mean_amount + std_amount

    def _analyze_pressure_zones(
        self,
        orders: List[List[float]],
        current_price: float,
        is_bids: bool,
        price_range_percent: float = 0.002,
    ) -> List[Dict[str, Any]]:
        if not orders:
            return []

        pressure_zones: List[Dict[str, Any]] = []
        current_zone = {
            "price_range": [orders[0][0], orders[0][0]],
            "total_amount": 0,
            "orders_count": 0,
        }

        for order in orders:
            try:
                if len(order) < 2:
                    continue

                price = float(order[0])
                amount = float(order[1])

                price_diff = abs(price - current_zone["price_range"][0]) / current_price

                if price_diff <= price_range_percent:
                    current_zone["price_range"][1] = price
                    current_zone["total_amount"] += amount
                    current_zone["orders_count"] += 1
                else:
                    if current_zone["orders_count"] > 0:
                        pressure_zones.append(current_zone.copy())
                    current_zone = {
                        "price_range": [price, price],
                        "total_amount": amount,
                        "orders_count": 1,
                    }
            except (IndexError, ValueError, TypeError) as exc:
                okx_logger.debug(f"处理订单数据时出错: {exc}, 订单数据: {order}")
                continue

        if current_zone["orders_count"] > 0:
            pressure_zones.append(current_zone.copy())

        pressure_zones.sort(key=lambda item: item["total_amount"], reverse=True)
        return pressure_zones[:5]
