from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from lib.globalVar import getVar
from okx_api.okx_account import OkxAccount
from okx_api.okx_history_data import OkxHistoryData
from okx_api.okx_market_watch import OkxMarketWatch
from okx_api.okx_price_snapshot import OkxPriceSnapshot
from okx_api.okx_trading_status import OkxTradingStatus


class OkxCoin:
    """
    统一的 OKX 价格门面类。

    - `OkxPriceSnapshot`：行情快照与统计数据。
    - `OkxTradingStatus`：账户在该交易对下的订单与持仓信息。
    - `OkxMarketWatch`：实时订阅、监控与预警能力。
    - `OkxHistoryData`：历史数据抓取与持久化管理。

    通过 `get_snapshot() / get_trading_status() / get_market_watch()` 获取对应业务模块，
    同时保留常用方法的直接调用以兼容旧接口。
    """

    def __init__(
        self,
        symbol: str,
        market_type: Optional[str] = None,
    ):
        """
        初始化价格查询类

        参数:
            symbol: 交易对符号（可仅传入币种名，如 'BTC' 或 'eth'，默认使用 USDT 计价）
            market_type: 交易类型（如 'spot', 'swap' 等），默认沿用账户设置
        """
        self.okx_account = self._resolve_account()
        self.exchange = self.okx_account.exchange
        self.market_type = market_type or self.okx_account.market_type
        self.symbol = self._normalize_symbol(symbol)

        self._snapshot: Optional[OkxPriceSnapshot] = None
        self._trading_status: Optional[OkxTradingStatus] = None
        self._market_watch: Optional[OkxMarketWatch] = None
        self._history: Optional[OkxHistoryData] = None

    def _resolve_account(self) -> OkxAccount:
        account = getVar("OKX_ACCOUNT")
        if account is None:
            raise ValueError("全局 OKX 账户未设置，无法构建 OkxCoin")
        return account

    def _normalize_symbol(self, raw_symbol: str) -> str:
        """
        将用户传入的 symbol 统一转换为 OKX 需要的标准格式
        """
        if raw_symbol is None or not str(raw_symbol).strip():
            raise ValueError("symbol 不能为空")

        symbol = str(raw_symbol).strip().upper()

        # 已经是标准格式，直接返回
        if (
            '/' in symbol
            or ':' in symbol
            or symbol.endswith('-SWAP')
        ):
            return symbol

        # 仅传入了币种名时，默认拼接 USDT
        quote_asset = 'USDT'
        if self.market_type in {'swap', 'future', 'futures'}:
            return f"{symbol}/{quote_asset}:USDT"
        return f"{symbol}/{quote_asset}"

    # ========== 模块访问 ==========

    def get_snapshot(self) -> OkxPriceSnapshot:
        """获取行情快照模块。"""
        if self._snapshot is None:
            self._snapshot = OkxPriceSnapshot(
                symbol=self.symbol,
                market_type=self.market_type,
            )
        return self._snapshot

    def get_trading_status(self) -> OkxTradingStatus:
        """获取交易状态模块。"""
        if self._trading_status is None:
            self._trading_status = OkxTradingStatus(
                symbol=self.symbol,
                market_type=self.market_type,
            )
        return self._trading_status

    def get_market_watch(self) -> OkxMarketWatch:
        """获取实时监控模块。"""
        if self._market_watch is None:
            self._market_watch = OkxMarketWatch(
                symbol=self.symbol,
                market_type=self.market_type,
            )
        return self._market_watch

    def get_history(self) -> OkxHistoryData:
        """获取历史数据模块。"""
        if self._history is None:
            self._history = OkxHistoryData(
                symbol=self.symbol,
                market_type=self.market_type,
            )
        return self._history

    # ========== 向后兼容：行情数据 ==========

    def get_latest_price(self) -> Dict[str, Any]:
        return self.get_snapshot().get_latest_price()

    def get_order_book(self, limit: int = 20) -> Dict[str, Any]:
        return self.get_snapshot().get_order_book(limit=limit)

    def get_klines(
        self, timeframe: str = "1h", limit: int = 100, since: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return self.get_snapshot().get_klines(
            timeframe=timeframe, limit=limit, since=since
        )

    def get_order_book_analysis(self, limit: int = 100) -> Dict[str, Any]:
        return self.get_snapshot().get_order_book_analysis(limit=limit)

    def get_trading_fee(self) -> Dict[str, Any]:
        return self.get_snapshot().get_trading_fee()

    def get_open_interest(self) -> Dict[str, Any]:
        return self.get_snapshot().get_open_interest()

    # ========== 向后兼容：交易状态 ==========

    def get_open_orders(self) -> List[Dict[str, Any]]:
        return self.get_trading_status().get_open_orders()

    def get_closed_orders(self) -> List[Dict[str, Any]]:
        return self.get_trading_status().get_closed_orders()

    def get_positions(self) -> List[Dict[str, Any]]:
        return self.get_trading_status().get_positions()

    # ========== 向后兼容：实时监控 ==========

    def subscribe_ticker(self) -> bool:
        return self.get_market_watch().subscribe_ticker()

    def subscribe_orderbook(self, depth: str = "books") -> bool:
        return self.get_market_watch().subscribe_orderbook(depth=depth)

    def subscribe_trades(self) -> bool:
        return self.get_market_watch().subscribe_trades()

    def subscribe_kline(self, timeframe: str = "1m") -> bool:
        return self.get_market_watch().subscribe_kline(timeframe=timeframe)

    def subscribe_all_basic_data(self, orderbook_depth: str = "books") -> Dict[str, bool]:
        return self.get_market_watch().subscribe_all_basic_data(orderbook_depth=orderbook_depth)

    def unsubscribe_ticker(self) -> bool:
        return self.get_market_watch().unsubscribe_ticker()

    def unsubscribe_orderbook(self) -> bool:
        return self.get_market_watch().unsubscribe_orderbook()

    def unsubscribe_trades(self) -> bool:
        return self.get_market_watch().unsubscribe_trades()

    def unsubscribe_all(self) -> Dict[str, bool]:
        return self.get_market_watch().unsubscribe_all()

    def wait_for_data(
        self, data_types: Optional[List[str]] = None, timeout: int = 10
    ) -> bool:
        return self.get_market_watch().wait_for_data(data_types=data_types, timeout=timeout)

    def start_real_time_price_monitoring(
        self, callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        self.get_market_watch().start_real_time_price_monitoring(callback)

    def start_real_time_orderbook_monitoring(
        self, callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        self.get_market_watch().start_real_time_orderbook_monitoring(callback)

    def start_real_time_trades_monitoring(
        self, callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        self.get_market_watch().start_real_time_trades_monitoring(callback)

    def get_real_time_trades(self, count: int = 100) -> List[Dict[str, Any]]:
        return self.get_market_watch().get_real_time_trades(count=count)

    def monitor_price_alerts(
        self,
        upper_threshold: Optional[float] = None,
        lower_threshold: Optional[float] = None,
        callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> None:
        self.get_market_watch().monitor_price_alerts(
            upper_threshold=upper_threshold,
            lower_threshold=lower_threshold,
            callback=callback,
        )

    def monitor_large_orders(
        self,
        threshold_amount: float,
        callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> None:
        self.get_market_watch().monitor_large_orders(
            threshold_amount=threshold_amount,
            callback=callback,
        )

    def get_real_time_volume_analysis(self) -> Dict[str, Any]:
        return self.get_market_watch().get_real_time_volume_analysis()

    def close(self) -> None:
        """关闭 WebSocket 连接（兼容旧接口）。"""
        if self.okx_account and self.okx_account.enable_websocket:
            self.okx_account.close()


if __name__ == "__main__":
    # 测试代码已移除，请使用独立的测试脚本
    pass
