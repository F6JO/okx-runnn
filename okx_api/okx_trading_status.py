from __future__ import annotations

from typing import Any, Dict, List, TYPE_CHECKING

from lib.globalVar import getVar
if TYPE_CHECKING:
    from okx_api.okx_account import OkxAccount


class OkxTradingStatus:
    """负责输出账户在当前交易对下的订单与持仓信息。"""

    def __init__(self, symbol: str, market_type: str):
        self.okx_account = self._resolve_account()
        self.exchange = self.okx_account.exchange
        self.symbol = symbol
        self.market_type = market_type

    def _resolve_account(self) -> "OkxAccount":
        account = getVar("OKX_ACCOUNT")
        if account is None:
            raise ValueError("全局 OKX 账户未设置，无法构建 OkxTradingStatus")
        return account

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """获取未完成订单列表。"""
        self.exchange.options["defaultType"] = self.market_type
        return self.exchange.fetch_open_orders(symbol=self.symbol)

    def get_closed_orders(self) -> List[Dict[str, Any]]:
        """获取已完成订单列表。"""
        self.exchange.options["defaultType"] = self.market_type
        return self.exchange.fetch_closed_orders(symbol=self.symbol)

    def get_positions(self) -> List[Dict[str, Any]]:
        """获取当前持仓信息（仅合约市场生效）。"""
        self.exchange.options["defaultType"] = self.market_type

        if self.market_type == "spot":
            return []

        positions = self.exchange.fetch_positions(self.symbol)
        return [
            pos for pos in positions if float(pos.get("contracts", 0) or 0) > 0
        ]
