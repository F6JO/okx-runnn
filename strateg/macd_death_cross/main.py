"""基于 MACD 死叉判断的示例策略。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from lib.globalVar import getVar
from lib.logger import okx_logger
from okx_api.tools.okx_indicator import OkxIndicator
from okx_api.tools.okx_signals import OkxSignals


@dataclass
class CandleSlice:
    latest: Dict[str, float]
    previous: Dict[str, float] | None
    tail_block: List[Dict[str, float]]


class strateg:  # noqa: N801  # 策略入口类名固定为 strateg
    def __init__(
        self,
        *,
        symbol: str,
        market_type: str,
        timeframe: str,
        latest: Dict[str, float],
    ) -> None:
        self._symbol = symbol
        self._market_type = market_type
        self._timeframe = timeframe
        self._latest = dict(latest)

    def start(self) -> bool:
        """计算 MACD，并判断是否发生死叉。"""
        slice_info = self._build_slice(limit=300)
        if len(slice_info.tail_block) < 60:
            okx_logger.warning(
                "[策略][macd_death_cross] 样本不足，无法计算MACD symbol=%s frame=%s size=%d",
                self._symbol,
                self._timeframe,
                len(slice_info.tail_block),
            )
            return False

        try:
            indicator = OkxIndicator(slice_info.tail_block)
            macd_data = indicator.calculate_macd()
        except Exception as exc:  # noqa: BLE001
            okx_logger.warning(
                "[策略][macd_death_cross][MACD计算失败] symbol=%s frame=%s 错误=%s",
                self._symbol,
                self._timeframe,
                exc,
            )
            return False

        passed = OkxSignals.macd_death_cross(macd_data)
        if passed:
            macd = macd_data.get("macd") or []
            signal = macd_data.get("signal") or []
            hist = macd_data.get("hist") or []
            if macd and signal and hist:
                okx_logger.debug(
                    "[策略][macd_death_cross][触发] symbol=%s frame=%s MACD=%.6f SIGNAL=%.6f HIST=%.6f",
                    self._symbol,
                    self._timeframe,
                    macd[-1],
                    signal[-1],
                    hist[-1],
                )
        return passed

    def _build_slice(self, limit: int) -> CandleSlice:
        candles = self._get_recent_candles(limit=limit + 1)
        previous = candles[-2] if len(candles) >= 2 else None
        tail_block = candles[-limit:] if candles else []
        return CandleSlice(
            latest=self._latest,
            previous=previous,
            tail_block=tail_block,
        )

    def _get_recent_candles(self, limit: int) -> List[Dict[str, float]]:
        cache = getVar("KLINE_CACHE")
        if cache is None:
            okx_logger.warning(
                "[策略][macd_death_cross][缓存缺失] symbol=%s market=%s frame=%s",
                self._symbol,
                self._market_type,
                self._timeframe,
            )
            return []
        try:
            candles = cache.get_recent(
                self._symbol,
                self._market_type,
                self._timeframe,
                limit,
            )
        except Exception as exc:  # noqa: BLE001
            okx_logger.warning(
                "[策略][macd_death_cross][缓存读取失败] symbol=%s market=%s frame=%s 错误=%s",
                self._symbol,
                self._market_type,
                self._timeframe,
                exc,
            )
            return []
        return [c for c in candles if isinstance(c, dict)]

    def _log_slice(self, slice_info: CandleSlice) -> None:
        prev_close = slice_info.previous.get("close") if slice_info.previous else None
        okx_logger.debug(
            "[策略][macd_death_cross] symbol=%s frame=%s 最新收盘=%.6f 前一根收盘=%s 样本大小=%d",
            self._symbol,
            self._timeframe,
            float(slice_info.latest.get("close", 0.0)),
            f"{prev_close}" if prev_close is not None else "-",
            len(slice_info.tail_block),
        )
