"""成交量 + MACD 交叉策略示例。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from lib.globalVar import getVar
from lib.logger import okx_logger, format_beijing_ts
from okx_api.tools.okx_indicator import OkxIndicator


# ===================== 可调参数（统一放在这里） =====================
VOLUME_LOOKBACK = 10  # 用于成交量比较与MACD检查的K线数量
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
MACD_WINDOW = 10  # 只在“前10根”内查找交叉
VOLUME_STRICT_GREATER = True  # True: 必须严格大于；False: 允许等于

# 为了计算 MACD，需要更多样本，确保“前10根”能拿到有效指标
MIN_CANDLES = max(VOLUME_LOOKBACK + 1, MACD_SLOW + MACD_SIGNAL + MACD_WINDOW + 2)


@dataclass
class CandleSlice:
    previous: Dict[str, float]
    prior_block: List[Dict[str, float]]
    tail_block: List[Dict[str, float]]


class strateg:  # noqa: N801  # 策略入口类名固定为 strateg
    """
    策略规则：
    1) 使用前一根已收盘K线作为判断对象（当前最新K线未收盘，不使用）。
    2) 若前一根成交量高于前10根K线的成交量，则继续判断：
       - 前10根K线中是否出现过 MACD 金叉或死叉。
    3) 若存在交叉：
       - 若出现过金叉且前一根为阳线 -> 返回 True
       - 否则（包含死叉或阴线）-> 返回 True
    """

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
        """策略执行入口。"""
        okx_logger.debug(
            "[策略][volume_macd_cross][开始] symbol=%s market=%s frame=%s latest_ts=%s",
            self._symbol,
            self._market_type,
            self._timeframe,
            self._format_ts(self._latest.get("timestamp")),
        )
        candles = self._get_recent_candles(limit=MIN_CANDLES)
        okx_logger.debug(
            "[策略][volume_macd_cross][样本读取] 期望=%d 实得=%d",
            MIN_CANDLES,
            len(candles),
        )
        if len(candles) < VOLUME_LOOKBACK + 1:
            okx_logger.warning(
                "[策略][volume_macd_cross] 样本不足 symbol=%s frame=%s size=%d",
                self._symbol,
                self._timeframe,
                len(candles),
            )
            return False

        slice_info = self._build_slice(candles)
        self._log_candle_summary(slice_info.previous, label="上一根")
        self._log_block_summary(slice_info.prior_block, label="前10根")
        if not self._is_volume_higher(slice_info.previous, slice_info.prior_block):
            okx_logger.debug("[策略][volume_macd_cross][成交量不满足] 退出")
            return False

        macd_map = self._build_macd_map(slice_info.tail_block)
        if not macd_map:
            okx_logger.debug("[策略][volume_macd_cross][MACD无有效数据] 退出")
            return False

        has_golden, has_death = self._detect_crosses(
            slice_info.prior_block,
            macd_map,
        )
        okx_logger.debug(
            "[策略][volume_macd_cross][交叉检测] 金叉=%s 死叉=%s",
            has_golden,
            has_death,
        )
        if not (has_golden or has_death):
            okx_logger.debug("[策略][volume_macd_cross][无交叉] 退出")
            return False

        is_bullish = self._is_bullish(slice_info.previous)
        okx_logger.debug(
            "[策略][volume_macd_cross][K线形态] 阳线=%s",
            is_bullish,
        )
        if has_golden and is_bullish:
            return True

        return True

    # ------------------------------------------------------------------
    # 数据获取与切片
    # ------------------------------------------------------------------

    def _get_recent_candles(self, limit: int) -> List[Dict[str, float]]:
        """
        从 KlineCache 获取已收盘K线。
        若缓存不足，缓存会自动从本地数据库补齐。
        """
        cache = getVar("KLINE_CACHE")
        if cache is None:
            okx_logger.warning(
                "[策略][volume_macd_cross][缓存缺失] symbol=%s market=%s frame=%s",
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
                "[策略][volume_macd_cross][缓存读取失败] symbol=%s market=%s frame=%s 错误=%s",
                self._symbol,
                self._market_type,
                self._timeframe,
                exc,
            )
            return []
        return [c for c in candles if isinstance(c, dict)]

    def _build_slice(self, candles: List[Dict[str, float]]) -> CandleSlice:
        """
        前一根K线为最近一根已收盘K线。
        prior_block 为“前一根之前的10根”。
        """
        previous = candles[-1]
        prior_block = candles[-(VOLUME_LOOKBACK + 1) : -1]
        tail_block = candles
        okx_logger.debug(
            "[策略][volume_macd_cross][切片结果] previous_ts=%s prior_count=%d tail_count=%d",
            self._format_ts(previous.get("timestamp")),
            len(prior_block),
            len(tail_block),
        )
        return CandleSlice(
            previous=previous,
            prior_block=prior_block,
            tail_block=tail_block,
        )

    # ------------------------------------------------------------------
    # 成交量与K线形态判断
    # ------------------------------------------------------------------

    def _is_volume_higher(
        self,
        previous: Dict[str, float],
        prior_block: List[Dict[str, float]],
    ) -> bool:
        """判断前一根成交量是否高于前10根K线。"""
        prev_vol = float(previous.get("volume", 0.0))
        okx_logger.debug(
            "[策略][volume_macd_cross][成交量比较] 前一根量=%.6f strict=%s",
            prev_vol,
            VOLUME_STRICT_GREATER,
        )
        for candle in prior_block:
            vol = float(candle.get("volume", 0.0))
            if VOLUME_STRICT_GREATER:
                if prev_vol <= vol:
                    okx_logger.debug(
                        "[策略][volume_macd_cross][成交量不满足] prev=%.6f compare=%.6f",
                        prev_vol,
                        vol,
                    )
                    return False
            else:
                if prev_vol < vol:
                    okx_logger.debug(
                        "[策略][volume_macd_cross][成交量不满足] prev=%.6f compare=%.6f",
                        prev_vol,
                        vol,
                    )
                    return False
        okx_logger.debug("[策略][volume_macd_cross][成交量满足] 通过")
        return True

    @staticmethod
    def _is_bullish(candle: Dict[str, float]) -> bool:
        """判断是否为阳线（收盘价 > 开盘价）。"""
        return float(candle.get("close", 0.0)) > float(candle.get("open", 0.0))

    # ------------------------------------------------------------------
    # MACD 计算与交叉检测
    # ------------------------------------------------------------------

    def _build_macd_map(self, candles: List[Dict[str, float]]) -> Dict[int, Tuple[float, float]]:
        """
        计算 MACD，并返回 timestamp -> (macd, signal) 映射。
        """
        try:
            indicator = OkxIndicator(candles)
            macd_data = indicator.calculate_macd(
                fast_period=MACD_FAST,
                slow_period=MACD_SLOW,
                signal_period=MACD_SIGNAL,
            )
        except Exception as exc:  # noqa: BLE001
            okx_logger.warning(
                "[策略][volume_macd_cross][MACD计算失败] symbol=%s frame=%s 错误=%s",
                self._symbol,
                self._timeframe,
                exc,
            )
            return {}

        macd = macd_data.get("macd") or []
        signal = macd_data.get("signal") or []
        ts_list = macd_data.get("timestamp") or []
        size = min(len(macd), len(signal), len(ts_list))
        okx_logger.debug(
            "[策略][volume_macd_cross][MACD结果] macd=%d signal=%d ts=%d used=%d",
            len(macd),
            len(signal),
            len(ts_list),
            size,
        )
        return {
            int(ts_list[i]): (float(macd[i]), float(signal[i]))
            for i in range(size)
        }

    def _detect_crosses(
        self,
        prior_block: List[Dict[str, float]],
        macd_map: Dict[int, Tuple[float, float]],
    ) -> Tuple[bool, bool]:
        """
        在前10根K线中寻找金叉或死叉。
        """
        has_golden = False
        has_death = False

        timestamps = [int(c.get("timestamp", 0)) for c in prior_block]
        okx_logger.debug(
            "[策略][volume_macd_cross][交叉扫描] 扫描根数=%d ts范围=%s~%s",
            len(timestamps),
            self._format_ts(timestamps[0]) if timestamps else None,
            self._format_ts(timestamps[-1]) if timestamps else None,
        )
        for idx in range(1, len(timestamps)):
            prev_ts = timestamps[idx - 1]
            curr_ts = timestamps[idx]
            if prev_ts not in macd_map or curr_ts not in macd_map:
                okx_logger.debug(
                    "[策略][volume_macd_cross][交叉扫描跳过] 缺少MACD ts=%s/%s",
                    self._format_ts(prev_ts),
                    self._format_ts(curr_ts),
                )
                continue
            prev_macd, prev_signal = macd_map[prev_ts]
            curr_macd, curr_signal = macd_map[curr_ts]
            okx_logger.debug(
                "[策略][volume_macd_cross][交叉对比] prev_ts=%s macd=%.6f sig=%.6f "
                "curr_ts=%s macd=%.6f sig=%.6f",
                self._format_ts(prev_ts),
                prev_macd,
                prev_signal,
                self._format_ts(curr_ts),
                curr_macd,
                curr_signal,
            )

            if prev_macd <= prev_signal and curr_macd > curr_signal:
                has_golden = True
                okx_logger.debug(
                    "[策略][volume_macd_cross][金叉发现] ts=%s",
                    self._format_ts(curr_ts),
                )
            if prev_macd >= prev_signal and curr_macd < curr_signal:
                has_death = True
                okx_logger.debug(
                    "[策略][volume_macd_cross][死叉发现] ts=%s",
                    self._format_ts(curr_ts),
                )

            if has_golden or has_death:
                break

        return has_golden, has_death

    # ------------------------------------------------------------------
    # 辅助日志
    # ------------------------------------------------------------------

    def _log_candle_summary(self, candle: Dict[str, float], *, label: str) -> None:
        okx_logger.debug(
            "[策略][volume_macd_cross][%s] ts=%s open=%.6f high=%.6f low=%.6f "
            "close=%.6f vol=%.6f",
            label,
            self._format_ts(candle.get("timestamp")),
            float(candle.get("open", 0.0)),
            float(candle.get("high", 0.0)),
            float(candle.get("low", 0.0)),
            float(candle.get("close", 0.0)),
            float(candle.get("volume", 0.0)),
        )

    def _log_block_summary(self, candles: List[Dict[str, float]], *, label: str) -> None:
        if not candles:
            okx_logger.debug("[策略][volume_macd_cross][%s] 空数据", label)
            return
        first = candles[0]
        last = candles[-1]
        volumes = [float(c.get("volume", 0.0)) for c in candles]
        closes = [float(c.get("close", 0.0)) for c in candles]
        okx_logger.debug(
            "[策略][volume_macd_cross][%s] count=%d ts=%s~%s",
            label,
            len(candles),
            self._format_ts(first.get("timestamp")),
            self._format_ts(last.get("timestamp")),
        )
        okx_logger.debug(
            "[策略][volume_macd_cross][%s] volume_min=%.6f volume_max=%.6f",
            label,
            min(volumes),
            max(volumes),
        )
        okx_logger.debug(
            "[策略][volume_macd_cross][%s] close_first=%.6f close_last=%.6f",
            label,
            closes[0],
            closes[-1],
        )

    @staticmethod
    def _format_ts(ts: object) -> str:
        """将毫秒时间戳格式化为北京时间字符串。"""
        return format_beijing_ts(ts)
