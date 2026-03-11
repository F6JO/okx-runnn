"""策略模板，用于开发者编写新策略。

本文件适合复制后改名使用。
请保持目录名与策略名一致，入口文件为 main.py，类名固定为 strateg。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Dict, List

from lib.globalVar import getVar
from lib.logger import okx_logger
from okx_api.tools.okx_indicator import OkxIndicator
from okx_api.tools.okx_signals import OkxSignals

# 模板开关：仅在指定级别执行策略。
# - 设为 None: 所有级别都执行（默认）
# - 设为 "5m": 仅 5m 执行，其他级别快速跳过
RUN_ONLY_TIMEFRAME: str | None = None


@dataclass
class CandleSlice:
    latest: Dict[str, float]
    previous: Dict[str, float] | None
    tail_block: List[Dict[str, float]]


class strateg:  # noqa: N801  # 策略入口类名固定为 strateg
    """
    模板策略：
        1) 从KlineCache获取最近蜡烛图
        2) 计算指标
        3) 根据信号返回True/False
        备注：
        - 控制器注入：symbol，market_type，timeframe，latest。
        - 策略必须实现start(self) -> bool。
        - 您可以通过lib.globalVar.getVar访问共享服务。
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
        # 可选：如果只想在某个级别执行，可在这里做统一守卫。
        # 例如 frames=5m|15m 时，只让 5m 执行核心逻辑。
        if RUN_ONLY_TIMEFRAME and self._timeframe != RUN_ONLY_TIMEFRAME:
            okx_logger.debug(
                "[策略][strategy_template][跳过] 仅%s执行 symbol=%s market=%s 当前级别=%s",
                RUN_ONLY_TIMEFRAME,
                self._symbol,
                self._market_type,
                self._timeframe,
            )
            return True

        # 统一从缓存读取数据切片：
        # 1) recent candles 用于指标计算
        # 2) previous candle 用于和最新信号做比较（可选）
        slice_info = self._build_slice(limit=300)
        if len(slice_info.tail_block) < 60:
            okx_logger.warning(
                "[策略][strategy_template] 样本不足，跳过计算 symbol=%s frame=%s size=%d",
                self._symbol,
                self._timeframe,
                len(slice_info.tail_block),
            )
            return False

        # 这里演示指标计算基本流程。
        # OkxIndicator 的最小字段要求：
        # timestamp、open、high、low、close、volume。
        try:
            indicator = OkxIndicator(slice_info.tail_block)
            macd_data = indicator.calculate_macd()
        except Exception as exc:  # noqa: BLE001
            okx_logger.warning(
                "[策略][strategy_template][指标计算失败] symbol=%s frame=%s 错误=%s",
                self._symbol,
                self._timeframe,
                exc,
            )
            return False

        # 示例：调用内置信号判断。
        # 也可以直接用 macd_data 自己写逻辑。
        passed = OkxSignals.macd_golden_cross(macd_data)
        if passed:
            okx_logger.debug(
                "[策略][strategy_template][触发] symbol=%s frame=%s",
                self._symbol,
                self._timeframe,
            )
        return passed

    def _build_slice(self, limit: int) -> CandleSlice:
        # 读取 limit+1 根，便于额外拿到“上一根”。
        # 约定：
        # - previous: 倒数第二根（可选用于交叉确认）
        # - tail_block: 最近 limit 根（用于指标计算）
        candles = self._get_recent_candles(limit=limit + 1)
        previous = candles[-2] if len(candles) >= 2 else None
        tail_block = candles[-limit:] if candles else []
        return CandleSlice(
            latest=self._latest,
            previous=previous,
            tail_block=tail_block,
        )

    def _get_recent_candles(
        self,
        limit: int,
        timeframe: str | None = None,
    ) -> List[Dict[str, float]]:
        # 数据读取封装：
        # - 默认读取当前策略级别 self._timeframe
        # - 传入 timeframe 可跨级别读取（如在 5m 策略里读取 15m）
        # KlineCache 在 monitor run 时预热；缓存不足时会尝试从 SQLite 回填。
        target_timeframe = (timeframe or self._timeframe).strip()
        if not target_timeframe:
            target_timeframe = self._timeframe

        cache = getVar("KLINE_CACHE")
        if cache is None:
            okx_logger.warning(
                "[策略][strategy_template][缓存缺失] symbol=%s market=%s frame=%s",
                self._symbol,
                self._market_type,
                target_timeframe,
            )
            return []
        try:
            candles = cache.get_recent(
                self._symbol,
                self._market_type,
                target_timeframe,
                limit,
            )
        except Exception as exc:  # noqa: BLE001
            okx_logger.warning(
                "[策略][strategy_template][缓存读取失败] symbol=%s market=%s frame=%s 错误=%s",
                self._symbol,
                self._market_type,
                target_timeframe,
                exc,
            )
            return []
        # 模板层做一次类型过滤，避免脏数据影响策略逻辑。
        return [c for c in candles if isinstance(c, dict)]

    # ------------------------------------------------------------------
    # 额外示例（供开发者复制改造）
    # ------------------------------------------------------------------

    def _example_access_latest_candle(self) -> float:
        """
        示例：访问最新一根 K 线（由 WebSocket 推送）。
        控制器在实例化策略时注入 latest。
        """
        return float(self._latest.get("close", 0.0))

    def _example_custom_signal(self, candles: List[Dict[str, float]]) -> bool:
        """
        示例：一个简单的自定义规则。
        - 若最新收盘价高于上一根收盘价，则返回 True。
        """
        if len(candles) < 2:
            return False
        return float(candles[-1].get("close", 0.0)) > float(
            candles[-2].get("close", 0.0)
        )

    def _example_fetch_shorter_window(self) -> List[Dict[str, float]]:
        """
        示例1：只读取最近 50 根，用于更轻量的计算。
        """
        return self._get_recent_candles(limit=50)

    def _example_fetch_other_timeframe(self) -> List[Dict[str, float]]:
        """
        示例2：在当前策略中读取其他级别（例如 15m）。
        常用于多级别共振判断。
        """
        return self._get_recent_candles(limit=200, timeframe="15m")

    def _format_candle_timestamps(
        self,
        candles: List[Dict[str, float]],
    ) -> List[Dict[str, Any]]:
        """
        将K线列表中的 timestamp 从毫秒时间戳转换为字符串格式。
        输出格式：YYYY-MM-DD HH:MM:SS（北京时间）。
        """
        beijing_tz = timezone(timedelta(hours=8))
        formatted: List[Dict[str, Any]] = []
        for candle in candles:
            if not isinstance(candle, dict):
                continue
            item: Dict[str, Any] = dict(candle)
            ts_raw = item.get("timestamp")
            try:
                ts_ms = float(ts_raw)
            except (TypeError, ValueError):
                formatted.append(item)
                continue

            item["timestamp"] = datetime.fromtimestamp(
                ts_ms / 1000,
                tz=beijing_tz,
            ).strftime("%Y-%m-%d %H:%M:%S")
            formatted.append(item)
        return formatted

    def _candles_to_matrix_without_confirm(
        self,
        candles: List[Dict[str, Any]],
    ) -> List[List[Any]]:
        """
        将K线字典列表转换为二维数组，并移除每根K线中的 confirm 字段。
        输入建议为 _format_candle_timestamps 的返回结果。
        第一行固定为标头。
        """
        ordered_fields = [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "volume_contract",
            "volume_currency",
            "volume_quote",
        ]
        matrix: List[List[Any]] = [list(ordered_fields)]

        for candle in candles:
            if not isinstance(candle, dict):
                continue
            item = dict(candle)
            item.pop("confirm", None)
            row = [item.get(field) for field in ordered_fields]
            matrix.append(row)
        return matrix
