from __future__ import annotations

from typing import Dict, List


class OkxSignals:
    """常用指标信号助手，直接基于 OkxIndicator 的输出进行判定。"""

    @staticmethod
    def macd_golden_cross(
        macd_data: Dict[str, List[float]],
        *,
        confirm_bars: int = 0,
        hist_threshold: float = 0.0,
    ) -> bool:
        """
        判断 MACD 金叉是否成立。

        参数:
            macd_data: OkxIndicator.calculate_macd() 的返回值。
            confirm_bars:
                交叉后要求持续维持的根数。
                - 0 表示刚发生交叉（当前这根就是交叉那根）。
                - N 表示交叉发生在 N 根之前，并且从交叉那根到当前这根一直维持金叉状态。
            hist_threshold:
                MACD 柱的最小绝对值阈值，用于过滤弱势信号。
                会自动取绝对值（abs），无需保证传入为正。

        返回:
            True 表示发生有效金叉并满足附加条件，否则 False。
        """
        macd = macd_data.get("macd") or []
        signal = macd_data.get("signal") or []
        hist = macd_data.get("hist") or []

        if confirm_bars < 0:
            raise ValueError("confirm_bars 不能为负数")

        # 三者长度可能理论上不一致，这里统一用最短长度
        n = min(len(macd), len(signal), len(hist))
        if n < 2:
            return False

        # 为了支持 confirm_bars，至少要有 cross_idx-1 这一根
        # cross_idx = latest - confirm_bars
        # prev = cross_idx - 1 >= 0  =>  n - 1 - confirm_bars - 1 >= 0 => n >= confirm_bars + 2
        if n < confirm_bars + 2:
            return False

        latest = n - 1
        cross_idx = latest - confirm_bars
        prev = cross_idx - 1
        if prev < 0:
            return False

        threshold = abs(hist_threshold)

        # 1) 在 cross_idx 这一根发生“从下向上突破”的金叉，
        #    即 cross_idx-1 时 macd 在下方 / 相等，cross_idx 时在上方，
        #    且柱子强度达到阈值。
        if not (
            macd[prev] <= signal[prev]
            and macd[cross_idx] > signal[cross_idx]
            and hist[cross_idx] >= threshold
        ):
            return False

        # 2) 从交叉发生那一根（含）到最新这一根，
        #    MACD 一直保持在 signal 之上，且柱子强度不低于阈值。
        for i in range(cross_idx, latest + 1):
            if macd[i] <= signal[i] or hist[i] < threshold:
                return False

        return True

    @staticmethod
    def macd_death_cross(
        macd_data: Dict[str, List[float]],
        *,
        confirm_bars: int = 0,
        hist_threshold: float = 0.0,
    ) -> bool:
        """
        判断 MACD 死叉是否成立。

        参数:
            macd_data: OkxIndicator.calculate_macd() 的返回值。
            confirm_bars:
                交叉后要求持续维持的根数。
                - 0 表示刚发生交叉（当前这根就是交叉那根）。
                - N 表示交叉发生在 N 根之前，并且从交叉那根到当前这根一直维持死叉状态。
            hist_threshold:
                MACD 柱的最小绝对值阈值，用于过滤弱势信号。
                会自动取绝对值（abs），并作为向下柱子的最小强度要求。

        返回:
            True 表示发生有效死叉并满足附加条件，否则 False。
        """
        macd = macd_data.get("macd") or []
        signal = macd_data.get("signal") or []
        hist = macd_data.get("hist") or []

        if confirm_bars < 0:
            raise ValueError("confirm_bars 不能为负数")

        n = min(len(macd), len(signal), len(hist))
        if n < 2:
            return False

        if n < confirm_bars + 2:
            return False

        latest = n - 1
        cross_idx = latest - confirm_bars
        prev = cross_idx - 1
        if prev < 0:
            return False

        threshold = abs(hist_threshold)

        # 1) 在 cross_idx 这一根发生“从上向下跌破”的死叉，
        #    即 cross_idx-1 时 macd 在上方 / 相等，cross_idx 时在下方，
        #    且柱子强度达到阈值（向下）。
        if not (
            macd[prev] >= signal[prev]
            and macd[cross_idx] < signal[cross_idx]
            and hist[cross_idx] <= -threshold
        ):
            return False

        # 2) 从交叉发生那一根（含）到最新这一根，
        #    MACD 一直保持在 signal 之下，且柱子强度不高于 -threshold。
        for i in range(cross_idx, latest + 1):
            if macd[i] >= signal[i] or hist[i] > -threshold:
                return False

        return True
