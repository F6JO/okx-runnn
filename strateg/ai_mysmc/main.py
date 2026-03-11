"""策略模板，用于开发者编写新策略。

本文件适合复制后改名使用。
请保持目录名与策略名一致，入口文件为 main.py，类名固定为 strateg。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Dict, List

from ai.openai_callbacks import OpenAiCallbacks
from ai.openai_chat_tool import OpenAiChatTool
from lib.globalVar import getVar
from lib.logger import okx_logger
from okx_api.tools.okx_indicator import OkxIndicator
from okx_api.tools.okx_signals import OkxSignals

from lib.logger import format_beijing_ts

import re

RUN_ONLY_TIMEFRAME = "15m"

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
        import datetime

        # 获取当前日期和时间
        now = datetime.datetime.now()

        # 格式化为 年-月-日 时:分:秒
        formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
        """策略执行入口。"""
        if self._timeframe != RUN_ONLY_TIMEFRAME:
            okx_logger.debug(
                "[策略][ai_mysmc][跳过] 仅%s执行 symbol=%s market=%s 当前级别=%s",
                RUN_ONLY_TIMEFRAME,
                self._symbol,
                self._market_type,
                self._timeframe,
            )
            return False

        aiChatTool = OpenAiChatTool(getVar("AI_CLIENTS")["deepseek"])
        

        candles_a_matrix = self._candles_to_matrix_without_confirm(self._format_candle_timestamps(self._get_recent_candles_with_latest(limit=180, timeframe="1h")))

        candles_b_matrix = self._candles_to_matrix_without_confirm(self._format_candle_timestamps(self._get_recent_candles_with_latest(limit=300, timeframe="15m")))

        f = open("strateg/ai_mysmc/aismc.md","r", encoding='utf-8')
        nr = f.read()
        f.close()
        jieguo = aiChatTool.think(f"{nr}\n\n如下是1h的数据：{candles_a_matrix}\n\n\n\n如下是15m的数据：{candles_b_matrix}")
        jieguo = re.sub(
            r"<think>.*?</think>",
            "",
            jieguo,
            flags=re.DOTALL,
        ).strip()
        json_match = re.match(r"^\s*(\{[\s\S]*\})\s*$", jieguo)
        json_text = json_match.group(1) if json_match else ""
        # json_text = '{"ret":"做多","profit":"2011","loss":"28181","rat":"发现15分钟实体突破并伴有FVG"}'

        if not json_text:
            okx_logger.warning("[策略][ai_mysmc][AI结果] 未匹配到完整JSON")
            return False
        else:
            okx_logger.debug(
                f"[策略][ai_mysmc][AIJson] json={json_text}"
            )

        try:
            result_obj = json.loads(json_text)
        except Exception as exc:  # noqa: BLE001
            okx_logger.warning("[策略][ai_mysmc][AI结果] JSON解析失败 错误=%s", exc)
            return False

        if not isinstance(result_obj, dict):
            okx_logger.warning("[策略][ai_mysmc][AI结果] JSON根节点不是对象")
            return False

        ret_value = str(result_obj.get("ret", "")).strip()
        rat_value = str(result_obj.get("rat", "")).strip()
        price_value = str(result_obj.get("price", "")).strip()
        profit_value = str(result_obj.get("profit", "")).strip()
        loss_value = str(result_obj.get("loss", "")).strip()
        okx_logger.info(
                "[策略][ai_mysmc][AI结果] ret=%s，%s",
                ret_value or "空",
                rat_value or "无",
            )
        if ret_value != "做多" and ret_value != "做空":
            return False
        # {"ret":"做空","price":"71880","profit":"71520","loss":"72080","rat":"于1.6。"}
        
        

        ding_content = (
            "## 条件满足，可以入场\n"
            f"- 时间：**{formatted_time}**\n"
            f"- 币种：**{self._symbol}**\n"
            f"- 市场：**{self._market_type}**\n"
            f"- 级别：**{self._timeframe}**\n"
            f"- 方向：**{ret_value}**\n"
            f"- 入场价：**{price_value}**\n"
            f"- 止盈价：**{profit_value or '-'}**\n"
            f"- 止损价：**{loss_value or '-'}**\n"
            f"- 入场原因：{rat_value or '-'}\n"
        )

        talk = getVar("TALK_DING")
        if talk is None:
            okx_logger.warning("[策略][ai_mysmc][通知跳过] TALK_DING 未配置")
            return True

        try:
            talk.send(
                ding_content,
                title=f"{self._symbol}-{self._timeframe} 入场通知",
            )
            okx_logger.info("[策略][ai_mysmc][通知发送成功] symbol=%s frame=%s", self._symbol, self._timeframe)
        except Exception as exc:  # noqa: BLE001
            okx_logger.warning("[策略][ai_mysmc][通知发送失败] 错误=%s", exc)

        return False

    def _build_slice(self, limit: int) -> CandleSlice:
        # 读取 limit+1 根，便于引用“上一根”K线。
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
        # KlineCache 在 monitor run 时预热，但新交易对可能仍为空。
        # 需要时会从本地 SQLite 历史库读取补齐。
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
        return [c for c in candles if isinstance(c, dict)]

    def _get_recent_candles_with_latest(
        self,
        limit: int,
        timeframe: str | None = None,
    ) -> List[Dict[str, float]]:
        candles = self._get_recent_candles(limit=limit, timeframe=timeframe)
        target_timeframe = (timeframe or self._timeframe).strip() or self._timeframe
        if target_timeframe == self._timeframe:
            latest = dict(self._latest) if isinstance(self._latest, dict) else {}
        else:
            live_candles = getVar("LIVE_CANDLES") or {}
            live_key = (self._symbol, self._market_type, target_timeframe)
            live_latest = live_candles.get(live_key) if isinstance(live_candles, dict) else None
            latest = dict(live_latest) if isinstance(live_latest, dict) else {}

        latest_ts = latest.get("timestamp")
        if latest_ts is None:
            return candles

        merged = [dict(candle) for candle in candles if isinstance(candle, dict)]
        if not merged:
            return [latest]

        last_ts = merged[-1].get("timestamp")
        if last_ts == latest_ts:
            merged[-1] = latest
        elif last_ts is None or latest_ts > last_ts:
            merged.append(latest)
        return merged

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
        示例：只读取最近 50 根，用于更轻量的计算。
        """
        return self._get_recent_candles(limit=50)

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
            "confirm",
        ]
        matrix: List[List[Any]] = [list(ordered_fields)]

        for candle in candles:
            if not isinstance(candle, dict):
                continue
            item = dict(candle)
            # item.pop("confirm", None)
            row = [item.get(field) for field in ordered_fields]
            matrix.append(row)
        return matrix
