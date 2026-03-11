from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

from lib.logger import okx_logger, format_beijing_ts
from lib.globalVar import getVar
from lib.talkding import TalkDing


class StrategResuController:
    """
    负责接收每轮策略执行的结果，方便扩展通知/交易等后续动作。
    当前实现仅输出一条友好的日志，便于观察策略状态。
    """

    def handle_results(
        self,
        *,
        symbol: str,
        market_type: str,
        timeframe: str,
        results: Dict[str, bool],
    ) -> None:
        if not results:
            okx_logger.debug(
                "[策略结果控制器][空结果] symbol=%s market=%s frame=%s",
                symbol,
                market_type,
                timeframe,
            )
            return

        success = [name for name, passed in results.items() if passed]
        failed = [name for name, passed in results.items() if not passed]

        okx_logger.info(
            "[策略结果控制器] symbol=%s market=%s frame=%s success=%s failed=%s",
            symbol,
            market_type,
            timeframe,
            success or "-",
            failed or "-",
        )

        if failed:
            return

        talk: TalkDing | None = getVar("TALK_DING")
        if talk is None:
            okx_logger.debug(
                "[策略结果控制器][通知跳过] 未配置钉钉 token symbol=%s market=%s frame=%s",
                symbol,
                market_type,
                timeframe,
            )
            return

        title = f"{symbol}-{timeframe} 策略通过"
        strategy_text = "、".join(success) if success else "无"
        trigger_time = format_beijing_ts(
            int(datetime.now(timezone.utc).timestamp() * 1000)
        )
        strategy_lines = "\n".join(f"> ✅ {name}" for name in success) or "> (无策略详情)"
        message = (
            f"## ✅ 策略全部通过\n"
            f"- 币种：**{symbol}**\n"
            f"- 市场：**{market_type}**\n"
            f"- 级别：**{timeframe}**\n"
            f"- 策略：**{strategy_text}**\n"
            f"- 策略数量：**{len(success)}**\n"
            f"- 触发时间：**{trigger_time}**\n"
            f"- 状态：全部检测通过，可继续关注行情。\n"
            "\n"
            "#### 策略详情\n"
            f"{strategy_lines}\n"
        )
        try:
            talk.send(message, title=title)
            okx_logger.info(
                "[策略结果控制器][通知发送成功] symbol=%s market=%s frame=%s",
                symbol,
                market_type,
                timeframe,
            )
        except Exception as exc:  # noqa: BLE001
            okx_logger.warning(
                "[策略结果控制器][通知发送失败] symbol=%s market=%s frame=%s 错误=%s",
                symbol,
                market_type,
                timeframe,
                exc,
            )
