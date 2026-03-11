from __future__ import annotations

from datetime import datetime, timedelta, timezone

from lib.logger import okx_logger


class BacktestController:
    """占位回测控制器，后续可扩展具体逻辑。"""

    def __init__(self, cli_args=None):
        self._cli_args = cli_args
        self._beijing_tz = timezone(timedelta(hours=8))

    def attach_cli_args(self, cli_args) -> None:
        self._cli_args = cli_args

    def run(self, args) -> None:
        """解析回测参数（-P + -i）。"""
        raw_pairs = getattr(args, "pairs", None) or []
        parsed_pairs = []
        for raw in raw_pairs:
            try:
                parsed_pairs.append(self._parse_pair(raw))
            except ValueError as exc:
                okx_logger.error(f"[命令行][回测][参数错误] pair={raw} 错误={exc}")
                return

        interval = getattr(args, "interval", None)
        time_range = None
        if interval:
            try:
                time_range = self._parse_interval(interval)
            except ValueError as exc:
                okx_logger.error(f"[命令行][回测][时间格式错误] 参数={interval} 错误={exc}")
                return

        okx_logger.info(
            "[命令行][回测][参数解析完成] pairs=%s interval=%s",
            parsed_pairs,
            time_range,
        )
        self._ensure_history(parsed_pairs, time_range)

    def _parse_pair(self, raw: str) -> dict:
        """
        解析 -P 参数，格式:
        symbol=btc,type=swap,frames=5m|15m,strategies=macd
        """
        if not raw or "=" not in raw:
            raise ValueError("格式应为 key=value,key=value")

        items = {}
        for segment in raw.split(","):
            segment = segment.strip()
            if not segment:
                continue
            if "=" not in segment:
                raise ValueError(f"缺少 '=' 分隔符: {segment}")
            key, value = segment.split("=", 1)
            key = key.strip().lower()
            value = value.strip()
            if not key or not value:
                raise ValueError(f"键和值均不能为空: {segment}")
            items[key] = value

        required = {"symbol", "type", "frames", "strategies"}
        missing = required - set(items.keys())
        if missing:
            raise ValueError(f"缺少必要字段: {','.join(sorted(missing))}")

        def split_multi(raw_value: str):
            return [part.strip() for part in raw_value.split("|") if part.strip()]

        parsed = {
            "symbol": split_multi(items["symbol"]),
            "type": split_multi(items["type"]),
            "frames": split_multi(items["frames"]),
            "strategies": split_multi(items["strategies"]),
        }
        okx_logger.debug(
            "[命令行][回测][参数解析] 原始=%s 解析结果=%s",
            raw,
            parsed,
        )
        return parsed

    def _parse_interval(self, interval: str) -> tuple[int, int]:
        """
        解析区间字符串，返回开始与结束的毫秒时间戳。
        支持格式: YYYYMMDD-YYYYMMDD 或 YYYYMMDD-
        """
        if not interval or "-" not in interval:
            raise ValueError("格式应为 YYYYMMDD-YYYYMMDD 或 YYYYMMDD-")

        start_str, end_str = interval.split("-", 1)
        if len(start_str) != 8 or not start_str.isdigit():
            raise ValueError("开始日期必须为 8 位数字（例如 20240101）")

        start_dt = datetime.strptime(start_str, "%Y%m%d").replace(tzinfo=self._beijing_tz)
        start_ts = int(start_dt.timestamp() * 1000)

        if end_str:
            if len(end_str) != 8 or not end_str.isdigit():
                raise ValueError("结束日期必须为 8 位数字（例如 20251010）")
            end_date = datetime.strptime(end_str, "%Y%m%d").replace(tzinfo=self._beijing_tz)
            end_dt = end_date + timedelta(days=1) - timedelta(milliseconds=1)
            end_ts = int(end_dt.timestamp() * 1000)
        else:
            end_ts = int(datetime.now(self._beijing_tz).timestamp() * 1000)

        if start_ts > end_ts:
            raise ValueError("开始时间不能晚于结束时间")

        return start_ts, end_ts

    def _ensure_history(
        self,
        parsed_pairs: list[dict],
        time_range: tuple[int, int] | None,
    ) -> None:
        """
        确保数据库覆盖回测区间。
        - 若指定了 -i 区间，必须覆盖该区间。
        - 若未指定 -i，则默认使用数据库最早时间到现在。
        """
        from lib.globalVar import getVar
        from lib.logger import format_beijing_ts

        account = getVar("OKX_ACCOUNT")
        if account is None:
            okx_logger.error(
                "[命令行][回测][账户缺失] 无法拉取历史数据，请配置 OKX API"
            )
            return

        for config in parsed_pairs:
            symbols = config.get("symbol", [])
            market_types = config.get("type", [])
            frames = config.get("frames", [])

            for symbol in symbols:
                for market_type in market_types:
                    try:
                        coin = account.get_OkxCoin(
                            symbol=symbol,
                            market_type=market_type,
                        )
                        history = coin.get_history()
                    except Exception as exc:  # noqa: BLE001
                        okx_logger.error(
                            "[命令行][回测][历史实例失败] symbol=%s market=%s 错误=%s",
                            symbol,
                            market_type,
                            exc,
                        )
                        continue

                    for frame in frames:
                        start_ts, end_ts = self._resolve_time_range(
                            history=history,
                            timeframe=frame,
                            time_range=time_range,
                        )
                        if start_ts is None or end_ts is None:
                            okx_logger.warning(
                                "[命令行][回测][区间缺失] symbol=%s market=%s frame=%s",
                                coin.symbol,
                                market_type,
                                frame,
                            )
                            continue

                        okx_logger.info(
                            "[命令行][回测][区间准备] symbol=%s market=%s frame=%s "
                            "start=%s end=%s",
                            coin.symbol,
                            market_type,
                            frame,
                            format_beijing_ts(start_ts),
                            format_beijing_ts(end_ts),
                        )

                        inserted = history.sync_missing_data(
                            timeframe=frame,
                            start_timestamp=start_ts,
                            end_timestamp=end_ts,
                        )
                        okx_logger.info(
                            "[命令行][回测][历史补齐] symbol=%s market=%s frame=%s inserted=%s",
                            coin.symbol,
                            market_type,
                            frame,
                            inserted,
                        )

                        missing = history.find_missing_ranges(
                            timeframe=frame,
                            start_timestamp=start_ts,
                            end_timestamp=end_ts,
                        )
                        if missing:
                            okx_logger.error(
                                "[命令行][回测][覆盖不足] symbol=%s market=%s frame=%s "
                                "missing=%s",
                                coin.symbol,
                                market_type,
                                frame,
                                [
                                    f"{format_beijing_ts(s)}~{format_beijing_ts(e)}"
                                    for s, e in missing
                                ],
                            )
                        else:
                            okx_logger.info(
                                "[命令行][回测][覆盖完整] symbol=%s market=%s frame=%s",
                                coin.symbol,
                                market_type,
                                frame,
                            )

    def _resolve_time_range(
        self,
        *,
        history,
        timeframe: str,
        time_range: tuple[int, int] | None,
    ) -> tuple[int | None, int | None]:
        """
        若指定 -i，使用该区间；
        否则使用数据库最早时间到现在。
        """
        if time_range:
            start_ts, end_ts = time_range
            return start_ts, self._align_end_to_closed_candle(
                history=history,
                timeframe=timeframe,
                end_ts=end_ts,
            )

        existing = history.get_existing_range(timeframe)
        if existing is None:
            return self._build_default_range(timeframe, history)

        start_ts, _ = existing
        now_ts = int(datetime.now(self._beijing_tz).timestamp() * 1000)
        return start_ts, self._align_end_to_closed_candle(
            history=history,
            timeframe=timeframe,
            end_ts=now_ts,
        )

    def _build_default_range(
        self,
        timeframe: str,
        history,
    ) -> tuple[int | None, int | None]:
        """
        数据库为空时，使用 monitor.default_history_days 作为默认回测区间。
        """
        from lib.globalVar import getVar

        default_days = getVar("MONITOR_DEFAULT_HISTORY_DAYS", {}) or {}
        if not isinstance(default_days, dict):
            okx_logger.error(
                "[命令行][回测][配置错误] MONITOR_DEFAULT_HISTORY_DAYS 需为字典"
            )
            return None, None

        days = default_days.get(timeframe)
        if days is None:
            okx_logger.error(
                "[命令行][回测][默认区间缺失] timeframe=%s 未配置 default_history_days",
                timeframe,
            )
            return None, None

        try:
            days = int(days)
        except (TypeError, ValueError):
            okx_logger.error(
                "[命令行][回测][默认区间非法] timeframe=%s days=%s",
                timeframe,
                days,
            )
            return None, None

        end_ts = int(datetime.now(self._beijing_tz).timestamp() * 1000)
        start_ts = end_ts - days * 24 * 60 * 60 * 1000
        return start_ts, self._align_end_to_closed_candle(
            history=history,
            timeframe=timeframe,
            end_ts=end_ts,
        )

    def _align_end_to_closed_candle(
        self,
        *,
        history,
        timeframe: str,
        end_ts: int,
    ) -> int:
        """
        将结束时间对齐到最近一根已收盘K线，避免当前未收盘K线导致缺口。
        """
        try:
            frame_ms = history._timeframe_to_milliseconds(timeframe)  # noqa: SLF001
        except Exception:
            return end_ts
        safe_now = int(datetime.now(self._beijing_tz).timestamp() * 1000) - frame_ms
        aligned_end = ((safe_now // frame_ms) * frame_ms) + (frame_ms - 1)
        return min(end_ts, aligned_end)
