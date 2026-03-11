from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple
import sqlite3

import ccxt

from lib.db import OkxDatabase
from lib.globalVar import getVar
from lib.logger import okx_logger
from lib.progressbar import progress_bar
from okx_api.okx_account import OkxAccount


class HistoryController:
    """负责处理 history 命令相关逻辑。"""

    def __init__(self, cli_args=None):
        self._cli_args = cli_args
        self._valid_timeframes = set(ccxt.okx().timeframes.keys())
        self._beijing_tz = timezone(timedelta(hours=8))

    def attach_cli_args(self, cli_args) -> None:
        """更新命令行工具引用，便于输出帮助。"""
        self._cli_args = cli_args

    # ========= 对外入口 =========

    def run_download(self, args) -> None:
        """处理历史数据下载命令。"""
        if not hasattr(args, "_prepared_download"):
            if not self.prepare_download_args(args):
                return

        start_ts, end_ts, symbols, timeframes = args._prepared_download

        account = self._resolve_account()
        if account is None:
            okx_logger.error("[命令行][历史任务][账户缺失] 未检测到 OKX 账户配置，终止任务")
            return

        for symbol in symbols:
            try:
                self._process_symbol_history(
                    account=account,
                    symbol=symbol,
                    market_type=args.market_type,
                    timeframes=timeframes,
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
            except Exception as exc:
                okx_logger.exception(f"[命令行][历史任务][执行异常] 币种={symbol} 错误={exc}")
                http_status = getattr(exc, "http_status", None)
                if http_status is not None:
                    okx_logger.error(f"[命令行][历史任务][HTTP状态] 币种={symbol} 状态={http_status}")

                http_body = getattr(exc, "http_body", None)
                if http_body:
                    okx_logger.error(f"[命令行][历史任务][HTTP正文] 币种={symbol} 正文={http_body}")

                response = getattr(exc, "response", None)
                if response:
                    okx_logger.error(f"[命令行][历史任务][HTTP响应] 币种={symbol} 响应={response}")

    def run_check(self, args) -> None:
        """处理历史数据检查命令。"""
        if not hasattr(args, "_prepared_check_interval"):
            if not self.prepare_check_args(args):
                return

        interval = getattr(args, "_prepared_check_interval")

        database = getVar("OKX_DATABASE")
        if not isinstance(database, OkxDatabase):
            raise RuntimeError("全局 OKX 数据库未初始化")
        data_root = database.get_data_root()

        if not data_root.exists():
            okx_logger.info(f"[命令行][历史检查][目录不存在] 路径={data_root}")
            return

        datasets = list(self._iter_datasets(data_root))
        if not datasets:
            okx_logger.info(f"[命令行][历史检查][无数据库文件] 路径={data_root}")
            return

        collected: List[Dict[str, object]] = []

        for symbol_key, market_type, timeframe, db_path in datasets:
            self._inspect_dataset(
                symbol_key=symbol_key,
                market_type=market_type,
                timeframe=timeframe,
                db_path=db_path,
                interval=interval,
                collector=collected.append,
            )

        okx_logger.info(f"[命令行][历史检查][汇总] 覆盖库文件数={len(collected)}")

        for item in collected:
            status = item["status"]
            symbol = item["symbol"]
            timeframe = item["timeframe"]
            if status == "empty":
                okx_logger.info(
                    f"[命令行][历史检查][空库] 币种={symbol} 周期={timeframe} 文件={item['file']}"
                )
                continue

            if status == "no_data":
                okx_logger.info(
                    f"[命令行][历史检查][区间无数据] 币种={symbol} 周期={timeframe} "
                    f"区间={item['interval']}"
                )
                continue

            rows = item["rows"]
            coverage = item["coverage"]
            missing = item["missing"]

            if missing:
                missing_lines = "\n".join(
                    f"  - {self._format_ts(ms)} ~ {self._format_ts(me)}"
                    for ms, me in missing
                )
                okx_logger.error(
                    f"[命令行][历史检查][缺失数据] 币种={symbol} 周期={timeframe} "
                    f"条数={rows} 覆盖={coverage[0]}~{coverage[1]} 缺失区间如下:\n{missing_lines}"
                )
            else:
                okx_logger.info(
                    f"[命令行][历史检查][连续完备] 币种={symbol} 周期={timeframe} "
                    f"条数={rows} 覆盖={coverage[0]}~{coverage[1]}"
                )

    def prepare_download_args(self, args) -> bool:
        """预检查历史下载命令参数，确保满足运行条件。"""
        try:
            start_ts, end_ts = self._parse_interval(args.interval)
        except ValueError as exc:
            okx_logger.error(f"[命令行][历史任务][时间格式错误] 参数={args.interval} 错误={exc}")
            return False

        symbols = self._parse_symbols(args.symbols)
        if not symbols:
            okx_logger.error("[命令行][历史任务][币种无效] 无有效 symbol，请检查 --symbol")
            return False

        timeframes = self._parse_timeframes(args.timeframes)
        if not timeframes:
            okx_logger.error("[命令行][历史任务][周期无效] 无有效 timeframe，请检查 --frame")
            return False

        args._prepared_download = (start_ts, end_ts, symbols, timeframes)
        return True

    def prepare_check_args(self, args) -> bool:
        """预检查历史检查命令参数。"""
        interval = None
        if getattr(args, "time_interval", None):
            try:
                interval = self._parse_interval(args.time_interval)
            except ValueError as exc:
                okx_logger.error(f"[命令行][历史检查][时间格式错误] 参数={args.time_interval} 错误={exc}")
                return False

        args._prepared_check_interval = interval
        return True

    # ========= 内部工具 =========

    def _resolve_account(self) -> Optional[OkxAccount]:
        account = getVar("OKX_ACCOUNT")
        if isinstance(account, OkxAccount):
            return account
        return None

    def _parse_interval(self, interval: str) -> Tuple[int, int]:
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

    def _format_ts(self, ts: int) -> str:
        """将毫秒时间戳格式化为可读字符串（北京时间）。"""
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(self._beijing_tz)
        return dt.strftime("%Y-%m-%d %H:%M:%S CST")

    def _parse_symbols(self, raw: str):
        if raw is None:
            return []
        return [
            segment.strip()
            for segment in raw.replace(" ", ",").split(",")
            if segment.strip()
        ]

    def _parse_timeframes(self, raw: str):
        """解析 frame 参数，支持逗号分隔或空格分隔。"""
        if raw is None:
            return []

        candidates = []
        for segment in raw.replace(" ", ",").split(","):
            seg = segment.strip()
            if seg:
                candidates.append(seg)
        valid = []
        invalid = []
        for tf in candidates:
            if tf in self._valid_timeframes:
                valid.append(tf)
            else:
                invalid.append(tf)

        if invalid:
            okx_logger.error(f"[命令行][历史任务][周期不支持] 列表={','.join(invalid)}")

        return valid

    def _iter_datasets(self, data_root: Path) -> Iterable[Tuple[str, str, str, Path]]:
        """遍历数据目录，生成 (symbol, market_type, timeframe, db_path)。"""
        for folder in data_root.iterdir():
            if not folder.is_dir():
                continue
            if "-" not in folder.name:
                continue
            symbol_part, market_type = folder.name.rsplit("-", 1)
            for db_file in folder.glob("*.db"):
                timeframe = db_file.stem
                yield symbol_part.upper(), market_type, timeframe, db_file

    def _inspect_dataset(
        self,
        symbol_key: str,
        market_type: str,
        timeframe: str,
        db_path: Path,
        interval: Optional[Tuple[int, int]],
        collector: Callable[[Dict[str, object]], None],
    ) -> None:
        """检查单个数据库文件的数据连续性与缺口。"""
        min_max = self._fetch_min_max(db_path)

        if min_max is None:
            collector(
                {
                    "status": "empty",
                    "symbol": f"{symbol_key}-{market_type}",
                    "timeframe": timeframe,
                    "file": db_path.name,
                }
            )
            return

        min_ts, max_ts = min_max
        start_ts, end_ts = min_ts, max_ts
        if interval:
            start_ts = max(start_ts, interval[0])
            end_ts = min(end_ts, interval[1])
            if start_ts > end_ts:
                collector(
                    {
                        "status": "no_data",
                        "symbol": f"{symbol_key}-{market_type}",
                        "timeframe": timeframe,
                        "interval": f"{self._format_ts(interval[0])}~{self._format_ts(interval[1])}",
                    }
                )
                return

        timestamps = self._fetch_timestamps(db_path, start_ts, end_ts)

        if not timestamps:
            collector(
                {
                    "status": "no_data",
                    "symbol": f"{symbol_key}-{market_type}",
                    "timeframe": timeframe,
                    "interval": f"{self._format_ts(start_ts)}~{self._format_ts(end_ts)}",
                }
            )
            return

        step = self._timeframe_to_milliseconds(timeframe)

        missing_ranges = self._compute_missing_ranges(
            timestamps=timestamps,
            step=step,
            start_ts=start_ts,
            end_ts=end_ts,
        )

        coverage_start = self._format_ts(timestamps[0])
        coverage_end = self._format_ts(timestamps[-1])

        collector(
            {
                "status": "ok",
                "symbol": f"{symbol_key}-{market_type}",
                "timeframe": timeframe,
                "rows": len(timestamps),
                "coverage": (coverage_start, coverage_end),
                "missing": missing_ranges,
            }
        )

    def _fetch_min_max(self, db_path: Path) -> Optional[Tuple[int, int]]:
        """查询数据库中的最小与最大时间戳（毫秒）。"""
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM klines")
            except sqlite3.OperationalError as exc:
                okx_logger.info(
                    f"[命令行][历史检查][旧库结构] 文件={db_path} 错误={exc} 将视为无数据"
                )
                return None
            row = cursor.fetchone()
            if not row or row[0] is None or row[1] is None:
                return None
            min_ts = self._convert_db_timestamp_to_millis(row[0])
            max_ts = self._convert_db_timestamp_to_millis(row[1])
            return min_ts, max_ts
        finally:
            conn.close()

    def _fetch_timestamps(
        self,
        db_path: Path,
        start_ts: int,
        end_ts: int,
    ) -> List[int]:
        """读取指定区间内的所有时间戳（毫秒）。"""
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            start_dt = datetime.fromtimestamp(
                start_ts / 1000, tz=timezone.utc
            ).astimezone(self._beijing_tz).replace(tzinfo=None)
            end_dt = datetime.fromtimestamp(
                end_ts / 1000, tz=timezone.utc
            ).astimezone(self._beijing_tz).replace(tzinfo=None)
            try:
                cursor.execute(
                    """
                    SELECT timestamp
                    FROM klines
                    WHERE timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp ASC
                    """,
                    (start_dt, end_dt),
                )
            except sqlite3.OperationalError as exc:
                okx_logger.info(
                    f"[命令行][历史检查][旧库结构] 文件={db_path} 错误={exc} 将视为无数据"
                )
                return []
            rows = cursor.fetchall()
            return [
                self._convert_db_timestamp_to_millis(row[0])
                for row in rows
                if row[0] is not None
            ]
        finally:
            conn.close()

    def _compute_missing_ranges(
        self,
        timestamps: List[int],
        step: int,
        start_ts: int,
        end_ts: int,
    ) -> List[Tuple[int, int]]:
        """计算缺失区间。"""
        if not timestamps:
            return [(start_ts, end_ts)] if start_ts <= end_ts else []

        missing: List[Tuple[int, int]] = []
        expected = start_ts

        for ts in timestamps:
            if ts < start_ts:
                continue
            if ts > end_ts:
                break
            if ts > expected:
                missing.append((expected, ts - step))
            expected = ts + step

        if expected <= end_ts:
            missing.append((expected, end_ts))

        return [
            (ms, me)
            for ms, me in missing
            if ms <= me
        ]

    def _timeframe_to_milliseconds(self, timeframe: str) -> int:
        """将周期字符串转换为毫秒。"""
        normalized = timeframe.strip()
        if not normalized:
            raise ValueError("timeframe 不能为空")

        special = normalized.lower()
        if special.endswith("mth"):
            unit = "mth"
            value = int(normalized[:-3])
        else:
            unit = normalized[-1].lower()
            value = int(normalized[:-1])

        mapping = {
            "s": 1000,
            "m": 60 * 1000,
            "h": 60 * 60 * 1000,
            "d": 24 * 60 * 60 * 1000,
            "w": 7 * 24 * 60 * 60 * 1000,
            "mth": 30 * 24 * 60 * 60 * 1000,
        }

        if unit not in mapping:
            raise ValueError(f"不支持的 timeframe: {timeframe}")

        return value * mapping[unit]

    def _convert_db_timestamp_to_millis(self, raw_ts) -> int:
        """将数据库中的时间字段统一转换为北京时间毫秒时间戳。"""
        if isinstance(raw_ts, str):
            parsed = None
            for fmt in (None, "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                try:
                    if fmt is None:
                        parsed = datetime.fromisoformat(raw_ts)
                    else:
                        parsed = datetime.strptime(raw_ts, fmt)
                    break
                except ValueError:
                    continue
            if parsed is None:
                raise ValueError(f"无法解析时间戳: {raw_ts}")
        elif isinstance(raw_ts, datetime):
            parsed = raw_ts
        else:
            return int(float(raw_ts) * 1000)

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=self._beijing_tz)
        else:
            parsed = parsed.astimezone(self._beijing_tz)

        return int(parsed.timestamp() * 1000)

    def _process_symbol_history(
        self,
        account: OkxAccount,
        symbol: str,
        market_type: str,
        timeframes: List[str],
        start_ts: int,
        end_ts: int,
    ) -> None:
        price = account.get_OkxCoin(
            symbol=symbol,
            market_type=market_type,
        )
        history = price.get_history()

        tasks = []  # (timeframe, ms, me)
        summary: Dict[str, Dict[str, int]] = {
            tf: {"segments": 0, "inserted": 0}
            for tf in timeframes
        }

        now_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        effective_end_ts = end_ts

        for timeframe in timeframes:
            frame_ms = history._timeframe_to_milliseconds(timeframe)  # noqa: SLF001
            safe_now = now_timestamp - frame_ms
            aligned_now = ((safe_now // frame_ms) * frame_ms) + (frame_ms - 1)
            safe_end_ts = min(end_ts, aligned_now)
            effective_end_ts = min(effective_end_ts, safe_end_ts)

            if start_ts > safe_end_ts:
                okx_logger.info(
                    f"[命令行][历史任务][跳过] 币种={symbol}-{market_type} 周期={timeframe} "
                    f"原因=区间全部位于最新未收盘的 K 线之后"
                )
                continue

            raw_missing = history.find_missing_ranges(
                timeframe=timeframe,
                start_timestamp=start_ts,
                end_timestamp=safe_end_ts,
            )

            valid_ranges = [(ms, me) for ms, me in raw_missing if ms <= me]
            if not valid_ranges:
                continue

            summary[timeframe]["segments"] = len(valid_ranges)
            okx_logger.info(
                f"[命令行][历史任务][缺口识别] 币种={symbol}-{market_type} 周期={timeframe} "
                f"缺口数量={len(valid_ranges)} "
                f"示例={', '.join(f'{self._format_ts(ms)}~{self._format_ts(me)}' for ms, me in valid_ranges[:3])}"
            )

            for ms, me in valid_ranges:
                adjusted_end = min(me, safe_end_ts)
                if adjusted_end < ms:
                    continue
                tasks.append((timeframe, ms, adjusted_end))

        if tasks:
            with progress_bar(total=len(tasks), desc=f"{symbol.upper()} 历史数据下载") as bar:
                for timeframe, ms, me in tasks:
                    inserted = history.sync_range_to_db(
                        timeframe=timeframe,
                        start_timestamp=ms,
                        end_timestamp=me,
                    )
                    summary[timeframe]["inserted"] += inserted
                    bar.update()

        detail_parts = []
        total_inserted = 0
        updated_count = 0

        for timeframe in timeframes:
            info = summary[timeframe]
            inserted = info["inserted"]
            segments = info["segments"]
            if inserted > 0:
                updated_count += 1
                total_inserted += inserted
                detail_parts.append(f"{timeframe}+{inserted}")
            elif segments > 0:
                detail_parts.append(f"{timeframe}+0")
            else:
                detail_parts.append(f"{timeframe}-")

        detail_msg = ", ".join(detail_parts)
        okx_logger.info(
            "[命令行][历史任务][汇总] "
            f"币种={price.symbol} 市场={market_type} "
            f"区间={self._format_ts(start_ts)}~{self._format_ts(effective_end_ts)} "
            f"周期统计={detail_msg} 更新周期数={updated_count} 新增条数={total_inserted}"
        )
