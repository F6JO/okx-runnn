from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from lib.globalVar import getVar
from lib.db import OkxDatabase
from lib.logger import okx_logger, format_beijing_ts

try:
    from ccxt.base.errors import ExchangeNotAvailable, NetworkError, RequestTimeout
except Exception:  # pragma: no cover - ccxt 未安装时兜底
    ExchangeNotAvailable = NetworkError = RequestTimeout = None

if TYPE_CHECKING:
    from okx_api.okx_account import OkxAccount


class OkxHistoryData:
    """
    历史数据管理类

    负责：
    - 批量抓取 OKX 历史 K 线数据
    - 写入 / 读取本地数据库
    - 自动检测缺口并补齐
    """

    def __init__(
        self,
        symbol: str,
        market_type: str,
        batch_limit: int = 300,
    ):
        self.okx_account = self._resolve_account()
        self.exchange = self.okx_account.exchange
        self.symbol = symbol
        self.market_type = market_type
        self.batch_limit = batch_limit
        self._storage_symbol = OkxDatabase.sanitize_symbol(symbol)
        database = getVar("OKX_DATABASE")
        if not isinstance(database, OkxDatabase):
            raise RuntimeError("全局 OKX 数据库未初始化")
        self.database = database
        retries_cfg = getVar("API_MAX_RETRIES", 3)
        try:
            self._max_retries = max(int(retries_cfg), 1)
        except (TypeError, ValueError):
            self._max_retries = 3

    def _is_transient_error(self, exc: Exception) -> bool:
        """判断异常是否可能是临时连接问题，用于调整日志级别。"""
        transient_classes = tuple(
            cls
            for cls in (NetworkError, ExchangeNotAvailable, RequestTimeout)
            if isinstance(cls, type)
        )
        if transient_classes and isinstance(exc, transient_classes):
            return True

        message = str(exc).lower()
        transient_keywords = (
            "disconnected",
            "disconnect",
            "connection reset",
            "connection aborted",
            "connection closed",
            "connection refused",
            "timed out",
            "timeout",
            "网络异常",
            "断开",
        )
        return any(keyword in message for keyword in transient_keywords)

    def _resolve_account(self) -> "OkxAccount":
        account = getVar("OKX_ACCOUNT")
        if account is None:
            raise ValueError("全局 OKX 账户未设置，无法构建 OkxHistoryData")
        return account

    # ========== 公共方法 ==========

    def fetch_range(
        self,
        timeframe: str,
        start_timestamp: int,
        end_timestamp: int,
    ) -> List[Dict[str, Any]]:
        """
        从交易所抓取指定时间段的 K 线数据（毫秒时间戳）
        """
        if start_timestamp > end_timestamp:
            raise ValueError("start_timestamp 必须早于 end_timestamp")

        timeframe_ms = self._timeframe_to_milliseconds(timeframe)
        start_str = format_beijing_ts(start_timestamp)
        end_str = format_beijing_ts(end_timestamp)

        okx_logger.info(
            f"[历史数据][抓取][开始] 币种={self.symbol} 周期={timeframe} "
            f"区间={start_str}~{end_str}"
        )

        cursor = start_timestamp
        max_limit = min(self.batch_limit, 300)
        results: List[Dict[str, Any]] = []

        while cursor <= end_timestamp:
            cursor_str = format_beijing_ts(cursor)
            okx_logger.debug(
                "[历史数据][抓取][请求] symbol=%s timeframe=%s since=%s limit=%s",
                self.symbol,
                timeframe,
                f"{cursor_str}({cursor})",
                max_limit,
            )
            batch: List[List[Any]] = []
            for attempt in range(1, self._max_retries + 1):
                try:
                    batch = self.exchange.fetch_ohlcv(
                        symbol=self.symbol,
                        timeframe=timeframe,
                        since=cursor,
                        limit=max_limit,
                    )
                    break
                except Exception as exc:  # noqa: BLE001
                    is_transient = self._is_transient_error(exc)
                    is_last_attempt = attempt >= self._max_retries
                    log_fn = okx_logger.error if is_last_attempt else okx_logger.warning
                    context_hint = " (可能的临时连接问题)" if is_transient else ""
                    log_fn(
                        "[历史数据][抓取][失败]%s 币种=%s 周期=%s since=%s 尝试=%s/%s 错误=%s",
                        context_hint,
                        self.symbol,
                        timeframe,
                        f"{cursor_str}({cursor})",
                        attempt,
                        self._max_retries,
                        exc,
                    )
                    if attempt < self._max_retries:
                        time.sleep(min(1 * attempt, 5))
                    else:
                        okx_logger.error(
                            "[历史数据][抓取][终止] 币种=%s 周期=%s since=%s 已达到最大重试次数",
                            self.symbol,
                            timeframe,
                            f"{cursor_str}({cursor})",
                        )
                        return results

            if not batch:
                okx_logger.info(
                    f"[历史数据][抓取][空批次] 币种={self.symbol} 周期={timeframe} 游标={format_beijing_ts(cursor)}"
                )
                break

            last_timestamp = cursor - timeframe_ms
            for candle in batch:
                ts = candle[0]
                if ts < start_timestamp:
                    continue
                if ts > end_timestamp:
                    break

                results.append(
                    {
                        "timestamp": ts,
                        "open": float(candle[1]),
                        "high": float(candle[2]),
                        "low": float(candle[3]),
                        "close": float(candle[4]),
                        "volume": float(candle[5]),
                    }
                )
                last_timestamp = ts
            else:
                last_timestamp = batch[-1][0]

            next_cursor = last_timestamp + timeframe_ms
            if next_cursor <= cursor:
                next_cursor = cursor + timeframe_ms
            cursor = next_cursor

            if len(batch) < max_limit:
                okx_logger.debug(
                    f"[历史数据][抓取][批次不足] 币种={self.symbol} 周期={timeframe} "
                    f"批次条数={len(batch)} 限制={max_limit}"
                )
                if last_timestamp >= end_timestamp:
                    okx_logger.debug(
                        f"[历史数据][抓取][到达区间末尾] 币种={self.symbol} 周期={timeframe}"
                    )
                    break

        okx_logger.info(
            f"[历史数据][抓取][完成] 币种={self.symbol} 周期={timeframe} 条数={len(results)}"
        )
        return results

    def sync_range_to_db(
        self,
        timeframe: str,
        start_timestamp: int,
        end_timestamp: int,
    ) -> int:
        """
        抓取指定区间数据并写入数据库，返回写入数量
        """
        candles = self.fetch_range(timeframe, start_timestamp, end_timestamp)
        if not candles:
            okx_logger.info(
                f"[历史数据][同步][无新增数据] 币种={self.symbol} 周期={timeframe}"
            )
            return 0

        for candle in candles:
            base_volume = float(candle.get("volume", 0) or 0)
            close_price = float(candle.get("close", 0) or 0)
            candle["volume_contract"] = base_volume
            candle["volume_currency"] = base_volume
            candle["volume_quote"] = round(base_volume * close_price, 8)
            candle["confirm"] = 1

        okx_logger.debug(
            "[历史数据][同步][准备写库] "
            f"symbol={self.symbol} timeframe={timeframe} rows={len(candles)}"
        )
        success = self.database.insert_klines(
            symbol=self._storage_symbol,
            market_type=self.market_type,
            timeframe=timeframe,
            data=json.dumps(candles),
        )
        if not success:
            okx_logger.error(
                f"[历史数据][同步][写库失败] 币种={self.symbol} 周期={timeframe}"
            )
            return 0

        return len(candles)

    def load_from_db(
        self,
        timeframe: str,
        start_timestamp: int,
        end_timestamp: int,
    ) -> List[Dict[str, Any]]:
        """
        从数据库读取指定区间 K 线
        """
        return self.database.query_klines(
            symbol=self._storage_symbol,
            market_type=self.market_type,
            timeframe=timeframe,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )

    def sync_missing_data(
        self,
        timeframe: str,
        start_timestamp: int,
        end_timestamp: Optional[int] = None,
    ) -> int:
        """
        自动补齐指定区间内的缺口数据，返回新增条数
        """
        if end_timestamp is None:
            end_timestamp = int(time.time() * 1000)

        frame_ms = self._timeframe_to_milliseconds(timeframe)
        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        safe_now = now_ts - frame_ms
        aligned_end_ts = ((safe_now // frame_ms) * frame_ms) + (frame_ms - 1)
        effective_end_ts = min(end_timestamp, aligned_end_ts)
        clipped_by_listing = False

        listing_ts = self._resolve_listing_timestamp()
        if listing_ts is not None:
            aligned_listing_ts = (listing_ts // frame_ms) * frame_ms
            if aligned_listing_ts > start_timestamp:
                okx_logger.debug(
                    f"[历史数据][补齐缺口][裁剪起点] 币种={self.symbol} 周期={timeframe} "
                    f"原始起点={format_beijing_ts(start_timestamp)} "
                    f"裁剪后={format_beijing_ts(aligned_listing_ts)}"
                )
                start_timestamp = aligned_listing_ts
                clipped_by_listing = True

        if start_timestamp > effective_end_ts:
            if clipped_by_listing:
                okx_logger.debug(
                    f"[历史数据][补齐缺口][跳过] 币种={self.symbol} 周期={timeframe} "
                    "原因=裁剪后的有效区间为空"
                )
                return 0
            okx_logger.info(
                f"[历史数据][补齐缺口][跳过] 币种={self.symbol} 周期={timeframe} "
                "原因=区间全部位于最新未收盘的 K 线之后"
            )
            return 0

        missing_ranges = self.find_missing_ranges(
            timeframe=timeframe,
            start_timestamp=start_timestamp,
            end_timestamp=effective_end_ts,
        )

        total = 0
        for missing_start, missing_end in missing_ranges:
            count = self.sync_range_to_db(
                timeframe=timeframe,
                start_timestamp=missing_start,
                end_timestamp=missing_end,
            )
            total += count

        okx_logger.info(
            f"[历史数据][补齐缺口][完成] 币种={self.symbol} 周期={timeframe} 新增条数={total}"
        )
        return total

    def find_missing_ranges(
        self,
        timeframe: str,
        start_timestamp: int,
        end_timestamp: int,
    ) -> List[Tuple[int, int]]:
        """
        检查数据库中缺失的时间段
        """
        timeframe_ms = self._timeframe_to_milliseconds(timeframe)
        data = self.load_from_db(
            timeframe=timeframe,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )

        if not data:
            return [(start_timestamp, end_timestamp)]

        data.sort(key=lambda item: item["timestamp"])
        missing: List[Tuple[int, int]] = []
        expected = start_timestamp

        for candle in data:
            ts = candle["timestamp"]
            if ts > expected:
                missing.append((expected, ts - timeframe_ms))
            expected = max(expected, ts + timeframe_ms)

        if expected <= end_timestamp:
            missing.append((expected, end_timestamp))

        readable_missing = [
            f"{format_beijing_ts(start)}~{format_beijing_ts(end)}"
            for start, end in missing
        ]
        okx_logger.debug(
            f"[历史数据][缺口分析][详情] 币种={self.symbol} 周期={timeframe} 缺口列表={readable_missing}"
        )
        return [
            (start, end)
            for start, end in missing
            if start <= end
        ]

    def get_existing_range(
        self,
        timeframe: str,
    ) -> Optional[Tuple[int, int]]:
        """
        返回数据库内已有的最早/最晚时间戳
        """
        conn = self.database._get_connection(
            symbol=self._storage_symbol.upper(),
            market_type=self.market_type,
            timeframe=timeframe,
        )
        self.database._init_table(conn)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM klines"
        )
        row = cursor.fetchone()
        conn.close()

        if not row or not row[0] or not row[1]:
            return None

        to_millis = self.database._convert_db_timestamp_to_millis

        return (
            to_millis(row[0]),
            to_millis(row[1]),
        )

    # ========== 工具方法 ==========
    def _prepare_market(self) -> Dict[str, Any]:
        """确保交易所市场元数据可用并返回指定交易对的信息。"""
        markets = getattr(self.exchange, "markets", None) or {}
        if not markets:
            self._load_markets_with_retry()
            markets = getattr(self.exchange, "markets", {})
        if self.symbol not in markets:
            # 部分情况下 load_markets 成功但未包含交易对，再尝试一次
            self._load_markets_with_retry()
        return self._get_market_with_retry()

    def _resolve_listing_timestamp(self) -> Optional[int]:
        """
        获取交易对上市/开始交易时间（毫秒时间戳）。

        优先使用 ccxt 统一后的 market["created"]，
        对 OKX 来说该值来自 contTdSwTime 或 listTime。
        """
        try:
            market = self._prepare_market()
        except Exception as exc:  # noqa: BLE001
            okx_logger.debug(
                f"[历史数据][元数据][上市时间获取失败] 币种={self.symbol} 错误={exc}"
            )
            return None

        created = market.get("created")
        if created not in (None, "", 0, "0"):
            try:
                return int(created)
            except (TypeError, ValueError):
                pass

        info = market.get("info") or {}
        for key in ("contTdSwTime", "listTime"):
            raw = info.get(key)
            if raw in (None, "", 0, "0"):
                continue
            try:
                return int(raw)
            except (TypeError, ValueError):
                continue

        return None

    def _load_markets_with_retry(self) -> None:
        for attempt in range(1, self._max_retries + 1):
            try:
                self.exchange.load_markets()
                return
            except Exception as exc:  # noqa: BLE001
                okx_logger.error(
                    f"[历史数据][元数据][拉取失败] 币种={self.symbol} "
                    f"尝试={attempt}/{self._max_retries} 错误={exc}"
                )
                if attempt < self._max_retries:
                    time.sleep(min(1 * attempt, 5))
                else:
                    raise

    def _get_market_with_retry(self) -> Dict[str, Any]:
        for attempt in range(1, self._max_retries + 1):
            try:
                return self.exchange.market(self.symbol)
            except Exception as exc:  # noqa: BLE001
                okx_logger.error(
                    f"[历史数据][元数据][解析失败] 币种={self.symbol} "
                    f"尝试={attempt}/{self._max_retries} 错误={exc}"
                )
                if attempt < self._max_retries:
                    time.sleep(min(1 * attempt, 5))
                else:
                    raise

    def _timeframe_to_milliseconds(self, timeframe: str) -> int:
        """
        将 OKX 时间周期字符串转换为毫秒
        """
        normalized = timeframe.strip()
        if not normalized:
            raise ValueError("timeframe 不能为空")

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

        if unit == "m" and normalized.endswith("M"):
            unit = "mth"

        if unit not in mapping:
            raise ValueError(f"不支持的 timeframe: {timeframe}")

        return value * mapping[unit]
