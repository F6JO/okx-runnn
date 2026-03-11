from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple
import sqlite3

from lib.db import OkxDatabase


class KlineCache:
    """共用的 K 线缓存服务，按 symbol/market/timeframe 分类存储最近数据。"""

    BEIJING_TZ = timezone(timedelta(hours=8))

    def __init__(
        self,
        *,
        database: OkxDatabase,
        max_length: int = 5000,
        preload_length: int = 1000,
    ) -> None:
        self._database = database
        self._max_length = max_length
        self._preload_length = preload_length
        self._lock = RLock()
        self._store: Dict[Tuple[str, str, str], Deque[Dict[str, Any]]] = {}

    @contextmanager
    def _locked(self) -> Any:
        """对内部存储的并发访问进行序列化。"""
        self._lock.acquire()
        try:
            yield
        finally:
            self._lock.release()

    def _normalize_key(self, symbol: str, market_type: str, timeframe: str) -> Tuple[str, str, str]:
        return (symbol.upper(), market_type, timeframe)

    def _get_or_create_cache(self, key: Tuple[str, str, str]) -> Deque[Dict[str, Any]]:
        cache_deque = self._store.get(key)
        if cache_deque is None:
            cache_deque = deque(maxlen=self._max_length)
            self._store[key] = cache_deque
        return cache_deque

    def _merge_candles(
        self,
        cache_deque: Deque[Dict[str, Any]],
        new_items: Iterable[Dict[str, Any]],
    ) -> None:
        items = [c for c in new_items if isinstance(c, dict)]
        if not items:
            return
        combined: List[Dict[str, Any]] = list(cache_deque)
        combined.extend(items)
        combined.sort(key=lambda c: c.get("timestamp", 0))

        merged: List[Dict[str, Any]] = []
        last_ts: Optional[int] = None
        for candle in combined:
            ts = candle.get("timestamp")
            if ts is None:
                continue
            if last_ts is not None and ts == last_ts:
                merged[-1] = candle
            else:
                merged.append(candle)
                last_ts = ts

        if len(merged) > self._max_length:
            merged = merged[-self._max_length :]

        cache_deque.clear()
        cache_deque.extend(merged)

    def _row_to_dict(self, row: sqlite3.Row) -> Optional[Dict[str, Any]]:
        try:
            ts_millis = self._database._convert_db_timestamp_to_millis(row["timestamp"])  # noqa: SLF001
        except Exception:
            return None

        return {
            "timestamp": ts_millis,
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "volume_contract": row["volume_contract"],
            "volume_currency": row["volume_currency"],
            "volume_quote": row["volume_quote"],
            "confirm": row["confirm"],
        }

    def _fetch_from_db(
        self,
        *,
        symbol: str,
        market_type: str,
        timeframe: str,
        limit: int,
        end_timestamp: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []

        try:
            conn = self._database._get_connection(  # noqa: SLF001
                symbol,
                market_type,
                timeframe,
                create_dirs=False,
                readonly=True,
            )
        except FileNotFoundError:
            return []
        except Exception:
            return []

        rows: List[Dict[str, Any]] = []
        try:
            cursor = conn.cursor()
            params: List[Any] = []
            query = """
                SELECT timestamp, open, high, low, close, volume,
                       volume_contract, volume_currency, volume_quote, confirm
                FROM klines
            """

            if end_timestamp is not None:
                end_dt = datetime.fromtimestamp(
                    end_timestamp / 1000,
                    tz=self.BEIJING_TZ,
                ).replace(tzinfo=None)
                query += "\n                WHERE timestamp <= ?"
                params.append(end_dt)

            query += "\n                ORDER BY timestamp DESC\n                LIMIT ?"
            params.append(limit)

            cursor.execute(query, tuple(params))
            fetched = cursor.fetchall()
            for row in reversed(fetched):
                mapped = self._row_to_dict(row)
                if mapped is not None:
                    rows.append(mapped)
        except sqlite3.OperationalError:
            rows = []
        finally:
            conn.close()

        return rows

    def load_initial(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """
        从数据库中拉取最新的 K 线并预热缓存。
        """
        symbol_key = symbol.upper()
        effective_limit = max(limit, self._preload_length)
        effective_limit = min(effective_limit, self._max_length)
        rows = self._fetch_from_db(
            symbol=symbol_key,
            market_type=market_type,
            timeframe=timeframe,
            limit=effective_limit,
        )

        key = self._normalize_key(symbol, market_type, timeframe)
        with self._locked():
            cache_deque = self._get_or_create_cache(key)
            cache_deque.clear()
            cache_deque.extend(rows)

        return rows

    def append(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
        candle: Dict[str, Any],
    ) -> None:
        """
        将最新一根 K 线写入缓存。
        """
        self.extend(symbol, market_type, timeframe, [candle])

    def extend(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
        candles: Iterable[Dict[str, Any]],
    ) -> None:
        """
        批量写入 K 线。
        """
        key = self._normalize_key(symbol, market_type, timeframe)
        with self._locked():
            cache_deque = self._get_or_create_cache(key)
            ordered = sorted(
                (c for c in candles if isinstance(c, dict)),
                key=lambda c: c.get("timestamp", 0),
            )
            self._merge_candles(cache_deque, ordered)

    def get_recent(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []

        key = self._normalize_key(symbol, market_type, timeframe)
        with self._locked():
            snapshot = list(self._store.get(key, []))

        if not snapshot:
            rows = self.load_initial(symbol, market_type, timeframe, limit)
            return rows[-limit:]

        if len(snapshot) >= limit:
            return snapshot[-limit:]

        earliest_ts = snapshot[0].get("timestamp")
        if earliest_ts is None:
            return snapshot[-limit:]

        missing = limit - len(snapshot)
        older = self._fetch_from_db(
            symbol=key[0],
            market_type=market_type,
            timeframe=timeframe,
            limit=missing,
            end_timestamp=earliest_ts - 1,
        )
        if older:
            self.extend(symbol, market_type, timeframe, older)
            with self._locked():
                refreshed = list(self._store.get(key, []))
            return refreshed[-limit:]
        return snapshot[-limit:]

    def get_range(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
        end_timestamp: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []

        key = self._normalize_key(symbol, market_type, timeframe)
        with self._locked():
            snapshot = [c for c in self._store.get(key, []) if c.get("timestamp") is not None]

        filtered = [c for c in snapshot if c["timestamp"] <= end_timestamp]
        if len(filtered) >= limit:
            return filtered[-limit:]

        missing = limit - len(filtered)
        fetch_end = end_timestamp
        if filtered:
            earliest_current = filtered[0]["timestamp"]
            fetch_end = min(fetch_end, earliest_current - 1)

        older = self._fetch_from_db(
            symbol=key[0],
            market_type=market_type,
            timeframe=timeframe,
            limit=missing,
            end_timestamp=fetch_end,
        )

        if older:
            self.extend(symbol, market_type, timeframe, older)
            with self._locked():
                refreshed = [
                    c
                    for c in self._store.get(key, [])
                    if c.get("timestamp") is not None and c["timestamp"] <= end_timestamp
                ]
            return refreshed[-limit:]

        return filtered[-limit:]

    def prune(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
    ) -> None:
        key = self._normalize_key(symbol, market_type, timeframe)
        with self._locked():
            cache_deque = self._store.get(key)
            if not cache_deque:
                return
            while len(cache_deque) > self._max_length:
                cache_deque.popleft()

    def clear(
        self,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> None:
        symbol_key = symbol.upper() if symbol else None
        with self._locked():
            if symbol_key is None and timeframe is None:
                self._store.clear()
                return

            targets = [
                k for k in self._store
                if (symbol_key is None or k[0] == symbol_key)
                and (timeframe is None or k[2] == timeframe)
            ]
            for key in targets:
                self._store.pop(key, None)

    def has(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
    ) -> bool:
        key = self._normalize_key(symbol, market_type, timeframe)
        with self._locked():
            cache_deque = self._store.get(key)
            return bool(cache_deque)
