from __future__ import annotations

import logging
from contextlib import contextmanager
import threading
import time
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Optional, TYPE_CHECKING
import importlib

from lib.globalVar import getVar, setVar
from lib.logger import okx_logger, format_beijing_ts
from okx_api.okx_account import OkxAccount
from module.strategResuController import StrategResuController

if TYPE_CHECKING:
    from module.historyController import HistoryController


class MonitorController:
    """占位的监控控制器，后续可扩展具体逻辑。"""

    def __init__(self, cli_args=None):
        self._cli_args = cli_args
        self._beijing_tz = timezone(timedelta(hours=8))
        self._history_controller: Optional["HistoryController"] = None
        self._active_subscriptions: list[tuple[str, str, str]] = []
        self._live_candles: dict[tuple[str, str, str], dict] = {}
        setVar("LIVE_CANDLES", self._live_candles)
        self._last_committed_ts: dict[tuple[str, str, str], int] = {}
        self._history_cache: dict[tuple[str, str], Any] = {}
        self._registered_candle_handlers: set[tuple[str, str, str]] = set()
        self._monitor_context: dict[str, dict] = {}
        self._strategy_class_cache: dict[str, type] = {}
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._ws_ready_event: Optional[threading.Event] = None
        self._ws_pause_logged = False
        self._kline_cache = getVar("KLINE_CACHE")
        self._default_strategy_interval = 60  # seconds
        self._strategy_result_controller = StrategResuController()

    def attach_cli_args(self, cli_args) -> None:
        """保存命令行解析器引用，便于输出帮助或提示。"""
        self._cli_args = cli_args

    def attach_history_controller(self, history_controller: "HistoryController") -> None:
        """注入 HistoryController 以便运行数据检查。"""
        self._history_controller = history_controller

    def attach_strategy_result_controller(
        self,
        controller: Optional[StrategResuController],
    ) -> None:
        """允许外部覆盖策略结果处理器。"""
        if controller is None:
            return
        self._strategy_result_controller = controller

    def run(self, args) -> None:
        """运行实时监控流程（占位实现）。"""
        raw_pairs = getattr(args, "pairs", None) or []

        parsed_pairs = []
        for raw in raw_pairs:
            try:
                parsed = self._parse_pair(raw)
                parsed_pairs.append(parsed)
            except ValueError as exc:
                okx_logger.error(f"[命令行][监控][参数错误] pair={raw} 错误={exc}")
                return

        self._monitor_context = self._build_monitor_context(parsed_pairs)

        self._ensure_history(parsed_pairs)
        self._top_up_recent_candles(parsed_pairs)
        self._run_history_check()
        self._preload_kline_cache(parsed_pairs)
        self._active_subscriptions = self._start_basic_subscriptions(parsed_pairs)

        okx_logger.info(
            "[命令行][监控][解析完成] "
            f"共收到 {len(parsed_pairs)} 个监控配置: {parsed_pairs}"
        )

        if self._active_subscriptions:
            self._start_monitor_thread()
            self._wait_until_stopped()

    def _parse_pair(self, raw: str) -> dict:
        """
        将形如 \"symbol=btc,type=swap,frames=5m|15m,strategies=macd|boll\" 的参数解析为字典。
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
            "interval": items.get("interval"),
        }
        okx_logger.debug(
            "[命令行][监控][参数解析] "
            f"原始={raw} 解析结果={parsed}"
        )
        return parsed

    def _build_monitor_context(self, parsed_pairs: list[dict]) -> dict[str, dict]:
        """为 symbol/market/timeframe 组合构建策略映射。"""
        context: dict[str, dict] = {}

        for index, config in enumerate(parsed_pairs):
            symbols = config.get("symbol", [])
            market_types = config.get("type", [])
            frames = config.get("frames", [])
            strategies = config.get("strategies", [])

            for symbol in symbols:
                for market_type in market_types:
                    for frame in frames:
                        try:
                            key, norm_symbol, norm_market, norm_frame = (
                                self._derive_context_key(symbol, market_type, frame)
                            )
                        except ValueError as exc:
                            okx_logger.error(
                                "[命令行][监控][上下文生成失败] symbol=%s market=%s frame=%s 错误=%s",
                                symbol,
                                market_type,
                                frame,
                                exc,
                            )
                            continue

                        interval_seconds = self._resolve_strategy_interval(config)

                        entry = context.setdefault(
                            key,
                            {
                                "symbol": norm_symbol,
                                "market_type": norm_market,
                                "timeframe": norm_frame,
                                "strategies": [],
                                "raw_configs": [],
                                "last_run_ts": None,
                                "last_strategy_results": {},
                                "strategy_interval": interval_seconds,
                                "remaining_seconds": 0,
                            },
                        )

                        if "strategy_interval" not in entry:
                            entry["strategy_interval"] = interval_seconds
                        if "remaining_seconds" not in entry:
                            entry["remaining_seconds"] = 0

                        entry["raw_configs"].append(
                            {
                                "source_index": index,
                                "config": config,
                            }
                        )

                        for strategy in strategies:
                            normalized = strategy.strip()
                            if not normalized:
                                continue
                            if normalized not in entry["strategies"]:
                                entry["strategies"].append(normalized)

        okx_logger.info(
            "[命令行][监控][上下文初始化] 组合数=%s",
            len(context),
        )
        return context

    def _derive_context_key(
        self,
        symbol: str,
        market_type: str,
        frame: str,
    ) -> tuple[str, str, str, str]:
        norm_symbol, norm_market, norm_frame = self._normalize_context_components(
            symbol,
            market_type,
            frame,
        )
        key = self._format_context_key(norm_symbol, norm_market, norm_frame)
        return key, norm_symbol, norm_market, norm_frame

    def _normalize_context_components(
        self,
        symbol: str,
        market_type: str,
        frame: str,
    ) -> tuple[str, str, str]:
        if not symbol:
            raise ValueError("symbol 不能为空")
        if not market_type:
            raise ValueError("market_type 不能为空")
        if not frame:
            raise ValueError("timeframe 不能为空")

        normalized_market = market_type.strip().lower()
        normalized_frame = frame.strip()
        normalized_symbol = self._normalize_symbol_for_context(
            raw_symbol=symbol.strip(),
            market_type=normalized_market,
        )

        if not normalized_symbol or not normalized_market or not normalized_frame:
            raise ValueError("symbol/market/timeframe 不能为空字符串")
        return normalized_symbol, normalized_market, normalized_frame

    def _normalize_symbol_for_context(self, raw_symbol: str, market_type: str) -> str:
        """
        使用与 OkxCoin 相同的规则，将用户输入的 symbol 统一到订阅实际使用的标准格式。
        """
        symbol = raw_symbol.upper()
        if "/" in symbol or ":" in symbol or symbol.endswith("-SWAP"):
            return symbol

        quote_asset = "USDT"
        if market_type in {"swap", "future", "futures"}:
            return f"{symbol}/{quote_asset}:USDT"
        return f"{symbol}/{quote_asset}"

    def _format_context_key(
        self,
        symbol: str,
        market_type: str,
        frame: str,
    ) -> str:
        return f"{symbol}-{market_type}-{frame}"

    def _resolve_strategy_interval(self, config: dict) -> int:
        """Determine strategy interval (seconds) for a config, fallback to default."""
        interval = config.get("interval")
        if isinstance(interval, (int, float)) and interval > 0:
            return int(interval)
        if isinstance(interval, str):
            try:
                value = int(float(interval.strip()))
            except (TypeError, ValueError):
                return self._default_strategy_interval
            if value > 0:
                return value
        return self._default_strategy_interval

    def _ensure_history(self, parsed_pairs: list[dict]) -> None:
        """根据监控配置补齐所需的历史数据。"""
        account = getVar("OKX_ACCOUNT")
        if account is None:
            okx_logger.error("[命令行][监控][账户缺失] 未检测到 OKX 账户配置，无法预热历史数据")
            return

        default_days = getVar("MONITOR_DEFAULT_HISTORY_DAYS", {}) or {}
        if not isinstance(default_days, dict):
            okx_logger.error("[命令行][监控][配置错误] MONITOR_DEFAULT_HISTORY_DAYS 需为字典")
            return

        for config in parsed_pairs:
            symbols = config.get("symbol", [])
            market_types = config.get("type", [])
            frames = config.get("frames", [])

            for symbol in symbols:
                for market_type in market_types:
                    try:
                        coin = account.get_OkxCoin(symbol=symbol, market_type=market_type)
                    except Exception as exc:  # noqa: BLE001
                        okx_logger.error(
                            f"[命令行][监控][历史预热失败] 币种={symbol} 市场={market_type} 错误={exc}"
                        )
                        continue

                    history = coin.get_history()
                    frame_records = []

                    for frame in frames:
                        days = default_days.get(frame)
                        okx_logger.debug(
                            "[命令行][监控][预热准备] "
                            f"symbol={symbol} market={market_type} frame={frame} days={days}"
                        )
                        if days is None:
                            okx_logger.error(
                                f"[命令行][监控][历史预热跳过] 周期={frame} 缺少默认天数配置"
                            )
                            continue

                        end_dt = datetime.now(self._beijing_tz)
                        start_dt = end_dt - timedelta(days=days)
                        start_ts = int(start_dt.timestamp() * 1000)
                        end_ts = int(end_dt.timestamp() * 1000)
                        okx_logger.debug(
                            "[命令行][监控][预热时间窗口] "
                            f"symbol={symbol} market={market_type} frame={frame} "
                            f"start={start_dt.strftime('%Y-%m-%d %H:%M:%S')} "
                            f"end={end_dt.strftime('%Y-%m-%d %H:%M:%S')}"
                        )

                        try:
                            inserted = self._sync_history_with_mute(
                                history=history,
                                frame=frame,
                                start_ts=start_ts,
                                end_ts=end_ts,
                            )
                        except Exception as exc:  # noqa: BLE001
                            okx_logger.error(
                                f"[命令行][监控][历史预热异常] 币种={coin.symbol} 周期={frame} 错误={exc}"
                            )
                            continue

                        frame_records.append((frame, inserted, start_dt))
                        okx_logger.debug(
                            "[命令行][监控][预热结果] "
                            f"symbol={symbol} market={market_type} frame={frame} "
                            f"inserted={inserted}"
                        )

                    if not frame_records:
                        okx_logger.debug(
                            "[命令行][监控][预热汇总跳过] "
                            f"symbol={symbol} market={market_type} 未成功处理任何周期"
                        )
                        continue

                    detail = ", ".join(
                        f"{frame}+{inserted}" if inserted > 0 else f"{frame}-"
                        for frame, inserted, _ in frame_records
                    )
                    total_inserted = sum(r[1] for r in frame_records)
                    base_date = min(r[2] for r in frame_records).strftime("%Y%m%d")
                    summary = (
                        "[命令行][监控][历史预热汇总] "
                        f"币种={coin.symbol} 市场={market_type} 起始={base_date}- 结果={detail}"
                    )
                    if total_inserted > 0:
                        okx_logger.info(summary)
                    else:
                        okx_logger.debug(summary)

    def _run_history_check(self) -> None:
        if self._history_controller is None:
            okx_logger.debug("[命令行][监控][历史检查] 未注入 HistoryController，跳过检查")
            return

        check_args = SimpleNamespace(check=True, time_interval=None)
        self._history_controller.attach_cli_args(self._cli_args)
        self._history_controller.run_check(check_args)

    def _preload_kline_cache(self, parsed_pairs: list[dict]) -> None:
        """
        历史数据校验完成后，预热 K 线缓存以降低后续查询压力。
        """
        cache = self._kline_cache or getVar("KLINE_CACHE")
        self._kline_cache = cache
        if cache is None:
            okx_logger.error("[命令行][监控][缓存预热] 未检测到 KlineCache，跳过")
            return

        targets: set[tuple[str, str, str]] = set()
        if self._monitor_context:
            for entry in self._monitor_context.values():
                targets.add(
                    (
                        entry.get("symbol"),
                        entry.get("market_type"),
                        entry.get("timeframe"),
                    )
                )
        else:
            for config in parsed_pairs:
                symbols = config.get("symbol", [])
                market_types = config.get("type", [])
                frames = config.get("frames", [])
                for symbol in symbols:
                    for market_type in market_types:
                        for frame in frames:
                            targets.add((symbol, market_type, frame))

        preload_limit = getattr(cache, "_preload_length", 1000)
        for symbol, market_type, frame in targets:
            try:
                cache.load_initial(
                    symbol=symbol,
                    market_type=market_type,
                    timeframe=frame,
                    limit=preload_limit,
                )
                okx_logger.info(
                    "[命令行][监控][缓存预热成功] symbol=%s market=%s frame=%s",
                    symbol,
                    market_type,
                    frame,
                )
            except Exception as exc:  # noqa: BLE001
                okx_logger.warning(
                    "[命令行][监控][缓存预热失败] symbol=%s market=%s frame=%s 错误=%s",
                    symbol,
                    market_type,
                    frame,
                    exc,
                )

    def _sync_history_with_mute(
        self,
        history,
        frame: str,
        start_ts: int,
        end_ts: int,
    ) -> int:
        with self._mute_noncritical_logs():
            return history.sync_missing_data(
                timeframe=frame,
                start_timestamp=start_ts,
                end_timestamp=end_ts,
            )

    @contextmanager
    def _mute_noncritical_logs(self):
        original_level = okx_logger.level
        debug_mode = getVar("DEBUG_MODE", False)
        try:
            if not debug_mode:
                okx_logger.debug("[命令行][监控][预热静默] 临时提升日志级别以压制冗余输出")
                okx_logger.setLevel(logging.WARNING)
            yield
        finally:
            okx_logger.setLevel(original_level)
            if not debug_mode:
                okx_logger.debug("[命令行][监控][预热静默] 恢复原始日志级别")

    def _start_basic_subscriptions(self, parsed_pairs: list[dict]) -> list[tuple[str, str, str]]:
        """
        根据监控配置发起基础 K 线订阅，返回成功的订阅组合。
        """
        account = getVar("OKX_ACCOUNT")
        if not isinstance(account, OkxAccount):
            okx_logger.error("[命令行][监控][订阅失败] 未检测到有效的 OKX 账户实例")
            return []
        if not getattr(account, "enable_websocket", False):
            okx_logger.error("[命令行][监控][订阅失败] 当前账户未启用 WebSocket，无法执行实时订阅")
            return []

        if self._ws_ready_event is None:
            event = getattr(account, "ws_ready_event", None)
            if isinstance(event, threading.Event):
                self._ws_ready_event = event

        active: list[tuple[str, str, str]] = []

        for config in parsed_pairs:
            symbols = config.get("symbol", [])
            market_types = config.get("type", [])
            frames = config.get("frames", [])

            for symbol in symbols:
                for market_type in market_types:
                    try:
                        coin = account.get_OkxCoin(symbol=symbol, market_type=market_type)
                        market_watch = coin.get_market_watch()
                    except Exception as exc:  # noqa: BLE001
                        okx_logger.error(
                            f"[命令行][监控][订阅失败] 币种={symbol} 市场={market_type} 错误={exc}"
                        )
                        continue

                    for frame in frames:
                        key = (coin.symbol, coin.market_type, frame)
                        if key in active:
                            okx_logger.debug(
                                "[命令行][监控][订阅跳过] 已存在订阅 "
                                f"symbol={coin.symbol} market={coin.market_type} frame={frame}"
                            )
                            continue

                        okx_logger.debug(
                            "[命令行][监控][订阅准备] "
                            f"symbol={coin.symbol} market={coin.market_type} frame={frame}"
                        )

                        try:
                            success = market_watch.subscribe_channel("candle", timeframe=frame)
                        except Exception as exc:  # noqa: BLE001
                            okx_logger.error(
                                f"[命令行][监控][订阅异常] 币种={coin.symbol} 周期={frame} 错误={exc}"
                            )
                            continue

                        if success:
                            active.append(key)
                            history = self._get_history_instance(coin)
                            self._register_candle_handler(
                                market_watch=market_watch,
                                coin=coin,
                                timeframe=frame,
                                history=history,
                            )
                            okx_logger.info(
                                "[命令行][监控][订阅成功] "
                                f"symbol={coin.symbol} market={coin.market_type} frame={frame}"
                            )
                        else:
                            okx_logger.error(
                                "[命令行][监控][订阅失败] "
                                f"symbol={coin.symbol} market={coin.market_type} frame={frame}"
                            )

        if not active:
            okx_logger.error("[命令行][监控][订阅结果] 未成功建立任何实时订阅")
        else:
            okx_logger.info(
                "[命令行][监控][订阅结果] "
                f"共建立 {len(active)} 项 K 线订阅: {active}"
            )
        return active

    def _start_monitor_thread(self) -> None:
        """
        启动监控守护线程，保持实时监听常驻。
        """
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="OkxMonitorLoop",
            daemon=False,
        )
        self._monitor_thread.start()

    def _wait_until_stopped(self) -> None:
        """
        阻塞主线程，直到监控线程结束或收到中断信号。
        """
        try:
            while self._monitor_thread and self._monitor_thread.is_alive():
                time.sleep(0.5)
        except KeyboardInterrupt:
            okx_logger.info("[命令行][监控] 收到中断信号，准备停止实时监控。")
            self._stop_event.set()
            if self._monitor_thread:
                self._monitor_thread.join(timeout=5)

    def _monitor_loop(self) -> None:
        """
        实时监控线程入口，后续可在此调度策略、健康检查等任务。
        """
        okx_logger.info("[命令行][监控] 实时监控线程已启动。")
        last_tick = time.monotonic()
        try:
            while not self._stop_event.is_set():
                if self._ws_ready_event and not self._ws_ready_event.is_set():
                    if not self._ws_pause_logged:
                        okx_logger.warning(
                            "[命令行][监控][暂停] WebSocket 连接未就绪，策略轮询已暂挂"
                        )
                        self._ws_pause_logged = True
                    time.sleep(1)
                    continue
                if self._ws_pause_logged:
                    okx_logger.info(
                        "[命令行][监控][恢复] WebSocket 连接恢复，重新开始策略轮询"
                    )
                    self._ws_pause_logged = False
                now = time.monotonic()
                elapsed = max(0.0, now - last_tick)
                last_tick = now
                if elapsed > 0:
                    self._process_strategy_intervals(elapsed)
                time.sleep(1)
        finally:
            okx_logger.info("[命令行][监控] 实时监控线程已退出。")

    def _process_strategy_intervals(self, elapsed_seconds: float) -> None:
        if not self._monitor_context:
            return

        active_keys = {k for k in self._active_subscriptions}

        for entry_key, context in list(self._monitor_context.items()):
            symbol = context.get("symbol")
            market_type = context.get("market_type")
            timeframe = context.get("timeframe")
            tuple_key = (symbol, market_type, timeframe)

            if active_keys and tuple_key not in active_keys:
                continue

            interval = context.get("strategy_interval") or self._default_strategy_interval
            try:
                interval = float(interval)
            except (TypeError, ValueError):
                interval = float(self._default_strategy_interval)
            if interval <= 0:
                interval = float(self._default_strategy_interval)

            remaining = context.get("remaining_seconds", interval)
            try:
                remaining = float(remaining)
            except (TypeError, ValueError):
                remaining = interval

            remaining -= elapsed_seconds
            triggered = False
            while remaining <= 0:
                triggered = True
                self._trigger_strategy_execution(entry_key, context)
                remaining += interval

            context["remaining_seconds"] = remaining
            context["strategy_interval"] = interval

            if triggered:
                context["last_run_ts"] = int(time.time() * 1000)

    def _trigger_strategy_execution(self, context_key: str, context: dict) -> None:
        symbol = context.get("symbol")
        market_type = context.get("market_type")
        timeframe = context.get("timeframe")
        strategies = context.get("strategies", [])
        key_tuple = (symbol, market_type, timeframe)
        latest = self._live_candles.get(key_tuple)
        if not latest:
            cache = self._kline_cache or getVar("KLINE_CACHE")
            self._kline_cache = cache
            if cache is not None and symbol and market_type and timeframe:
                try:
                    cached_rows = cache.get_recent(symbol, market_type, timeframe, 1)
                except Exception as exc:  # noqa: BLE001
                    okx_logger.debug(
                        "[命令行][监控][策略调度回退失败] key=%s 错误=%s",
                        key_tuple,
                        exc,
                        exc_info=True,
                    )
                    cached_rows = []
                if cached_rows:
                    latest = dict(cached_rows[-1])
                    okx_logger.debug(
                        "[命令行][监控][策略调度回退] 使用缓存最新K线 key=%s",
                        key_tuple,
                    )
        if not latest:
            okx_logger.debug(
                "[命令行][监控][策略调度跳过] 无最新K线 key=%s",
                key_tuple,
            )
            return

        okx_logger.debug(
            "[命令行][监控][策略调度] key=%s symbol=%s market=%s frame=%s strategies=%s",
            context_key,
            symbol,
            market_type,
            timeframe,
            strategies,
        )

        pool = getVar("GLOBAL_THREAD_POOL")
        for name in strategies:
            normalized = (name or "").strip()
            if not normalized:
                continue
            task_kwargs = {
                "strategy_name": normalized,
                "symbol": symbol,
                "market_type": market_type,
                "timeframe": timeframe,
                "latest": dict(latest),
            }
            if pool is not None and hasattr(pool, "submit"):
                try:
                    pool.submit(
                        self._run_strategy_task,
                        **task_kwargs,
                    )
                    okx_logger.debug(
                        "[命令行][监控][策略异步提交] name=%s symbol=%s market=%s frame=%s",
                        normalized,
                        symbol,
                        market_type,
                        timeframe,
                    )
                    continue
                except Exception as exc:  # noqa: BLE001
                    okx_logger.warning(
                        "[命令行][监控][策略异步提交失败] name=%s symbol=%s market=%s frame=%s 错误=%s",
                        normalized,
                        symbol,
                        market_type,
                        timeframe,
                        exc,
                    )
            self._run_strategy_task(**task_kwargs)

    def _run_strategy_task(
        self,
        *,
        strategy_name: str,
        symbol: str,
        market_type: str,
        timeframe: str,
        latest: dict,
    ) -> None:
        success = self._execute_strategy(
            strategy_name=strategy_name,
            symbol=symbol,
            market_type=market_type,
            timeframe=timeframe,
            latest=latest,
        )

        controller = getattr(self, "_strategy_result_controller", None)
        if controller is None:
            return
        try:
            controller.handle_results(
                symbol=symbol,
                market_type=market_type,
                timeframe=timeframe,
                results={strategy_name: success},
            )
        except Exception as exc:  # noqa: BLE001
            okx_logger.warning(
                "[命令行][监控][策略结果处理异常] name=%s symbol=%s market=%s frame=%s 错误=%s",
                strategy_name,
                symbol,
                market_type,
                timeframe,
                exc,
            )

    def _execute_strategy(
        self,
        *,
        strategy_name: str,
        symbol: str,
        market_type: str,
        timeframe: str,
        latest: dict,
    ) -> bool:
        try:
            strategy_cls = self._load_strategy_class(strategy_name)
        except Exception as exc:  # noqa: BLE001
            okx_logger.error(
                "[命令行][监控][策略加载失败] name=%s symbol=%s market=%s frame=%s 错误=%s",
                strategy_name,
                symbol,
                market_type,
                timeframe,
                exc,
            )
            return False

        try:
            instance = strategy_cls(
                symbol=symbol,
                market_type=market_type,
                timeframe=timeframe,
                latest=latest,
            )
        except Exception as exc:  # noqa: BLE001
            okx_logger.error(
                "[命令行][监控][策略实例化失败] name=%s symbol=%s market=%s frame=%s 错误=%s",
                strategy_name,
                symbol,
                market_type,
                timeframe,
                exc,
            )
            return False

        try:
            result = instance.start()
        except Exception as exc:  # noqa: BLE001
            okx_logger.error(
                "[命令行][监控][策略执行异常] name=%s symbol=%s market=%s frame=%s 错误=%s",
                strategy_name,
                symbol,
                market_type,
                timeframe,
                exc,
            )
            return False

        okx_logger.debug(
            "[命令行][监控][策略结果] name=%s symbol=%s market=%s frame=%s result=%s",
            strategy_name,
            symbol,
            market_type,
            timeframe,
            result,
        )
        return bool(result)

    def _load_strategy_class(self, strategy_name: str) -> type:
        cached = self._strategy_class_cache.get(strategy_name)
        if cached is not None:
            return cached

        module_name = f"strateg.{strategy_name}.main"
        module = importlib.import_module(module_name)
        try:
            strategy_cls = getattr(module, "strateg")
        except AttributeError as exc:
            raise AttributeError(
                f"策略模块 {module_name} 缺少类 strateg"
            ) from exc

        self._strategy_class_cache[strategy_name] = strategy_cls
        return strategy_cls

    def _get_history_instance(self, coin) -> Any:
        """
        获取并缓存指定交易对的历史数据实例。
        """
        cache_key = (coin.symbol, coin.market_type)
        history = self._history_cache.get(cache_key)
        if history is None:
            history = coin.get_history()
            self._history_cache[cache_key] = history
        return history

    def _register_candle_handler(
        self,
        *,
        market_watch,
        coin,
        timeframe: str,
        history,
    ) -> None:
        """
        注册实时 K 线回调，检测收盘并写入数据库。
        """
        key = (coin.symbol, coin.market_type, timeframe)
        if key in self._registered_candle_handlers:
            return

        timeframes = getattr(getattr(coin, "exchange", None), "timeframes", {}) or {}
        ws_timeframe = timeframes.get(timeframe, timeframe)
        expected_channel = f"candle{ws_timeframe}"
        try:
            expected_inst = market_watch._format_symbol_for_ws()  # noqa: SLF001
        except Exception:
            expected_inst = None

        def _handle(entry: dict) -> None:
            channel = entry.get("channel")
            if channel and channel != expected_channel:
                return
            inst_id = entry.get("instId")
            if expected_inst and inst_id and inst_id != expected_inst:
                return

            values = entry.get("values")
            if not values or len(values) < 6:
                return

            try:
                timestamp = int(values[0])
            except (TypeError, ValueError):
                okx_logger.warning(
                    "[命令行][监控][K线解析失败] 无法解析时间戳 "
                    f"symbol={coin.symbol} frame={timeframe} raw={values[0]}"
                )
                return

            def _to_float(index: int) -> float:
                try:
                    return float(values[index])
                except (IndexError, TypeError, ValueError):
                    return 0.0

            candle = {
                "timestamp": timestamp,
                "open": _to_float(1),
                "high": _to_float(2),
                "low": _to_float(3),
                "close": _to_float(4),
                "volume": _to_float(5),
                "volume_currency": _to_float(6),
                "volume_quote": _to_float(7),
                "confirm": self._normalize_confirm(values[-1]),
            }
            candle["volume_contract"] = candle["volume"]

            self._live_candles[key] = candle

            if candle["confirm"] == 1:
                self._persist_closed_candle(
                    key=key,
                    candle=candle,
                    history=history,
                    timeframe=timeframe,
                )

        market_watch.start_real_time_candle_monitoring(_handle)
        self._registered_candle_handlers.add(key)

    @staticmethod
    def _normalize_confirm(raw_confirm: Any) -> int:
        """
        将 confirm 字段转换为 0/1。
        """
        if isinstance(raw_confirm, (int, float)):
            return 1 if int(raw_confirm) == 1 else 0
        if isinstance(raw_confirm, str):
            return 1 if raw_confirm.strip() in {"1", "true", "True"} else 0
        if isinstance(raw_confirm, bool):
            return int(raw_confirm)
        return 0

    def _persist_closed_candle(
        self,
        *,
        key: tuple[str, str, str],
        candle: dict,
        history,
        timeframe: str,
    ) -> None:
        """
        将已收盘的 K 线写入历史数据库。
        """
        last_ts = self._last_committed_ts.get(key)
        ts = candle.get("timestamp")
        ts_display = format_beijing_ts(ts) if ts is not None else "未知"
        if last_ts is not None and ts <= last_ts:
            okx_logger.debug(
                "[命令行][监控][落库跳过] 重复时间戳 "
                f"key={key} ts={ts_display}"
            )
            return

        payload = [
            {
                "timestamp": ts,
                "open": candle.get("open"),
                "high": candle.get("high"),
                "low": candle.get("low"),
                "close": candle.get("close"),
                "volume": candle.get("volume"),
                "volume_contract": candle.get("volume_contract"),
                "volume_currency": candle.get("volume_currency"),
                "volume_quote": candle.get("volume_quote"),
                "confirm": 1,
            }
        ]

        try:
            success = history.database.insert_klines(
                symbol=history._storage_symbol,
                market_type=history.market_type,
                timeframe=timeframe,
                data=json.dumps(payload),
            )
        except Exception as exc:  # noqa: BLE001
            okx_logger.error(
                "[命令行][监控][落库异常] "
                f"key={key} 错误={exc}"
            )
            return

        if success:
            self._append_cache(
                key=key,
                timeframe=timeframe,
                candle=payload[0],
                history=history,
            )
            self._last_committed_ts[key] = ts
            self._live_candles.pop(key, None)
            ts_readable = ts_display
            okx_logger.debug(
                "[命令行][监控][落库成功] "
                f"key={key} ts={ts_readable}"
            )
        else:
            ts_readable = ts_display
            okx_logger.error(
                "[命令行][监控][落库失败] "
                f"key={key} ts={ts_readable}"
            )

    def _append_cache(
        self,
        *,
        key: tuple[str, str, str],
        timeframe: str,
        candle: dict,
        history,
    ) -> None:
        cache = self._kline_cache or getVar("KLINE_CACHE")
        if cache is None:
            return
        self._kline_cache = cache

        symbol_key, market_type, _ = key
        cache_symbol = symbol_key or getattr(history, "_storage_symbol", None)
        if not cache_symbol:
            return

        try:
            cache.append(
                cache_symbol,
                market_type,
                timeframe,
                dict(candle),
            )
            candle_ts = candle.get("timestamp")
            ts_for_log = (
                f"{format_beijing_ts(candle_ts)}({candle_ts})"
                if candle_ts is not None
                else "unknown"
            )
            okx_logger.debug(
                "[命令行][监控][缓存写入成功] symbol=%s market=%s frame=%s ts=%s",
                cache_symbol,
                market_type,
                timeframe,
                ts_for_log,
            )
        except Exception as exc:  # noqa: BLE001
            okx_logger.warning(
                "[命令行][监控][缓存写入失败] symbol=%s market=%s frame=%s 错误=%s",
                cache_symbol,
                market_type,
                timeframe,
                exc,
            )

    def _top_up_recent_candles(self, parsed_pairs: list[dict]) -> None:
        """
        在历史预热后，再次补齐最新收盘的 K 线，避免下载耗时造成缺口。
        """
        account = getVar("OKX_ACCOUNT")
        if account is None:
            return

        for config in parsed_pairs:
            symbols = config.get("symbol", [])
            market_types = config.get("type", [])
            frames = config.get("frames", [])

            for symbol in symbols:
                for market_type in market_types:
                    try:
                        coin = account.get_OkxCoin(symbol=symbol, market_type=market_type)
                        history = self._get_history_instance(coin)
                    except Exception as exc:  # noqa: BLE001
                        okx_logger.debug(
                            "[命令行][监控][补齐跳过] "
                            f"symbol={symbol} market={market_type} 错误={exc}",
                            exc_info=True,
                        )
                        continue

                    for frame in frames:
                        frame_ms = history._timeframe_to_milliseconds(frame)  # noqa: SLF001
                        existing = history.get_existing_range(frame)
                        if existing is None:
                            continue

                        _, max_ts = existing
                        start_ts = max_ts + frame_ms
                        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
                        if start_ts > now_ts:
                            continue

                        start_str = format_beijing_ts(start_ts)
                        now_str = format_beijing_ts(now_ts)
                        okx_logger.debug(
                            "[命令行][监控][补齐检查] "
                            f"symbol={coin.symbol} market={market_type} frame={frame} "
                            f"start_ts={start_str}({start_ts}) now_ts={now_str}({now_ts})"
                        )

                        inserted = history.sync_missing_data(
                            timeframe=frame,
                            start_timestamp=start_ts,
                            end_timestamp=now_ts,
                        )
                        if inserted:
                            okx_logger.info(
                                "[命令行][监控][补齐新增] "
                                f"symbol={coin.symbol} market={market_type} frame={frame} "
                                f"新增条数={inserted}"
                            )
