import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler

_console_handler: Optional[RichHandler] = None
_error_hooks: List[Callable[[logging.LogRecord, str], None]] = []
_BEIJING_TZ = timezone(timedelta(hours=8))


def _to_beijing_datetime(timestamp: float) -> datetime:
    """Convert a unix timestamp to Beijing timezone datetime."""
    return datetime.fromtimestamp(timestamp, tz=_BEIJING_TZ)


class BeijingFormatter(logging.Formatter):
    """Formatter that forces %(asctime)s into Beijing time."""

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:  # noqa: N802
        dt = _to_beijing_datetime(record.created)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat(timespec="seconds")


class BeijingRichHandler(RichHandler):
    """Rich handler that renders log timestamps in Beijing time."""

    def render(  # type: ignore[override]
        self,
        *,
        record: logging.LogRecord,
        traceback,
        message_renderable,
    ):
        path = Path(record.pathname).name
        level = self.get_level_text(record)
        time_format = None if self.formatter is None else self.formatter.datefmt
        log_time = _to_beijing_datetime(record.created)

        log_renderable = self._log_render(
            self.console,
            [message_renderable] if not traceback else [message_renderable, traceback],
            log_time=log_time,
            time_format=time_format,
            level=level,
            path=path,
            line_no=record.lineno,
            link_path=record.pathname if self.enable_link_path else None,
        )
        return log_renderable


def format_beijing_ts(ts_ms: object) -> str:
    """Convert milliseconds timestamp to Beijing time string."""
    try:
        ts_sec = float(ts_ms) / 1000
    except (TypeError, ValueError):
        return str(ts_ms)
    dt = _to_beijing_datetime(ts_sec)
    return dt.strftime("%Y-%m-%d %H:%M:%S CST")


def format_beijing_range(start_ms: object, end_ms: object) -> str:
    """Format a timestamp range in Beijing time."""
    return f"{format_beijing_ts(start_ms)}~{format_beijing_ts(end_ms)}"


class _ErrorCallbackHandler(logging.Handler):
    """自定义 Handler，用于在 ERROR 级别时触发回调。"""

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno < logging.ERROR:
            return
        message = self.format(record)
        for hook in list(_error_hooks):
            try:
                hook(record, message)
            except Exception:
                # 回调失败不影响主流程，记录到原始 logger
                logging.getLogger(record.name).debug(
                    "[Logger][Hook][InvokeFailed] callback=%r record=%r",
                    hook,
                    record,
                    exc_info=True,
                )

_rich_console = Console()

def setup_okx_logger(name: str = 'okx'):
    """
    设置OKX项目的统一日志配置
    
    参数:
        name: logger名称
    
    返回:
        配置好的logger实例
    """
    # 创建logs目录（基于项目根路径）
    base_dir = Path(__file__).resolve().parent.parent
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    # 创建共享 Console
    # 创建logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # 避免重复添加handler
    if logger.handlers:
        return logger

    logger.propagate = False
    
    # 创建formatter
    formatter = BeijingFormatter(
        '[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 创建文件handler - 普通日志
    info_handler = logging.FileHandler(logs_dir / 'okx.log', encoding='utf-8')
    info_handler.setLevel(logging.DEBUG)
    info_handler.setFormatter(formatter)
    
    # 创建文件handler - 错误日志
    error_handler = logging.FileHandler(logs_dir / 'okx_error.log', encoding='utf-8')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    # 回调 handler
    callback_handler = _ErrorCallbackHandler()
    callback_handler.setLevel(logging.ERROR)
    callback_handler.setFormatter(formatter)
    
    # 创建控制台handler（rich）
    console_handler = BeijingRichHandler(
        console=_rich_console,
        rich_tracebacks=True,
        show_time=True,
        show_path=False,
        markup=True,
        log_time_format="[%Y-%m-%d %H:%M:%S]",
    )
    console_handler.setLevel(logging.INFO)

    # 添加handlers
    logger.addHandler(info_handler)
    logger.addHandler(error_handler)
    logger.addHandler(callback_handler)
    logger.addHandler(console_handler)

    global _console_handler  # noqa: PLW0603
    _console_handler = console_handler

    return logger


def setup_ai_logger(name: str = "okx.ai"):
    """设置专用于 AI 回复落盘的日志器。"""
    base_dir = Path(__file__).resolve().parent.parent
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    logger.propagate = False

    formatter = BeijingFormatter(
        '[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    ai_handler = logging.FileHandler(logs_dir / 'ai.log', encoding='utf-8')
    ai_handler.setLevel(logging.INFO)
    ai_handler.setFormatter(formatter)
    logger.addHandler(ai_handler)
    return logger

# 创建全局logger实例
okx_logger = setup_okx_logger()
ai_logger = setup_ai_logger()


rich_console = _rich_console

def set_console_debug(enabled: bool) -> None:
    """根据调试开关调整控制台日志级别。"""
    if _console_handler is None:
        return
    _console_handler.setLevel(logging.DEBUG if enabled else logging.INFO)


def register_error_hook(callback: Callable[[logging.LogRecord, str], None]) -> None:
    """
    注册错误日志回调。

    回调函数会在每条 ERROR 级别日志记录后被调用，
    参数为原始 LogRecord 与格式化后的消息文本。
    """
    if not callable(callback):
        raise TypeError("callback must be callable")
    _error_hooks.append(callback)


def log_ai_reply(
    *,
    model: str,
    stream: bool,
    enable_think: bool,
    result: dict[str, Optional[str]],
) -> None:
    """将 AI 完整回复写入独立日志文件，不输出到控制台。"""
    timestamp = datetime.now(_BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    separator = "=" * 80
    think_text = result.get("think") or ""
    data_text = result.get("data") or ""

    message = "\n".join(
        [
            separator,
            f"time: {timestamp}",
            f"model: {model}",
            f"stream: {stream}",
            f"enable_think: {enable_think}",
            "[think]",
            think_text or "(empty)",
            "[data]",
            data_text or "(empty)",
            separator,
        ]
    )
    ai_logger.info(message)
