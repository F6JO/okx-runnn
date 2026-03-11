from __future__ import annotations

import traceback
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from ai.openai_api import OpenAiApi
from lib.config import RuntimeConfig, build_runtime_config
from lib.globalVar import getVar, setVar
from lib.kline_cache import KlineCache
from lib.logger import (
    format_beijing_ts,
    okx_logger,
    register_error_hook,
    set_console_debug,
)
from lib.talkding import TalkDing
from lib.threadpool import ThreadPool
from lib.db import OkxDatabase
from okx_api.okx_account import OkxAccount
from module.historyController import HistoryController
from module.monitorController import MonitorController
from module.backtestController import BacktestController


def _error_push_hook(record, message) -> None:
    """错误通知钩子：在 ERROR 级别时推送钉钉。"""
    talk = getVar("TALK_DING")
    if talk is None:
        return

    title = f"[OKX-monitor通知][{record.name}]"
    timestamp = format_beijing_ts(int(record.created * 1000))
    level = record.levelname
    location = f"{Path(record.pathname).name}:{record.lineno}"

    quoted_message = "\n".join(
        f"> {line}" for line in (message.splitlines() or ["(无日志内容)"])
    )

    traceback_text = ""
    if record.exc_info:
        traceback_text = "".join(traceback.format_exception(*record.exc_info)).strip()

    content_lines = [
        "### :rotating_light: 错误通知",
        f"- **时间**：{timestamp}",
        f"- **级别**：`{level}`",
        f"- **模块**：`{record.name}`",
        f"- **位置**：`{location}`",
        "",
        "#### 详情",
        quoted_message,
    ]

    if traceback_text:
        content_lines.extend(
            [
                "",
                "#### Traceback",
                "```python",
                traceback_text,
                "```",
            ]
        )

    markdown_content = "\n".join(content_lines)

    pool = getVar("GLOBAL_THREAD_POOL")

    def _send() -> None:
        try:
            talk.send(markdown_content, title=title)
        except Exception:
            okx_logger.debug(
                "[错误钩子][发送失败] title=%s message=%s",
                title,
                markdown_content,
                exc_info=True,
            )

    if isinstance(pool, ThreadPool):
        pool.submit(_send)
    else:
        _send()


class MainController:
    """
    依据解析后的命令行参数，调度对应处理逻辑。

    支持注册自定义处理函数，同时内置 history 模块。
    """

    def __init__(self):
        self._handlers: Dict[str, Dict[Optional[str], Callable[[object], None]]] = {}
        self._cli_args = None
        self._history_controller = HistoryController()
        self._monitor_controller = MonitorController()
        self._monitor_controller.attach_history_controller(self._history_controller)
        self._backtest_controller = BacktestController()
        self._runtime_config: Optional[RuntimeConfig] = None

        # 预注册内置命令
        self.register("history", self._history_controller.run_download, "dowload")
        self.register("history", self._history_controller.run_check, "check")
        self.register("monitor", self._monitor_controller.run, "run")
        self.register("backtest", self._backtest_controller.run, None)

    def register(
        self,
        command: str,
        handler: Callable[[object], None],
        subcommand: Optional[str] = None,
    ) -> None:
        """
        注册命令处理函数。

        参数:
            command: 一级命令名称。
            handler: 对应的处理函数，接收 argparse.Namespace。
            subcommand: 二级子命令名称，可为空。
        """
        self._handlers.setdefault(command, {})[subcommand] = handler

    def run(self, args, cli_args) -> None:
        """
        根据解析后的参数执行对应命令。

        若未匹配到处理器，则输出帮助信息。
        """
        self._cli_args = cli_args
        self._history_controller.attach_cli_args(cli_args)
        self._monitor_controller.attach_cli_args(cli_args)
        self._backtest_controller.attach_cli_args(cli_args)

        command = getattr(args, "command", None)
        cmd_handlers = self._handlers.get(command, {})
        subcommand_attr = f"{command}_command"
        subcommand = getattr(args, subcommand_attr, None)
        handler = cmd_handlers.get(subcommand)
        if handler is None:
            return

        if command == "history":
            if subcommand == "dowload":
                if not self._history_controller.prepare_download_args(args):
                    return
            elif subcommand == "check":
                if not self._history_controller.prepare_check_args(args):
                    return

        config_path = getattr(args, "config", "config.yaml")
        self._runtime_config = build_runtime_config(config_path)
        self._initialize_runtime_environment()

        handler(args)

    # ------------------------------------------------------------------
    # 初始化运行环境
    # ------------------------------------------------------------------

    def _initialize_runtime_environment(self) -> None:
        if self._runtime_config is None:
            raise RuntimeError("runtime config 尚未加载")

        runtime_config = self._runtime_config

        set_console_debug(runtime_config.debug_mode)
        setVar("DEBUG_MODE", runtime_config.debug_mode)
        okx_logger.info(
            "[命令行][环境配置][调试模式] DEBUG=%s",
            runtime_config.debug_mode,
        )

        thread_settings = runtime_config.thread_pool
        if thread_settings.error_message:
            okx_logger.error(thread_settings.error_message)
        thread_pool = ThreadPool(
            thread_settings.workers,
            thread_name_prefix="okx-global",
        )
        setVar("GLOBAL_THREAD_POOL", thread_pool)
        okx_logger.debug(
            "[命令行][环境配置][线程池初始化] workers=%s prefix=%s",
            thread_settings.workers,
            thread_pool.thread_name_prefix,
        )

        if runtime_config.talk_ding_token:
            talk = TalkDing(runtime_config.talk_ding_token)
            setVar("TALK_DING", talk)
            register_error_hook(_error_push_hook)
            okx_logger.info("[命令行][环境配置][钉钉token] TALK_DING_TOKEN=True")
        else:
            okx_logger.info("[命令行][环境配置][钉钉token] TALK_DING_TOKEN=False")

        okx_logger.info(
            "[命令行][环境配置][数据目录] 路径=%s",
            runtime_config.data_dir,
        )

        database = OkxDatabase()
        setVar("OKX_DATABASE", database)

        kline_cache = KlineCache(database=database)
        setVar("KLINE_CACHE", kline_cache)

        self._initialize_ai_clients(runtime_config.ai_config)

        account = self._create_account()
        setVar("OKX_ACCOUNT", account)

    def _create_account(self) -> Optional[OkxAccount]:
        if self._runtime_config is None:
            return None

        creds = self._runtime_config.credentials
        api_key = creds.api_key
        api_secret = creds.api_secret
        password = creds.api_password
        proxy = (
            getVar("HTTPS_PROXY")
            or getVar("HTTP_PROXY")
            or getVar("SOCKS5_PROXY")
        )

        if not all([api_key, api_secret, password]):
            okx_logger.info("未检测到完整的 OKX API 配置，将以只解析模式运行。")
            okx_logger.debug(
                "[账户][创建][缺失凭证] api_key=%s api_secret=%s password=%s",
                "已配置" if api_key else "缺失",
                "已配置" if api_secret else "缺失",
                "已配置" if password else "缺失",
            )
            return None

        account = OkxAccount(
            api_key=api_key,
            api_secret=api_secret,
            password=password,
            market_type="spot",
            proxy=proxy,
            testnet=False,
            enable_websocket=True,
        )
        okx_logger.debug(
            "[账户][创建][成功] market_type=%s proxy=%s testnet=%s",
            account.market_type,
            proxy,
            False,
        )
        return account

    def _initialize_ai_clients(self, ai_config: Dict[str, Dict[str, Any]]) -> None:
        if not ai_config:
            setVar("AI_CLIENTS", {})
            okx_logger.info("[命令行][环境配置][AI] 未配置 AI 客户端")
            return

        clients: Dict[str, OpenAiApi] = {}
        for provider, cfg in ai_config.items():
            try:
                client = OpenAiApi(
                    base_url=cfg["base_url"],
                    key=cfg["api_key"],
                    model=cfg.get("model"),
                    enable_think=cfg.get("enable_think"),
                    stream=cfg.get("stream") or False,
                )
            except Exception:
                okx_logger.error(
                    "[命令行][环境配置][AI] 初始化失败 provider=%s",
                    provider,
                    exc_info=True,
                )
                continue
            clients[provider] = client
            okx_logger.info("[命令行][环境配置][AI] 初始化成功 provider=%s", provider)

        setVar("AI_CLIENTS", clients)
