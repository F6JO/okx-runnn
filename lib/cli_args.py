import argparse
from typing import Dict, Optional


class CliArgs:
    """命令行参数管理类，包含基础命令与子命令结构。"""

    def __init__(self, description: str = "OKX 命令行工具"):
        self._parser = argparse.ArgumentParser(description=description)
        self._parser.add_argument(
            "-c",
            "--config",
            dest="config",
            default="config.yaml",
            help="配置文件路径，默认为 config.yaml",
        )

        self._subparsers = self._parser.add_subparsers(dest="command")
        self._subparsers.required = True
        self._command_parsers: Dict[str, argparse.ArgumentParser] = {}
        self._subcommand_parsers: Dict[str, Dict[str, argparse.ArgumentParser]] = {}

        self._init_history_group()
        self._init_monitor_group()
        self._init_backtest_group()

    def _init_history_group(self) -> None:
        """初始化历史数据相关命令。"""
        history_parser = self._subparsers.add_parser(
            "history",
            help="历史数据相关功能",
        )
        self._command_parsers["history"] = history_parser

        history_subparsers = history_parser.add_subparsers(dest="history_command")
        history_subparsers.required = True
        self._subcommand_parsers["history"] = {}

        self._init_history_download(history_subparsers)
        self._init_history_check(history_subparsers)

    def _init_history_download(self, subparsers) -> None:
        parser = subparsers.add_parser(
            "dowload",
            help="下载历史数据",
        )
        parser.add_argument(
            "-s",
            "--symbol",
            dest="symbols",
            required=True,
            help="指定币种，多个请用逗号分隔，例如 btc,eth",
        )
        parser.add_argument(
            "-t",
            "--type",
            dest="market_type",
            choices=["spot", "swap"],
            required=True,
            help="交易类型，仅支持 spot 或 swap",
        )
        parser.add_argument(
            "-f",
            "--frame",
            dest="timeframes",
            required=True,
            help="K线级别，多个请用逗号分隔，如 5m,15m,1h",
        )
        parser.add_argument(
            "-i",
            "--interval",
            dest="interval",
            required=True,
            help="时间区间，例如 20240101-20251010 或 20240101-",
        )
        self._subcommand_parsers["history"]["dowload"] = parser

    def _init_history_check(self, subparsers) -> None:
        parser = subparsers.add_parser(
            "check",
            help="检查历史数据",
        )
        parser.add_argument(
            "-c",
            "--check",
            action="store_true",
            required=True,
            help="检查本地数据库数据完整性并输出详细信息",
        )
        parser.add_argument(
            "-t",
            "--time",
            dest="time_interval",
            help="限定检查的时间区间，例如 20240101-20251010 或 20240101-",
        )
        self._subcommand_parsers["history"]["check"] = parser

    def _init_monitor_group(self) -> None:
        """初始化实时监控相关命令。"""
        monitor_parser = self._subparsers.add_parser(
            "monitor",
            help="实时监控相关功能",
        )
        self._command_parsers["monitor"] = monitor_parser

        monitor_subparsers = monitor_parser.add_subparsers(dest="monitor_command")
        monitor_subparsers.required = True
        self._subcommand_parsers["monitor"] = {}

        parser = monitor_subparsers.add_parser(
            "run",
            help="运行实时监控流程",
        )
        parser.add_argument(
            "-P",
            "--pair",
            dest="pairs",
            action="append",
            required=True,
            help=(
                "定义单个监控配置，格式如 symbol=btc,type=swap,frames=5m|15m,strategies=macd|boll,interval=30(default 60)；"
                "可重复使用该参数为多个币种设置。"
            ),
        )
        self._subcommand_parsers["monitor"]["run"] = parser

    def _init_backtest_group(self) -> None:
        """初始化回测命令（无子命令，直接运行）。"""
        backtest_parser = self._subparsers.add_parser(
            "backtest",
            help="运行回测",
        )
        self._command_parsers["backtest"] = backtest_parser
        backtest_parser.add_argument(
            "-P",
            "--pair",
            dest="pairs",
            action="append",
            required=True,
            help=(
                "定义回测配置，格式如 symbol=btc,type=swap,frames=5m|15m,strategies=macd；"
                "可重复使用该参数配置多个组合。"
            ),
        )
        backtest_parser.add_argument(
            "-i",
            "--interval",
            dest="interval",
            help="回测时间区间，例如 20240101-20240401 或 20240101-",
        )

    def parse(self, args=None):
        """
        解析命令行参数。

        参数:
            args: 可选，传入自定义参数列表以便测试。

        返回:
            argparse.Namespace: 解析后的命令行参数。
        """
        return self._parser.parse_args(args)

    def get_parser(self) -> argparse.ArgumentParser:
        """返回顶层解析器。"""
        return self._parser

    def print_help(
        self,
        command: Optional[str] = None,
        subcommand: Optional[str] = None,
    ) -> None:
        """
        打印帮助信息。

        优先输出指定命令或子命令的帮助，若不存在则回退到顶层帮助。
        """
        if command is None:
            self._parser.print_help()
            return

        command_parser = self._command_parsers.get(command)
        if command_parser is None:
            self._parser.print_help()
            return

        if subcommand is None:
            command_parser.print_help()
            return

        sub_parser = self._subcommand_parsers.get(command, {}).get(subcommand)
        if sub_parser is not None:
            sub_parser.print_help()
        else:
            command_parser.print_help()
