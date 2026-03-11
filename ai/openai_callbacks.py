from __future__ import annotations

from typing import List

from ai.openai_api import Callback


class OpenAiCallbacks:
    """用于 OpenAiChatTool 流式回调的静态方法集合。"""

    @staticmethod
    def print_plain(text: str, is_think: bool) -> None:
        """
        直接输出增量文本，不区分 think/data。

        可直接作为 callback 传入：
        `tool.think(..., callback=OpenAiCallbacks.print_plain)`
        """
        del is_think
        print(text, end="", flush=True)

    @staticmethod
    def print_tagged(text: str, is_think: bool) -> None:
        """
        输出带标签的增量文本：
        - think 片段前缀 [THINK]
        - data  片段前缀 [DATA]
        """
        prefix = "[THINK]" if is_think else "[DATA]"
        print(f"{prefix} {text}", end="", flush=True)

    @staticmethod
    def print_data_only(text: str, is_think: bool) -> None:
        """仅输出最终回答片段（忽略 think 片段）。"""
        if is_think:
            return
        print(text, end="", flush=True)

    @staticmethod
    def print_think_only(text: str, is_think: bool) -> None:
        """仅输出 think 片段。"""
        if not is_think:
            return
        print(text, end="", flush=True)

    @staticmethod
    def noop(text: str, is_think: bool) -> None:
        """空回调：不做任何输出。"""
        del text, is_think

    @staticmethod
    def make_list_collector(
        target: List[str],
        *,
        include_think: bool = True,
    ) -> Callback:
        """
        生成一个可收集流式文本的回调函数。

        参数:
            target: 外部列表，用于收集片段。
            include_think: 是否收集 think 片段。
        """

        def _collector(text: str, is_think: bool) -> None:
            if is_think and not include_think:
                return
            target.append(text)

        return _collector

