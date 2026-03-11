from __future__ import annotations

from typing import Optional, Sequence, Union

from ai.openai_api import Callback, Message, OpenAiApi


class OpenAiChatTool:
    """
    基于 OpenAiApi 的调用模式封装。

    提供两种模式接口：
    1) think: 思考模式
    2) no_think: 非思考模式

    流式与否由 callback 是否传入决定：
    - 传 callback: 流式
    - 不传 callback: 非流式

    约定：
    - 非流式：返回完整字符串。
    - 流式：需要传入 callback，同时也返回完整字符串。
    """

    def __init__(self, client: OpenAiApi) -> None:
        if not isinstance(client, OpenAiApi):
            raise TypeError("client 必须是 OpenAiApi 实例")
        self._client = client

    def think(
        self,
        messages: Union[str, Sequence[Message]],
        callback: Optional[Callback] = None,
        *,
        model: Optional[str] = None,
    ) -> str:
        """
        思考模式。

        - 传 callback: 流式
        - 不传 callback: 非流式
        返回完整字符串（包含 think 与 data）。
        """
        stream = callback is not None
        result = self._chat(
            messages=messages,
            enable_think=True,
            stream=stream,
            callback=callback,
            model=model,
        )
        return self._compose_text(result, include_think=True)

    def no_think(
        self,
        messages: Union[str, Sequence[Message]],
        callback: Optional[Callback] = None,
        *,
        model: Optional[str] = None,
    ) -> str:
        """
        非思考模式。

        - 传 callback: 流式
        - 不传 callback: 非流式
        返回完整字符串（仅 data）。
        """
        stream = callback is not None
        result = self._chat(
            messages=messages,
            enable_think=False,
            stream=stream,
            callback=callback,
            model=model,
        )
        return self._compose_text(result, include_think=False)

    def _chat(
        self,
        *,
        messages: Union[str, Sequence[Message]],
        enable_think: bool,
        stream: bool,
        callback: Optional[Callback],
        model: Optional[str],
    ) -> dict[str, Optional[str]]:
        if callback is not None and not callable(callback):
            raise ValueError("callback 必须是可调用对象")
        if stream and callback is None:
            raise ValueError("流式模式必须提供 callback 回调函数")

        return self._client.chat(
            messages,
            callback=callback,
            model=model,
            enable_think=enable_think,
            stream=stream,
        )

    @staticmethod
    def _compose_text(
        result: dict[str, Optional[str]],
        *,
        include_think: bool,
    ) -> str:
        think_text = result.get("think") or ""
        data_text = result.get("data") or ""

        if include_think and think_text:
            return f"<think>{think_text}</think>{data_text}"
        return data_text
