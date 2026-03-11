from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

import openai
from lib.logger import log_ai_reply

Message = Dict[str, str]
Callback = Callable[[str, bool], None]


class OpenAiApi:
    """
    通用 OpenAI 兼容客户端，支持思考开关和流式模式。
    """

    THINK_OPEN = "<think>"
    THINK_CLOSE = "</think>"

    def __init__(
        self,
        base_url: str,
        key: str,
        model: Optional[str] = None,
        enable_think: Optional[bool] = None,
        stream: bool = False,
    ):
        self.base_url = base_url
        self.key = key
        self.model = model
        self.enable_think = enable_think
        self.stream = stream
        self.client = openai.OpenAI(api_key=key, base_url=base_url)

    def set_model_and_think(self, model: Optional[str], enable_think: Optional[bool]) -> None:
        self.model = model
        self.enable_think = enable_think

    def set_stream(self, stream: bool) -> None:
        self.stream = stream

    def chat(
        self,
        messages: Union[str, Sequence[Message]],
        callback: Optional[Callback] = None,
        *,
        model: Optional[str] = None,
        enable_think: Optional[bool] = None,
        stream: Optional[bool] = None,
    ) -> Dict[str, Optional[str]]:
        active_model = model if model is not None else self.model
        active_enable_think = (
            enable_think if enable_think is not None else self.enable_think
        )
        active_stream = stream if stream is not None else self.stream

        if not active_model:
            raise ValueError("model 未设置")
        if active_enable_think is None:
            raise ValueError("enable_think 未设置")
        if active_stream and callback is None:
            raise ValueError("stream 模式必须提供回调函数")

        normalized = self._normalize_messages(messages)
        if active_stream:
            result = self._chat_stream(
                normalized,
                callback,
                model=active_model,
                enable_think=bool(active_enable_think),
            )
        else:
            result = self._chat_once(
                normalized,
                model=active_model,
                enable_think=bool(active_enable_think),
            )

        log_ai_reply(
            model=active_model,
            stream=bool(active_stream),
            enable_think=bool(active_enable_think),
            result=result,
        )
        return result

    def _chat_once(
        self,
        messages: List[Message],
        *,
        model: str,
        enable_think: bool,
    ) -> Dict[str, Optional[str]]:
        response = self.client.chat.completions.create(
            model=model,
            messages=messages,
            stream=False,
        )
        message = response.choices[0].message
        content = self._to_text(getattr(message, "content", ""))
        if not enable_think:
            return {"think": None, "data": content}
        reasoning = self._to_text(getattr(message, "reasoning_content", None))
        if reasoning:
            return {"think": reasoning, "data": content}
        think, answer = self._split_think(content)
        return {"think": think, "data": answer if answer is not None else content}

    def _chat_stream(
        self,
        messages: List[Message],
        callback: Optional[Callback],
        *,
        model: str,
        enable_think: bool,
    ) -> Dict[str, Optional[str]]:
        response = self.client.chat.completions.create(
            model=model,
            messages=messages,
            stream=True,
        )

        think_parts: List[str] = []
        data_parts: List[str] = []
        in_think = False

        for chunk in response:
            content_text, reasoning_text = self._extract_chunk_parts(chunk)
            if enable_think:
                if reasoning_text:
                    think_parts.append(reasoning_text)
                    self._emit_callback(reasoning_text, True, callback)
                if content_text:
                    in_think = self._process_think_chunk(
                        content_text,
                        in_think,
                        think_parts,
                        data_parts,
                        callback,
                    )
            else:
                if content_text:
                    data_parts.append(content_text)
                    self._emit_callback(content_text, False, callback)

        think_text = "".join(think_parts) or None
        data_text = "".join(data_parts) or None
        if not enable_think:
            return {"think": None, "data": data_text}
        return {"think": think_text, "data": data_text}

    def _process_think_chunk(
        self,
        text: str,
        in_think: bool,
        think_parts: List[str],
        data_parts: List[str],
        callback: Optional[Callback],
    ) -> bool:
        remaining = text
        while remaining:
            if in_think:
                if self.THINK_CLOSE in remaining:
                    content, _, remaining = remaining.partition(self.THINK_CLOSE)
                    if content:
                        think_parts.append(content)
                        self._emit_callback(content, True, callback)
                    in_think = False
                else:
                    think_parts.append(remaining)
                    self._emit_callback(remaining, True, callback)
                    remaining = ""
            else:
                if self.THINK_OPEN in remaining:
                    content, _, remaining = remaining.partition(self.THINK_OPEN)
                    if content:
                        data_parts.append(content)
                        self._emit_callback(content, False, callback)
                    in_think = True
                else:
                    data_parts.append(remaining)
                    self._emit_callback(remaining, False, callback)
                    remaining = ""
        return in_think

    def _split_think(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        think_parts: List[str] = []
        data_parts: List[str] = []
        in_think = False
        remaining = text

        while remaining:
            if in_think:
                if self.THINK_CLOSE in remaining:
                    content, _, remaining = remaining.partition(self.THINK_CLOSE)
                    think_parts.append(content)
                    in_think = False
                else:
                    think_parts.append(remaining)
                    break
            else:
                if self.THINK_OPEN in remaining:
                    content, _, remaining = remaining.partition(self.THINK_OPEN)
                    data_parts.append(content)
                    in_think = True
                else:
                    data_parts.append(remaining)
                    break

        think_text = "".join(think_parts) or None
        data_text = "".join(data_parts) or None
        return think_text, data_text

    @staticmethod
    def _normalize_messages(
        messages: Union[str, Sequence[Message]],
    ) -> List[Message]:
        if isinstance(messages, str):
            return [{"role": "user", "content": messages}]
        return list(messages)

    @staticmethod
    def _extract_chunk_parts(chunk) -> Tuple[str, str]:
        choices = getattr(chunk, "choices", None)
        if not choices:
            return "", ""
        delta = getattr(choices[0], "delta", None)
        if not delta:
            return "", ""
        content = OpenAiApi._to_text(getattr(delta, "content", ""))
        reasoning = OpenAiApi._to_text(getattr(delta, "reasoning_content", ""))
        return content, reasoning

    @staticmethod
    def _to_text(value) -> str:
        if value is None:
            return ""
        if isinstance(value, list):
            return "".join(str(item) for item in value)
        return str(value)

    @staticmethod
    def _emit_callback(
        text: str,
        is_think: bool,
        callback: Optional[Callback],
    ) -> None:
        if not text:
            return
        if callback is None:
            raise ValueError("stream 模式必须提供回调函数")
        callback(text, is_think)
