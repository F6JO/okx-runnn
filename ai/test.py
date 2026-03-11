"""
示例脚本：展示 OpenAiApi 的几种常见用法。

运行前请在环境变量或代码中填入真实的 API Key/模型参数。
"""

from openai_api import OpenAiApi


def stream_callback(text: str, is_think: bool) -> None:
    # prefix = "[THINK]" if is_think else "[DATA]"
    # print(f"{prefix} {text}")
    print(text, end="", flush=True)


def demo_stream_with_think(msg: str):
    print("=== stream + think ===")
    client = OpenAiApi(
        # base_url="https://api.deepseek.com",
        # base_url="http://127.0.0.1:5001/v1",
        base_url="https://maas-api.ai-yuanjing.com/openapi/compatible-mode/v1",
        # key="1qaz@WSX#EDC$RFV%TGB",
        key="sk-2e0fafea9f7742118f67bb63b3d0aaec",
        model="glm-5",
        enable_think=True,
        stream=True,
    )
    result = client.chat(msg, callback=stream_callback)
    print("\nresult:", result)


def demo_stream_without_think(str1):
    print("=== stream + no think ===")
    client = OpenAiApi(
        base_url="http://127.0.0.1:5001/v1",
        # base_url="https://api.deepseek.com",
        key="1qaz@WSX#EDC$RFV%TGB",
        # key="sk-c332c950b8ad4aa69145e998791ed93b",
        model="deepseek-chat",
        enable_think=False,
        stream=True,
    )
    result = client.chat(str1, callback=stream_callback)
    print("\nresult:", result)


def demo_non_stream_with_think():
    print("=== non-stream + think ===")
    client = OpenAiApi(
        # base_url="https://api.deepseek.com",
        base_url="http://127.0.0.1:5001/v1",
        key="1qaz@WSX#EDC$RFV%TGB",
        stream=False,
    )
    client.set_model_and_think("deepseek-reasoner", True)
    result = client.chat("请先思考再回答：给我一个安全提示")
    print("result:", result)


def demo_non_stream_without_think():
    print("=== non-stream + no think ===")
    client = OpenAiApi(
        base_url="http://127.0.0.1:5001/v1",
        # base_url="https://api.deepseek.com",
        key="1qaz@WSX#EDC$RFV%TGB",
        stream=False,
    )
    client.set_model_and_think("deepseek-chat", False)
    result = client.chat("列举三项人工智能应用场景")
    print("result:", result)


if __name__ == "__main__":

    wt = open("ai/test.txt","r").read()
    # 思考 + 流式
    demo_stream_with_think("你是谁，简短回答我")

    # 不思考 + 流式
    # demo_stream_without_think(wt)

    # 思考 + 非流式
    # demo_non_stream_with_think()

    # 不思考 + 非流式
    # demo_non_stream_without_think()
