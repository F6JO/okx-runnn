from typing import Any, Dict

_GLOBAL_STORE: Dict[str, Any] = {}


def setVar(key: str, value: Any) -> None:
    """
    将对象以引用形式保存到全局容器。

    参数:
        key: 全局键名。
        value: 任意需要共享的对象或数据。
    """
    _GLOBAL_STORE[key] = value


def getVar(key: str, default: Any = None) -> Any:
    """
    获取已保存的对象引用。

    参数:
        key: 需要获取的键名。
        default: 未找到时返回的默认值。
    """
    return _GLOBAL_STORE.get(key, default)


def hasVar(key: str) -> bool:
    """判断指定键是否存在。"""
    return key in _GLOBAL_STORE


def delVar(key: str) -> None:
    """删除指定键的全局存储。"""
    _GLOBAL_STORE.pop(key, None)
