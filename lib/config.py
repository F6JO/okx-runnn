from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import yaml

from lib.globalVar import setVar
from lib.logger import okx_logger

PROXY_SCHEME_ENV_MAPPING = {
    "http": "HTTP_PROXY",
    "https": "HTTPS_PROXY",
    "socks5": "SOCKS5_PROXY",
    "socks5h": "SOCKS5_PROXY",
}

@dataclass(frozen=True)
class ThreadPoolSettings:
    workers: int
    error_message: Optional[str] = None


@dataclass(frozen=True)
class ProxySettings:
    raw: Optional[str]
    env_var: Optional[str]
    value: Optional[str]
    scheme: Optional[str]
    error_message: Optional[str] = None


@dataclass(frozen=True)
class ApiCredentials:
    api_key: Optional[str]
    api_secret: Optional[str]
    api_password: Optional[str]


@dataclass(frozen=True)
class RuntimeConfig:
    config_path: str
    data_dir: str
    debug_mode: bool
    thread_pool: ThreadPoolSettings
    proxy: ProxySettings
    talk_ding_token: Optional[str]
    api_max_retries: int
    credentials: ApiCredentials
    monitor_history_days: Dict[str, int]
    ai_config: Dict[str, Dict[str, Any]]
    raw_config: Dict[str, Any]


def _load_raw_config(config_path: str) -> tuple[Dict[str, Any], Path]:
    resolved = Path(config_path or "config.yaml").expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"未找到配置文件: {resolved}")

    with resolved.open("r", encoding="utf-8") as handle:
        content = yaml.safe_load(handle) or {}

    if not isinstance(content, dict):
        raise TypeError(f"配置文件 {resolved} 顶层必须为对象/dict")

    okx_logger.info("[配置][加载][完成] 文件=%s", resolved)
    return content, resolved


def _coerce_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_bool(value: Any) -> bool:
    if value is None:
        raise ValueError("缺少布尔配置")
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"非法布尔值: {value}")


def _coerce_int(value: Any, minimum: int = 1) -> int:
    if value is None:
        raise ValueError("缺少整数配置")
    try:
        num = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"非法整数值: {value}")

    if num < minimum:
        raise ValueError(f"数值 {num} 小于允许的最小值 {minimum}")
    return num


def _resolve_monitor_history(raw_config: Dict[str, Any]) -> Dict[str, int]:
    history = None
    monitor_section = raw_config.get("monitor")
    if isinstance(monitor_section, dict):
        history = monitor_section.get("default_history_days")
    if history is None:
        history = raw_config.get("monitor_default_history_days")
    if history is None:
        history = raw_config.get("MONITOR_DEFAULT_HISTORY_DAYS")

    if history is None:
        raise ValueError("监控历史配置缺失，请在 config.yaml 中设置 monitor.default_history_days")
    if not isinstance(history, dict):
        raise TypeError(
            f"监控历史配置必须为字典，当前类型={type(history)}"
        )

    normalized: Dict[str, int] = {}
    for key, value in history.items():
        try:
            normalized[str(key)] = int(value)
        except (TypeError, ValueError):
            raise ValueError(f"监控历史配置项无法解析为整数 key={key} value={value!r}")

    if not normalized:
        raise ValueError("监控历史配置项为空，请至少配置一个时间周期")
    return normalized


def _build_proxy_settings(raw_config: Dict[str, Any]) -> ProxySettings:
    proxy_value: Optional[str] = None
    proxy_section = raw_config.get("proxy")
    if isinstance(proxy_section, dict):
        proxy_value = _coerce_optional_str(
            proxy_section.get("value")
            or proxy_section.get("url")
            or proxy_section.get("address")
        )
    else:
        proxy_value = _coerce_optional_str(proxy_section)

    if proxy_value is None:
        proxy_value = _coerce_optional_str(raw_config.get("PROXY"))

    env_var: Optional[str] = None
    scheme: Optional[str] = None
    error_message: Optional[str] = None
    if proxy_value:
        parsed = urlparse(proxy_value)
        scheme = (parsed.scheme or "").lower()
        env_var = PROXY_SCHEME_ENV_MAPPING.get(scheme)
        if env_var is None:
            error_message = (
                f"[配置][代理] 不支持的协议 {scheme or '未指定'}，仅支持 http/https/socks5"
            )

    return ProxySettings(
        raw=proxy_value,
        env_var=env_var,
        value=proxy_value if env_var else None,
        scheme=scheme,
        error_message=error_message,
    )


def _build_credentials(raw_config: Dict[str, Any]) -> ApiCredentials:
    api_section = raw_config.get("okx")
    if isinstance(api_section, dict):
        # 支持 okx.credentials 或 okx.api
        if "credentials" in api_section and isinstance(api_section["credentials"], dict):
            api_section = api_section["credentials"]
    elif raw_config.get("OKX") and isinstance(raw_config["OKX"], dict):
        api_section = raw_config["OKX"]
    else:
        api_section = {}

    api_key = _coerce_optional_str(
        api_section.get("api_key")
        or raw_config.get("okx_api_key")
        or raw_config.get("OKX_API_KEY")
        or os.getenv("OKX_API_KEY")
    )
    api_secret = _coerce_optional_str(
        api_section.get("api_secret")
        or raw_config.get("okx_api_secret")
        or raw_config.get("OKX_API_SECRET")
        or os.getenv("OKX_API_SECRET")
    )
    api_password = _coerce_optional_str(
        api_section.get("api_password")
        or raw_config.get("okx_api_password")
        or raw_config.get("OKX_API_PASSWORD")
        or os.getenv("OKX_API_PASSWORD")
    )

    return ApiCredentials(
        api_key=api_key,
        api_secret=api_secret,
        api_password=api_password,
    )


def _build_ai_config(raw_config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    ai_section = raw_config.get("ai")
    if ai_section is None:
        ai_section = raw_config.get("AI")
    if not isinstance(ai_section, dict) or not ai_section:
        raise ValueError("缺少 ai 配置或配置格式错误")

    normalized: Dict[str, Dict[str, Any]] = {}
    for provider, settings in ai_section.items():
        if not isinstance(settings, dict):
            raise TypeError(f"AI 配置项必须为对象 provider={provider}")

        base_url = _coerce_optional_str(settings.get("base_url"))
        api_key = _coerce_optional_str(settings.get("api_key"))
        if not base_url or not api_key:
            raise ValueError(f"AI 配置缺少 base_url 或 api_key provider={provider}")

        model = _coerce_optional_str(settings.get("model"))

        enable_think = settings.get("enable_think")
        if enable_think is not None:
            enable_think = _coerce_bool(enable_think)

        stream = settings.get("stream")
        if stream is not None:
            stream = _coerce_bool(stream)

        normalized[provider] = {
            "base_url": base_url,
            "api_key": api_key,
            "model": model,
            "enable_think": enable_think,
            "stream": stream,
        }

    if not normalized:
        raise ValueError("AI 配置为空")
    return normalized


def _apply_proxy_globals(proxy_settings: ProxySettings) -> None:
    for env_key in ("HTTP_PROXY", "HTTPS_PROXY", "SOCKS5_PROXY"):
        setVar(env_key, None)
    if proxy_settings.env_var and proxy_settings.value:
        setVar(proxy_settings.env_var, proxy_settings.value)
        okx_logger.info(
            "[配置][代理] %s=%s",
            proxy_settings.env_var,
            proxy_settings.value,
        )
    elif proxy_settings.error_message:
        okx_logger.error(proxy_settings.error_message)
    else:
        okx_logger.info("[配置][代理] 未启用代理")


def build_runtime_config(config_path: str) -> RuntimeConfig:
    raw_config, resolved_path = _load_raw_config(config_path)

    data_dir_value = raw_config.get("data_dir") or raw_config.get("DATA_DIR")
    if data_dir_value is None:
        raise ValueError("缺少 data_dir 配置")
    data_dir = _coerce_optional_str(data_dir_value)
    if not data_dir:
        raise ValueError("data_dir 配置不能为空字符串")

    debug_raw = raw_config.get("debug")
    if debug_raw is None:
        debug_raw = raw_config.get("DEBUG")
    if debug_raw is None:
        raise ValueError("缺少 debug 配置")
    debug_mode = _coerce_bool(debug_raw)

    thread_raw = raw_config.get("thread_workers") or raw_config.get("THREAD_WORKERS")
    if thread_raw is None:
        raise ValueError("缺少 thread_workers 配置")
    thread_workers = _coerce_int(thread_raw, minimum=1)
    thread_pool_settings = ThreadPoolSettings(workers=thread_workers)

    proxy_settings = _build_proxy_settings(raw_config)

    talk_token = _coerce_optional_str(
        raw_config.get("talk_ding_token")
        or raw_config.get("TALK_DING_TOKEN")
    )

    retries_raw = raw_config.get("api_max_retries") or raw_config.get("API_MAX_RETRIES")
    if retries_raw is None:
        raise ValueError("缺少 api_max_retries 配置")
    api_retries = _coerce_int(retries_raw, minimum=1)

    monitor_history_days = _resolve_monitor_history(raw_config)
    credentials = _build_credentials(raw_config)
    ai_config = _build_ai_config(raw_config)

    # 将配置写入全局变量，便于其他模块直接读取
    setVar("CONFIG_FILE", str(resolved_path))
    setVar("CONFIG_VALUES", raw_config)
    setVar("DATA_DIR", data_dir)
    setVar("DEBUG_MODE", debug_mode)
    setVar("THREAD_WORKERS", thread_workers)
    setVar("API_MAX_RETRIES", api_retries)
    setVar("MONITOR_DEFAULT_HISTORY_DAYS", monitor_history_days)
    setVar(
        "OKX_CREDENTIALS",
        {
            "api_key": credentials.api_key,
            "api_secret": credentials.api_secret,
            "api_password": credentials.api_password,
        },
    )
    setVar("TALK_DING_TOKEN", talk_token)
    setVar("AI_CONFIGS", ai_config)

    _apply_proxy_globals(proxy_settings)

    okx_logger.info("[配置][数据目录] %s", data_dir)
    okx_logger.info("[配置][调试模式] DEBUG=%s", debug_mode)
    return RuntimeConfig(
        config_path=str(resolved_path),
        data_dir=data_dir,
        debug_mode=debug_mode,
        thread_pool=thread_pool_settings,
        proxy=proxy_settings,
        talk_ding_token=talk_token,
        api_max_retries=api_retries,
        credentials=credentials,
        monitor_history_days=monitor_history_days,
        ai_config=ai_config,
        raw_config=raw_config,
    )
