# 策略开发说明

本文件用于约束 `strateg/` 目录下策略的目录结构、加载方式、数据使用方式与编写规范。  
这份说明按当前项目真实架构编写，供你本人后续维护，也供 AI 读取后生成新策略时使用。

## 1. 当前策略架构

当前不是旧版的“一个策略一个 `.py` 文件”结构，而是：

```text
strateg/
  strategy_template/
    main.py
  ai_mysmc/
    main.py
    mysmc.md
  macd_death_cross/
    main.py
  ...
```

必须遵守的约定：

1. 策略名就是目录名  
例如：`ai_mysmc`

2. 入口文件固定为 `main.py`

3. 入口类名固定为 `strateg`

4. 控制器动态加载规则固定为：

```python
module_name = f"strateg.{strategy_name}.main"
strategy_cls = getattr(module, "strateg")
```

也就是说：

- `strategies=ai_mysmc`
- 会加载 `strateg/ai_mysmc/main.py`
- 取类 `strateg`

旧版“类名与文件名同名”的写法已经不是当前架构，不要再按旧规则生成。

## 2. 监控如何触发策略

监控命令示例：

```bash
python3 main.py monitor run -P "symbol=btc,type=swap,frames=15m|1h,strategies=ai_mysmc,interval=900"
```

含义：

1. 监控 `btc`
2. 市场为 `swap`
3. 同时订阅 `15m` 和 `1h`
4. 每 900 秒触发一次策略调度
5. 策略名为 `ai_mysmc`

当前运行模型：

1. 启动时先做历史预热
2. 然后预热 `KLINE_CACHE`
3. 再发起 WebSocket K 线订阅
4. 监控循环按 `interval` 调度
5. 每次调度时把策略任务提交到全局线程池

结论：

- 主线程不会等待单个策略执行完成
- 同一策略可能在不同轮次重叠执行
- 策略代码要尽量幂等，避免依赖可变全局状态

线程池大小来自 `config.yaml` 中的：

```yaml
thread_workers: 10
```

## 3. 策略实例化契约

控制器会按固定关键字参数实例化策略：

```python
instance = strategy_cls(
    symbol=symbol,
    market_type=market_type,
    timeframe=timeframe,
    latest=latest,
)
```

推荐构造函数：

```python
def __init__(
    self,
    *,
    symbol: str,
    market_type: str,
    timeframe: str,
    latest: Dict[str, float],
) -> None:
    ...
```

约定字段含义：

- `symbol`: 统一格式，例如 `BTC/USDT:USDT`
- `market_type`: 如 `swap` / `spot`
- `timeframe`: 当前策略实例所属级别，例如 `15m`
- `latest`: 当前触发时刻的最新 K 线快照字典

## 4. `start()` 入口约定

每个策略必须实现：

```python
def start(self) -> bool:
    ...
```

返回值约定：

- `True`: 本轮策略通过 / 主动跳过但不视为失败
- `False`: 本轮不通过，或执行失败

建议：

1. 明确返回 `True/False`
2. 不要返回复杂对象
3. 策略内部自行捕获异常并记录日志
4. 不要把异常直接抛给框架

## 5. 多级别运行与“只在某一级别执行”

如果命令中写了：

```text
frames=15m|1h
```

那么框架会：

1. 同时预热 `15m` 和 `1h`
2. 同时订阅 `15m` 和 `1h`
3. 同时为 `15m` 和 `1h` 各建立一个策略上下文

也就是说，策略会分别在两个级别被调度一次。

如果你只想在某个级别执行核心逻辑，推荐在策略文件顶部定义守卫：

```python
RUN_ONLY_TIMEFRAME = "15m"
```

并在 `start()` 开头这样写：

```python
if self._timeframe != RUN_ONLY_TIMEFRAME:
    okx_logger.debug(
        "[策略][my_strategy][跳过] 仅%s执行，当前=%s",
        RUN_ONLY_TIMEFRAME,
        self._timeframe,
    )
    return True
```

说明：

- `return True` 表示“主动跳过，不算失败”
- 这也是当前 `ai_mysmc` 采用的思路

## 6. 策略可拿到的数据

### 6.1 `self._latest`

`latest` 是框架注入的最新快照，通常包含：

- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `volume_contract`
- `volume_currency`
- `volume_quote`
- `confirm`

注意：

- `self._latest` 可能是未收盘 K 线
- `confirm == 0` 表示未收盘
- `confirm == 1` 表示已收盘

### 6.2 `KLINE_CACHE`

策略访问历史数据的主入口是全局缓存：

```python
from lib.globalVar import getVar

cache = getVar("KLINE_CACHE")
candles = cache.get_recent(symbol, market_type, timeframe, limit)
```

这也是模板策略内 `_get_recent_candles()` 的底层来源。

### 6.3 `LIVE_CANDLES`

如果策略需要拿“当前未收盘”的实时 K 线，可以从：

```python
getVar("LIVE_CANDLES")
```

里按如下 key 读取：

```python
(symbol, market_type, timeframe)
```

当前 `ai_mysmc` 的 `_get_recent_candles_with_latest()` 就是：

1. 先从 `KLINE_CACHE` 读取历史
2. 再把 `self._latest` 或 `LIVE_CANDLES` 中对应级别的最新 K 线拼进去

## 7. `_get_recent_candles()` 与“最新未收盘 K 线”

当前模板里的 `_get_recent_candles()` 行为是：

1. 只从 `KLINE_CACHE` 读取
2. 默认返回缓存中的历史窗口
3. 不负责自动拼接未收盘实时 K 线

所以：

- 如果你只调用 `_get_recent_candles()`，通常拿到的是缓存中的历史数据
- 如果想把当前未收盘 K 线也纳入分析，需要自行拼接

推荐做法有两种：

1. 当前级别：把 `self._latest` 拼进去
2. 其他级别：从 `LIVE_CANDLES[(symbol, market_type, timeframe)]` 拼进去

前提：

- 那个级别必须在 `monitor run` 的 `frames=` 中被同时订阅

例如你在 `15m` 策略里想读取包含最新未收盘的 `1h`：

```text
frames=15m|1h
```

否则框架没有 `1h` 的实时订阅，`LIVE_CANDLES` 里也不会有对应数据。

## 8. K 线数据格式

当前策略里读到的 K 线数据是：

```python
List[Dict[str, Any]]
```

不是二维数组。

单根 K 线字典典型结构：

```python
{
    "timestamp": 1762336500000,
    "open": 123.4,
    "high": 125.6,
    "low": 122.8,
    "close": 124.9,
    "volume": 100.0,
    "volume_contract": 100.0,
    "volume_currency": 100.0,
    "volume_quote": 12490.0,
    "confirm": 1,
}
```

其中：

- `timestamp` 默认是毫秒时间戳
- 数据库存储虽然是日期字符串，但读入缓存后会统一转换为毫秒时间戳

## 9. 如果要转成 AI 友好的输入

当前模板和 `ai_mysmc` 已经给出两类常用辅助方法。

### 9.1 时间戳转可读时间

```python
formatted = self._format_candle_timestamps(candles)
```

效果：

- 把 `timestamp` 从毫秒时间戳转成北京时间字符串
- 格式：`YYYY-MM-DD HH:MM:SS`

### 9.2 转成二维数组并移除 `confirm`

```python
matrix = self._candles_to_matrix_without_confirm(formatted)
```

效果：

1. 第一行是表头
2. 后续每行是一根 K 线
3. 自动删除 `confirm`

输出形态类似：

```python
[
    ["timestamp", "open", "high", "low", "close", "volume", "volume_contract", "volume_currency", "volume_quote"],
    ["2026-03-11 12:00:00", 1900, 1910, 1895, 1908, 123, 123, 123, 234684],
]
```

这类结构比较适合直接拼进 AI 提示词。

## 10. 日志规范

策略内统一使用：

```python
from lib.logger import okx_logger
```

不要用 `print()`。

推荐日志前缀：

```text
[策略][策略名][阶段]
```

例如：

```python
okx_logger.debug("[策略][ai_mysmc][跳过] 当前级别=%s", self._timeframe)
okx_logger.warning("[策略][ai_mysmc][AI结果] JSON解析失败")
okx_logger.info("[策略][ai_mysmc][通知发送成功] symbol=%s", self._symbol)
```

## 11. AI 调用规范

如果策略内部会调用 AI：

1. 推荐使用 `ai/openai_chat_tool.py`
2. 默认不要在策略里直接管理底层 SDK
3. 流式输出如果需要回调，可使用 `ai/openai_callbacks.py`
4. AI 的最终回复会自动记录到 `logs/ai.log`
5. 不会写入 `logs/okx.log` 和 `logs/okx_error.log`

注意：

- 当前项目策略是线程池并发执行的
- 同一时刻可能有多个策略线程同时请求 AI
- 因此不要在策略里自己维护共享的可变 AI 状态

## 12. 编写新策略时的推荐骨架

优先复制：

- [strategy_template/main.py](strategy_template/main.py)

再按以下步骤改：

1. 复制目录并改目录名
2. 保留 `main.py`
3. 保留类名 `strateg`
4. 改 `RUN_ONLY_TIMEFRAME`
5. 实现自己的 `start()`
6. 视需要保留或删减辅助方法

## 13. 一个最小可运行示例

```python
from __future__ import annotations

from typing import Dict, List

from lib.globalVar import getVar
from lib.logger import okx_logger

RUN_ONLY_TIMEFRAME = "15m"


class strateg:
    def __init__(
        self,
        *,
        symbol: str,
        market_type: str,
        timeframe: str,
        latest: Dict[str, float],
    ) -> None:
        self._symbol = symbol
        self._market_type = market_type
        self._timeframe = timeframe
        self._latest = dict(latest)

    def start(self) -> bool:
        if self._timeframe != RUN_ONLY_TIMEFRAME:
            return True

        candles = self._get_recent_candles(limit=200)
        if len(candles) < 50:
            okx_logger.warning(
                "[策略][my_strategy] 样本不足 symbol=%s frame=%s size=%d",
                self._symbol,
                self._timeframe,
                len(candles),
            )
            return False

        latest_close = float(candles[-1].get("close", 0.0))
        prev_close = float(candles[-2].get("close", 0.0))
        return latest_close > prev_close

    def _get_recent_candles(
        self,
        limit: int,
        timeframe: str | None = None,
    ) -> List[Dict[str, float]]:
        target_timeframe = (timeframe or self._timeframe).strip() or self._timeframe
        cache = getVar("KLINE_CACHE")
        if cache is None:
            return []
        rows = cache.get_recent(
            self._symbol,
            self._market_type,
            target_timeframe,
            limit,
        )
        return [item for item in rows if isinstance(item, dict)]
```

## 14. AI 生成策略时必须遵守的清单

1. 目录名必须等于策略名
2. 文件名必须为 `main.py`
3. 类名必须为 `strateg`
4. 必须实现 `start(self) -> bool`
5. 必须兼容 `symbol / market_type / timeframe / latest`
6. 默认使用 `okx_logger`，不要用 `print`
7. 读取历史 K 线优先走 `KLINE_CACHE`
8. 如果需要实时未收盘 K 线，显式使用 `self._latest` 或 `LIVE_CANDLES`
9. 不要在策略中自行开线程、开事件循环、做复杂全局状态管理
10. AI 调用返回结果必须自己做解析、兜底和日志记录

## 15. 当前目录中的参考策略

1. `strategy_template`
- 推荐作为新策略起点
- 说明更完整，注释更多

2. `ai_mysmc`
- 是一个偏业务化、偏 AI 驱动的实战策略样例
- 包含：
  - 多级别取数
  - 实时 K 线拼接
  - 时间格式化
  - 二维数组构造
  - AI 响应解析
  - 钉钉通知

3. `macd_death_cross` / `macd_golden_cross` / `volume_macd_cross`
- 是偏指标型、规则型的简单样例

如果你要让 AI 生成新策略，最推荐它参考：

1. [strategy_template/main.py](strategy_template/main.py)
2. [ai_mysmc/main.py](ai_mysmc/main.py)
