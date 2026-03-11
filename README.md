# OKX Run

这是一个**我自己使用的 OKX 量化/监控项目**。  
它不是面向外部用户打包发布的产品，而是一个偏个人化、可持续迭代的交易研究与执行框架。

项目目标很明确：

1. 管理 OKX 行情与历史 K 线
2. 在本地缓存与数据库基础上运行策略
3. 支持多币种、多级别实时监控
4. 支持 AI 驱动策略与规则型策略并存
5. 把通知、日志、数据落库都收敛到同一个 CLI 工具里

## 1. 项目定位

这个项目更像是一个“自用交易工作台”，不是通用 SDK，也不是完整商业系统。

因此代码风格和功能设计有几个明显特点：

1. 偏向本地单机运行
2. 偏向 OKX 单一交易所
3. 偏向我自己的监控与策略工作流
4. 配置、目录结构、日志方式都优先服务当前使用场景
5. AI 策略是项目中的重要一环

## 2. 当前主要能力

### 2.1 历史数据

- 支持按币种、市场、周期下载历史 K 线
- 会落到本地 SQLite 数据库
- 可检查本地历史是否连续、有无缺口
- `monitor run` 启动时会自动按默认配置预热历史数据

### 2.2 实时监控

- 通过 WebSocket 订阅 OKX K 线
- 监控线程按设定 `interval` 调度策略
- 每个策略任务提交到全局线程池执行
- 已收盘 K 线会自动落库并写入缓存

### 2.3 策略执行

- 支持同一命令中配置多个币种、多个级别、多个策略
- 支持规则型策略
- 支持 AI 驱动策略
- 策略统一放在 `strateg/` 目录，以目录包形式组织

### 2.4 AI 调用

- 通过 `config.yaml` 初始化多个 AI provider
- 当前项目中已有 OpenAI 风格接口封装
- 支持思考/非思考、流式/非流式
- AI 回复单独写入 `logs/ai.log`

### 2.5 通知与日志

- 常规日志：`logs/okx.log`
- 错误日志：`logs/okx_error.log`
- AI 回复日志：`logs/ai.log`
- 支持钉钉消息通知

## 3. 目录说明

### 3.1 入口与配置

- [main.py](main.py)  
项目 CLI 入口

- [config.yaml](config.yaml)  
本地配置文件，包含 OKX、代理、线程池、AI、监控历史天数等配置

### 3.2 通用基础层

- `lib/`
  - CLI 参数定义
  - 配置加载
  - 数据库封装
  - K 线缓存
  - 日志
  - 全局变量
  - 线程池
  - 钉钉发送

### 3.3 控制器层

- `module/`
  - `mainController.py`：总控入口
  - `monitorController.py`：实时监控与策略调度
  - `historyController.py`：历史数据下载与检查
  - `backtestController.py`：回测占位控制器（功能未完成）
  - `strategResuController.py`：策略结果处理

### 3.4 OKX 接口层

- `okx_api/`
  - `okx_account.py`：账户、REST、WebSocket 基础封装
  - `okx_coin.py`：按交易对聚合的门面类
  - `okx_market_watch.py`：实时订阅与监控
  - `okx_history_data.py`：历史数据抓取与落库
  - `okx_price_snapshot.py`：行情快照
  - `okx_trading_status.py`：订单与持仓状态

### 3.5 AI 层

- `ai/`
  - `openai_api.py`：底层 OpenAI 风格接口封装
  - `openai_chat_tool.py`：更易直接给策略使用的聊天工具封装
  - `openai_callbacks.py`：流式输出回调工具

### 3.6 策略层

- `strateg/`
  - 每个策略一个目录
  - 入口文件固定 `main.py`
  - 入口类固定 `strateg`
  - 详细规范见：
    - [strateg/README.md](strateg/README.md)

## 4. 当前策略架构

当前策略不是旧版单文件模式，而是包目录模式：

```text
strateg/
  ai_mysmc/
    main.py
  strategy_template/
    main.py
  macd_death_cross/
    main.py
```

动态加载规则固定为：

```python
strateg.<strategy_name>.main
```

并取类：

```python
strateg
```

例如：

- `strategies=ai_mysmc`
- 对应 `strateg/ai_mysmc/main.py`
- 入口类为 `strateg`

## 5. 运行流程概览

以监控命令为例：

```bash
python3 main.py monitor run -P "symbol=btc,type=swap,frames=15m|1h,strategies=ai_mysmc,interval=900"
```

大致流程是：

1. 解析监控配置
2. 初始化 OKX、数据库、缓存、AI 客户端、线程池
3. 预热历史数据
4. 预热 `KLINE_CACHE`
5. 建立 WebSocket K 线订阅
6. 主监控循环按 `interval` 调度
7. 调度时把策略任务扔进线程池
8. K 线收盘后自动落库并同步缓存
9. 策略结果交由结果控制器处理

## 6. 常用命令

### 6.1 实时监控

```bash
python3 main.py monitor run -P "symbol=btc,type=swap,frames=15m|1h,strategies=ai_mysmc,interval=900"
```

说明：

- `symbol` 支持用 `|` 分隔多个币种
- `frames` 支持多级别
- `strategies` 支持多个策略
- `interval` 单位是秒

### 6.2 下载历史

注意：当前命令名在代码里是 `dowload`，不是 `download`。

```bash
python3 main.py history dowload -s btc,eth -t swap -f 15m,1h -i 20250101-
```

### 6.3 检查历史完整性

```bash
python3 main.py history check -c
```

### 6.4 回测

当前 `backtest` 命令对应的控制器仍是未完成状态。

目前代码里已有：

1. 参数解析
2. 时间区间解析
3. 历史数据补齐与覆盖检查

但还没有真正完成：

1. 策略逐K回放
2. 订单撮合
3. 仓位变化
4. 收益统计
5. 回测结果输出

因此当前 README 不再提供回测使用示例，避免误导为“已经可直接使用”。

## 7. 配置项概览

核心配置在 [config.yaml](config.yaml)：

```yaml
okx:
  api_key: ...
  api_secret: ...
  api_password: ...

talk_ding_token: ...

data_dir: data/
debug: false
thread_workers: 10
api_max_retries: 10
proxy: "socks5://127.0.0.1:7890"

# 对应k线级别默认拉取的天数
monitor:
  default_history_days:
    1m: 60
    5m: 120
    15m: 180
    30m: 240
    1h: 365
    2h: 540
    4h: 720
    6h: 900
    12h: 1095
    1d: 1460
    1w: 1825

# 初始化后放在全局的AI_CLIENTS中，以字典格式，值为OpenAiApi对象
ai: 
  deepseek:
    base_url: http://127.0.0.1:5001/v1
    api_key: ...
    model: deepseek-reasoner-search
    enable_think: true
```

重点说明：

1. `thread_workers`
- 控制全局线程池大小
- 策略调度使用这个线程池

2. `monitor.default_history_days`
- 决定 `monitor run` 启动时各周期默认补多少历史

3. `proxy`
- 同时影响 REST / WebSocket
- AI 是否走代理取决于 AI client 初始化方式和目标地址

4. `ai`
- 初始化后会放到全局变量 `AI_CLIENTS`
- 策略可以通过 `getVar("AI_CLIENTS")` 获取

## 8. 数据与缓存

### 8.1 SQLite 历史库

历史 K 线会按币种 / 市场 / 周期写入本地数据库。

### 8.2 `KLINE_CACHE`

监控与策略读取历史窗口时优先用它。

### 8.3 `LIVE_CANDLES`

保存实时订阅收到的最新 K 线，包含可能未收盘的当前 K 线。

### 8.4 重要区别

- `KLINE_CACHE` 更适合拿历史窗口
- `LIVE_CANDLES` / `latest` 更适合拿当前最新一根
- 如果策略需要“历史窗口 + 当前未收盘”，要自己显式拼接

## 9. AI 相关约定

当前项目里，AI 在策略中主要用于：

1. 把结构化 K 线数据喂给模型
2. 让模型给出方向、止盈、止损、原因
3. 解析 JSON 或半结构化文本结果
4. 再决定是否通知 / 入场

相关文件：

- [ai/openai_api.py](ai/openai_api.py)
- [ai/openai_chat_tool.py](ai/openai_chat_tool.py)
- [ai/openai_callbacks.py](ai/openai_callbacks.py)
- [strateg/ai_mysmc/main.py](strateg/ai_mysmc/main.py)

## 10. 关于“这是我自己使用的项目”

这点单独写清楚：

1. 这个仓库首先是给我自己跑实盘监控、做研究、写策略用的
2. 文档和结构说明也优先服务我自己的工作流
3. 代码会持续按我的使用习惯演化，不追求对外通用性
4. AI 生成代码时也应默认这是一个“自用、本地、单人维护”的项目

换句话说，这个项目不是“给任何人开箱即用”的，而是一个围绕我自己交易流程搭建的工具集合。

## 11. 后续维护建议

1. 新策略优先从 [strateg/strategy_template/main.py](strateg/strategy_template/main.py) 复制
2. AI 生成策略前，先让它读取 [strateg/README.md](strateg/README.md)
3. 若修改了策略加载规则，优先同步更新 `strateg/README.md`
4. 若修改了 CLI 或项目结构，再同步更新本 README
