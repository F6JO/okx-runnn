<div align="left"> <img src="https://komarev.com/ghpvc/?username=okx-run" /> </div>

# OKX Run

这是一个**我自己使用的 OKX 监控与策略运行项目**。

主要用来做几件事：

1. 拉取和维护历史 K 线
2. 订阅实时 K 线
3. 运行本地策略
4. 支持 AI 辅助策略
5. 记录日志并发送钉钉通知

## 目录

- [main.py](main.py)：CLI 入口
- [config.yaml](config.yaml)：配置文件
- `module/`：监控、历史、总控
- `okx_api/`：OKX REST / WebSocket 封装
- `ai/`：AI 接口与工具
- [strateg/README.md](strateg/README.md)：策略编写说明
- `strateg/`：策略目录

## 当前状态

- `monitor`：可用
- `history`：可用
- `backtest`：未完成，不建议使用

## 常用命令

实时监控，可存在多个-P参数：

```bash
python3 main.py monitor run -P "symbol=btc,type=swap,frames=15m|1h,strategies=ai_mysmc,interval=900"
```

下载历史：

```bash
python3 main.py history dowload -s btc,eth -t swap -f 15m,1h -i 20250101-
```

检查历史：

```bash
python3 main.py history check -c
```

## 配置

主要看 [config.yaml](config.yaml)：

- `okx`：OKX API 配置
- `proxy`：代理
- `thread_workers`：全局线程池大小
- `monitor.default_history_days`：各周期默认预热天数
- `ai`：AI provider 配置

## 策略

当前策略使用目录结构：

```text
strateg/
  ai_mysmc/
    main.py
  strategy_template/
    main.py
```

约定：

1. 策略名等于目录名
2. 入口文件固定为 `main.py`
3. 入口类固定为 `strateg`

详细说明看：

- [strateg/README.md](strateg/README.md)
