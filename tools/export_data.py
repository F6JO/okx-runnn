
import argparse
import os
import sys
import json
import csv
import time
from datetime import datetime, timedelta, timezone

# 调整路径以便从父目录的 lib 文件夹导入模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from lib.db import OkxDatabase, BEIJING_TZ
except ImportError as e:
    print(f"导入模块时出错: {e}")
    print("请确保此脚本在项目的根目录下运行，或者 `lib` 目录位于 Python 路径中。")
    sys.exit(1)

def format_timestamp_ms(ts, fmt='%Y-%m-%d %H:%M:%S'):
    """将毫秒时间戳转换为格式化字符串。"""
    return datetime.fromtimestamp(ts / 1000).strftime(fmt)

def parse_time_string(time_str):
    """将 YYYYMMDDHHMM 格式的时间字符串解析为 datetime 对象。"""
    try:
        return datetime.strptime(time_str, '%Y%m%d%H%M')
    except ValueError:
        print(f"错误：时间格式无效 '{time_str}'。请使用 YYYYMMDDHHMM 格式。")
        sys.exit(1)

def round_down_time(dt, timeframe):
    """根据K线的时间级别向下取整 datetime 对象。"""
    if 'm' in timeframe:
        minutes = int(timeframe.replace('m', ''))
        new_minute = (dt.minute // minutes) * minutes
        return dt.replace(minute=new_minute, second=0, microsecond=0)
    elif 'h' in timeframe:
        hours = int(timeframe.replace('h', ''))
        new_hour = (dt.hour // hours) * hours
        return dt.replace(hour=new_hour, minute=0, second=0, microsecond=0)
    else:
        return dt

def convert_db_time_to_ms(db_time_str: str) -> int:
    """将数据库中的时间字符串（北京时间）转换为毫秒时间戳"""
    dt_beijing = datetime.strptime(db_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=BEIJING_TZ)
    return int(dt_beijing.timestamp() * 1000)

def get_db_time_range(db: OkxDatabase, symbol: str, market_type: str, timeframe: str):
    """通过直接SQL查询获取数据库中的最小和最大时间戳"""
    try:
        # 使用内部方法获取只读连接
        conn = db._get_connection(symbol, market_type, timeframe, readonly=True)
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM klines")
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0] and result[1]:
            min_ts = convert_db_time_to_ms(result[0])
            max_ts = convert_db_time_to_ms(result[1])
            return min_ts, max_ts
    except FileNotFoundError:
        return None, None # 数据库文件不存在
    except Exception as e:
        print(f"查询数据库时间范围时出错: {e}")
        return None, None
    return None, None

def main():
    parser = argparse.ArgumentParser(description="从本地数据库导出K线数据。")
    parser.add_argument('-s', '--symbol', required=True, type=str, help="币种，例如：BTC, ETH")
    parser.add_argument('-t', '--type', required=True, choices=['spot', 'swap'], help="产品类型，例如：spot, swap")
    parser.add_argument('-f', '--timeframe', required=True, type=str, help="K线级别，例如：5m, 15m, 1h")
    parser.add_argument('-i', '--interval', required=True, type=str, help="时间区间，例如：202501011215- 或 202501011215-202501020215")
    parser.add_argument('-o', '--output', required=True, choices=['json', 'csv', 'txt', 'array'], help="输出格式：json, csv, txt, array")
    parser.add_argument('-p', '--path', default='output', type=str, help="保存路径（默认为 'output' 文件夹）")

    args = parser.parse_args()
    
    # 1. 复制主程序的符号命名逻辑
    symbol_upper = args.symbol.upper()
    if args.type == 'swap':
        normalized_symbol = f"{symbol_upper}/USDT:USDT"
    else:
        normalized_symbol = f"{symbol_upper}/USDT"
    
    storage_symbol = normalized_symbol.replace("/", "_").replace(":", "_")
    print(f"Info: 根据输入参数 -s {args.symbol} -t {args.type}，计算出的存储符号为: {storage_symbol}")

    # 2. 解析并取整时间区间
    time_parts = args.interval.split('-')
    if not (1 <= len(time_parts) <= 2):
        print("错误：无效的时间区间格式。请使用 'YYYYMMDDHHMM-' 或 'YYYYMMDDHHMM-YYYYMMDDHHMM'。")
        sys.exit(1)

    start_dt_raw = parse_time_string(time_parts[0])
    start_dt = round_down_time(start_dt_raw, args.timeframe)
    start_ts = int(start_dt.timestamp() * 1000)

    end_ts = None
    if len(time_parts) == 2 and time_parts[1]:
        end_dt_raw = parse_time_string(time_parts[1])
        end_dt = round_down_time(end_dt_raw, args.timeframe)
        end_ts = int(end_dt.timestamp() * 1000)
    else:
        end_ts = int(datetime.now().timestamp() * 1000)

    if start_dt_raw != start_dt:
        print(f"提示：开始时间 '{format_timestamp_ms(start_dt_raw.timestamp()*1000)}' 已根据时间级别 '{args.timeframe}' 向下取整为 '{format_timestamp_ms(start_ts)}'。")

    # 3. 连接数据库并检查数据是否存在
    try:
        db = OkxDatabase()
    except Exception as e:
        print(f"初始化数据库时出错: {e}")
        sys.exit(1)

    min_ts, max_ts = get_db_time_range(db, storage_symbol, args.type, args.timeframe)

    if min_ts is None or max_ts is None:
        inst_name = f"{storage_symbol}-{args.type}-{args.timeframe}"
        print(f"错误：在数据库中找不到 {inst_name} 的数据，或者数据库为空。")
        sys.exit(1)
        
    print(f"数据库包含从 {format_timestamp_ms(min_ts)} 到 {format_timestamp_ms(max_ts)} 的数据")

    if start_ts > max_ts:
        print(f"错误：开始时间 {format_timestamp_ms(start_ts)} 超出了可用数据范围。")
        sys.exit(1)
    if end_ts < min_ts:
        print(f"错误：结束时间 {format_timestamp_ms(end_ts)} 超出了可用数据范围。")
        sys.exit(1)

    # 4. 查询数据
    print("正在查询数据...")
    klines_dicts = db.query_klines(
        symbol=storage_symbol,
        market_type=args.type,
        timeframe=args.timeframe,
        start_timestamp=start_ts,
        end_timestamp=end_ts
    )

    if not klines_dicts:
        print("在指定的时间范围内没有找到数据。")
        sys.exit(0)
    
    print(f"找到了 {len(klines_dicts)} 条记录。")

    # 5. 根据要求处理数据格式
    processed_klines = []
    output_header = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    for kline in klines_dicts:
        beijing_dt = datetime.fromtimestamp(kline['timestamp'] / 1000, tz=BEIJING_TZ)
        processed_kline = {
            'timestamp': beijing_dt.strftime('%Y%m%d%H%M'),
            'open': kline['open'],
            'high': kline['high'],
            'low': kline['low'],
            'close': kline['close'],
            'volume': kline['volume'],
        }
        processed_klines.append(processed_kline)

    # 6. 格式化并保存数据
    os.makedirs(args.path, exist_ok=True)
    
    file_timestamp = int(time.time())
    file_extension = 'json' if args.output == 'array' else args.output
    output_filename = os.path.join(args.path, f"{file_timestamp}.{file_extension}")

    print(f"正在将数据保存到 {output_filename}...")
    
    if args.output == 'json':
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(processed_klines, f, ensure_ascii=False, separators=(',', ':'))
    elif args.output == 'array':
        output_array = [output_header]
        for kline in processed_klines:
            output_array.append([kline[h] for h in output_header])
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(output_array, f, ensure_ascii=False, separators=(',', ':'))
    elif args.output == 'csv':
        with open(output_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=output_header)
            writer.writeheader()
            writer.writerows(processed_klines)
    elif args.output == 'txt':
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(" ".join(output_header) + '\n')
            for kline in processed_klines:
                f.write(" ".join(map(str, (kline.get(h) for h in output_header))) + '\n')

    print("导出完成。")


if __name__ == "__main__":
    main()
