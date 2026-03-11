import os
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime, timezone, timedelta

from lib.logger import okx_logger
from lib.globalVar import getVar

BEIJING_TZ = timezone(timedelta(hours=8))


class OkxDatabase:
    """OKX数据库操作类"""
    
    def __init__(self):
        """
        初始化数据库操作类
        """
        data_dir = self._get_data_dir()
        # 确保数据目录存在
        data_dir.mkdir(parents=True, exist_ok=True)
        okx_logger.info(f"[数据库][初始化][完成] 路径={data_dir}")

    def _get_data_dir(self) -> Path:
        """从全局变量解析数据目录。"""
        raw_path = getVar("DATA_DIR", "data")
        base_dir = Path(__file__).resolve().parent.parent
        path_obj = Path(raw_path)
        if path_obj.is_absolute():
            return path_obj
        return (base_dir / path_obj).resolve()

    def get_data_root(self) -> Path:
        """公开的数据目录路径。"""
        return self._get_data_dir()
    
    @staticmethod
    def sanitize_symbol(symbol: str) -> str:
        """
        将 symbol 规范为适合文件系统/日志的格式。
        """
        sanitized = (symbol or "").upper()
        for delimiter in ("/", ":", "-"):
            sanitized = sanitized.replace(delimiter, "_")
        return sanitized

    def _get_db_path(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
        *,
        create_dirs: bool = True,
    ) -> Path:
        """
        获取数据库文件路径
        
        参数:
            symbol: 币种符号，如 'BTC'
            market_type: 市场类型，'spot' 或 'perp'
            timeframe: 时间周期，如 '15m'
            
        返回:
            数据库文件完整路径
        """
        data_dir = self._get_data_dir()
        sanitized_symbol = self.sanitize_symbol(symbol)
        folder_name = f"{sanitized_symbol}-{market_type}"
        folder_path = data_dir / folder_name
        if create_dirs:
            folder_path.mkdir(parents=True, exist_ok=True)

        db_filename = f"{timeframe}.db"
        return folder_path / db_filename
    
    def _get_connection(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
        *,
        create_dirs: bool = True,
        readonly: bool = False,
    ) -> sqlite3.Connection:
        """
        获取数据库连接
        
        参数:
            symbol: 币种符号
            market_type: 市场类型
            timeframe: 时间周期
            
        返回:
            SQLite连接对象
        """
        db_path = self._get_db_path(
            symbol,
            market_type,
            timeframe,
            create_dirs=create_dirs and not readonly,
        )

        if readonly:
            if not db_path.exists():
                raise FileNotFoundError(
                    f"database not found for {symbol}-{market_type}-{timeframe}"
                )
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        else:
            if create_dirs and not db_path.parent.exists():
                db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(db_path)

        conn.row_factory = sqlite3.Row  # 使查询结果可以按列名访问
        return conn
    
    def _init_table(self, conn: sqlite3.Connection):
        """
        初始化数据表
        
        参数:
            conn: 数据库连接
        """
        cursor = conn.cursor()
        
        # 创建K线数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS klines (
                timestamp DATETIME PRIMARY KEY,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                volume_contract REAL,
                volume_currency REAL,
                volume_quote REAL,
                confirm INTEGER
            )
        ''')

        cursor.execute("PRAGMA table_info(klines)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        extra_columns = {
            'volume_contract': 'REAL',
            'volume_currency': 'REAL',
            'volume_quote': 'REAL',
            'confirm': 'INTEGER'
        }
        for column, definition in extra_columns.items():
            if column not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE klines ADD COLUMN {column} {definition}")
                except sqlite3.OperationalError:
                    pass
        
        conn.commit()
    
    def insert_klines(self, symbol: str, market_type: str, timeframe: str, data: str) -> bool:
        """
        插入K线数据
        
        参数:
            symbol: 币种符号（自动转换为大写）
            market_type: 市场类型，'spot' 或 'perp'
            timeframe: 时间周期，如 '15m'
            data: JSON字符串格式的K线数据
            
        返回:
            bool: 插入是否成功
        """
        try:
            # 规范化币种格式
            symbol = self.sanitize_symbol(symbol)
            
            # 解析JSON数据
            klines_data = json.loads(data)
            
            if not isinstance(klines_data, list):
                raise ValueError("数据格式错误：应为列表格式")
            
            # 获取数据库连接
            conn = self._get_connection(symbol, market_type, timeframe)
            
            # 初始化表
            self._init_table(conn)
            
            cursor = conn.cursor()
            
            # 批量插入数据
            for kline in klines_data:
                # 验证必需字段
                required_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                for field in required_fields:
                    if field not in kline:
                        raise ValueError(f"缺少必需字段: {field}")
                
                # 转换时间戳（毫秒转秒），统一转换为北京时间
                timestamp = (
                    datetime.fromtimestamp(kline['timestamp'] / 1000, tz=timezone.utc)
                    .astimezone(BEIJING_TZ)
                    .replace(tzinfo=None)
                )
                
                # 插入数据（使用 INSERT OR REPLACE 避免重复）
                volume_value = float(kline.get('volume', 0) or 0)
                volume_contract = float(kline.get('volume_contract', volume_value) or 0)
                volume_currency = float(kline.get('volume_currency', volume_value) or 0)
                volume_quote = float(kline.get('volume_quote', 0) or 0)
                confirm_value = int(kline.get('confirm', 1) or 0)

                cursor.execute('''
                    INSERT OR REPLACE INTO klines 
                    (timestamp, open, high, low, close, volume, volume_contract, volume_currency, volume_quote, confirm)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    timestamp,
                    kline['open'],
                    kline['high'],
                    kline['low'],
                    kline['close'],
                    volume_value,
                    volume_contract,
                    volume_currency,
                    volume_quote,
                    confirm_value,
                ))
            
            conn.commit()
            conn.close()
            
            okx_logger.debug(
                f"[数据库][写入][成功] 币种={symbol}-{market_type}-{timeframe} 条数={len(klines_data)}"
            )
            return True
            
        except json.JSONDecodeError as e:
            okx_logger.error(
                f"[数据库][写入][JSON格式错误] 币种={symbol}-{market_type}-{timeframe} 错误={e}"
            )
            return False
        except ValueError as e:
            okx_logger.error(
                f"[数据库][写入][校验失败] 币种={symbol}-{market_type}-{timeframe} 错误={e}"
            )
            return False
        except Exception as e:
            okx_logger.error(
                f"[数据库][写入][失败] 币种={symbol}-{market_type}-{timeframe} 错误={e}"
            )
            return False
    
    def query_klines(self, symbol: str, market_type: str, timeframe: str, 
                     start_timestamp: int, end_timestamp: int) -> List[Dict[str, Any]]:
        """
        查询指定时间段的K线数据
        
        参数:
            symbol: 币种符号（自动转换为大写）
            market_type: 市场类型，'spot' 或 'perp'
            timeframe: 时间周期，如 '15m'
            start_timestamp: 开始时间戳（毫秒）
            end_timestamp: 结束时间戳（毫秒）
            
        返回:
            List[Dict]: K线数据列表
        """
        symbol = self.sanitize_symbol(symbol)
        start_time = (
            datetime.fromtimestamp(start_timestamp / 1000, tz=timezone.utc)
            .astimezone(BEIJING_TZ)
            .replace(tzinfo=None)
        )
        end_time = (
            datetime.fromtimestamp(end_timestamp / 1000, tz=timezone.utc)
            .astimezone(BEIJING_TZ)
            .replace(tzinfo=None)
        )

        conn = self._get_connection(symbol, market_type, timeframe)
        rows = []

        try:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT timestamp, open, high, low, close, volume, volume_contract, volume_currency, volume_quote, confirm
                FROM klines
                WHERE timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp ASC
                ''',
                (start_time, end_time),
            )
            rows = cursor.fetchall()
        except sqlite3.OperationalError:
            okx_logger.info(
                f"[数据库][查询][空缓存] 币种={symbol}-{market_type}-{timeframe}"
            )
            return []
        except Exception as exc:
            okx_logger.error(
                f"[数据库][查询][失败] 币种={symbol}-{market_type}-{timeframe} "
                f"区间={start_timestamp}-{end_timestamp} 错误={exc}"
            )
            return []
        finally:
            conn.close()

        result = []
        for row in rows:
            raw_ts = row['timestamp']
            ts_millis = self._convert_db_timestamp_to_millis(raw_ts)

            result.append({
                'timestamp': ts_millis,
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row['volume'],
                'volume_contract': row['volume_contract'],
                'volume_currency': row['volume_currency'],
                'volume_quote': row['volume_quote'],
                'confirm': row['confirm'],
            })

        okx_logger.info(
            f"[数据库][查询][成功] 币种={symbol}-{market_type}-{timeframe} "
            f"区间={start_time}~{end_time} 条数={len(result)}"
        )
        return result

    def _convert_db_timestamp_to_millis(self, raw_ts: Any) -> int:
        """
        将数据库中的时间字段统一转换为 UTC 毫秒时间戳。
        """
        if isinstance(raw_ts, str):
            parsed_ts = None
            for fmt in (None, "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                try:
                    if fmt is None:
                        parsed_ts = datetime.fromisoformat(raw_ts)
                    else:
                        parsed_ts = datetime.strptime(raw_ts, fmt)
                    break
                except ValueError:
                    continue
            if parsed_ts is None:
                raise ValueError(f"无法解析时间戳: {raw_ts}")
        elif isinstance(raw_ts, datetime):
            parsed_ts = raw_ts
        else:
            # SQLite 可能直接返回时间戳（秒），需要统一换算到毫秒
            return int(float(raw_ts) * 1000)

        if parsed_ts.tzinfo is None:
            parsed_ts = parsed_ts.replace(tzinfo=BEIJING_TZ)
        else:
            parsed_ts = parsed_ts.astimezone(BEIJING_TZ)

        return int(parsed_ts.timestamp() * 1000)
