import pandas as pd
import numpy as np
import talib
from typing import Dict, List, Any

class OkxIndicator:
    def __init__(self, klines_data: List[Dict[str, Any]]):
        """
        初始化指标计算类
        
        参数:
            klines_data: K线数据列表
            
        异常:
            ValueError: 当数据为空或格式不正确时
        """
        if not klines_data:
            raise ValueError("K线数据不能为空")

        # 转换为DataFrame便于计算
        self.df = pd.DataFrame(klines_data)
        
        # 检查必要的列是否存在
        required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            raise ValueError(f"K线数据缺少必要的列: {', '.join(missing_columns)}")

        # 按时间戳排序并去重（优先按毫秒时间戳解析）
        ts_series = self.df['timestamp']
        ts_numeric = pd.to_numeric(ts_series, errors='coerce')
        if ts_numeric.notna().any() and ts_numeric.median() > 1e11:
            self.df['timestamp'] = pd.to_datetime(ts_numeric, errors='coerce', unit='ms')
        else:
            self.df['timestamp'] = pd.to_datetime(ts_series, errors='coerce')
        if self.df['timestamp'].isna().any():
            raise ValueError("存在无效的时间戳数据")
        self.df = (
            self.df.sort_values('timestamp')
            .drop_duplicates(subset='timestamp', keep='last')
            .reset_index(drop=True)
        )
        self._timestamp_ms = (self.df['timestamp'].astype('int64') // 10**6)

        # 转换数值列
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            self.df[col] = pd.to_numeric(self.df[col], errors='raise')

        # 验证数据有效性
        if (self.df['high'] < self.df['low']).any():
            raise ValueError("存在最高价低于最低价的无效数据")
        if (self.df['volume'] < 0).any():
            raise ValueError("存在成交量为负的无效数据")

    def calculate_macd(self, 
                      fast_period: int = 12,
                      slow_period: int = 26,
                      signal_period: int = 9) -> Dict[str, List[float]]:
        """
        计算MACD指标
        
        参数:
            fast_period: 快线周期
            slow_period: 慢线周期
            signal_period: 信号线周期
            
        返回:
            包含MACD指标的字典
            
        异常:
            可能抛出计算过程中的相关异常
        """
        for name, value in {
            "fast_period": fast_period,
            "slow_period": slow_period,
            "signal_period": signal_period,
        }.items():
            if value <= 0:
                raise ValueError(f"{name} 必须大于 0")

        macd_line, signal_line, macd_hist = talib.MACD(
            self.df['close'].values,
            fastperiod=fast_period,
            slowperiod=slow_period,
            signalperiod=signal_period
        )
        valid_mask = ~(
            np.isnan(macd_line) | np.isnan(signal_line) | np.isnan(macd_hist)
        )
        macd_line = macd_line[valid_mask]
        signal_line = signal_line[valid_mask]
        macd_hist = macd_hist[valid_mask]
        timestamps = self._timestamp_ms.values[valid_mask]

        return {
            'macd': macd_line.tolist(),
            'signal': signal_line.tolist(),
            'hist': macd_hist.tolist(),
            'timestamp': timestamps.tolist()
        }

    def calculate_rsi(self, period: int = 14) -> Dict[str, List[float]]:
        """
        计算RSI指标
        
        参数:
            period: RSI周期
            
        返回:
            包含RSI指标的字典
            
        异常:
            可能抛出计算过程中的相关异常
        """
        if period <= 0:
            raise ValueError("period 必须大于 0")
        rsi = talib.RSI(self.df['close'].values, timeperiod=period)
        valid_mask = ~np.isnan(rsi)
        rsi = rsi[valid_mask]
        timestamps = self._timestamp_ms.values[valid_mask]
        
        return {
            'rsi': rsi.tolist(),
            'timestamp': timestamps.tolist()
        }

    def calculate_bollinger_bands(self, 
                                period: int = 20, 
                                std_dev: float = 2.0) -> Dict[str, List[float]]:
        """
        计算布林带指标
        
        参数:
            period: 移动平均周期
            std_dev: 标准差倍数
            
        返回:
            包含布林带指标的字典
            
        异常:
            可能抛出计算过程中的相关异常
        """
        if period <= 0:
            raise ValueError("period 必须大于 0")
        if std_dev <= 0:
            raise ValueError("std_dev 必须大于 0")
        upper, middle, lower = talib.BBANDS(
            self.df['close'].values,
            timeperiod=period,
            nbdevup=std_dev,
            nbdevdn=std_dev,
            matype=talib.MA_Type.SMA
        )
        valid_mask = ~(
            np.isnan(upper) | np.isnan(middle) | np.isnan(lower)
        )
        upper = upper[valid_mask]
        middle = middle[valid_mask]
        lower = lower[valid_mask]
        timestamps = self._timestamp_ms.values[valid_mask]
        
        return {
            'upper': upper.tolist(),
            'middle': middle.tolist(),
            'lower': lower.tolist(),
            'timestamp': timestamps.tolist()
        }

    def calculate_ma(self, periods: List[int] = [5, 10, 20]) -> Dict[str, List[float]]:
        """
        计算多个周期的移动平均线
        
        参数:
            periods: MA周期列表
            
        返回:
            包含多个MA的字典
            
        异常:
            可能抛出计算过程中的相关异常
        """
        if not periods:
            raise ValueError("periods 不能为空")
        ma_values = {}
        valid_mask = np.ones(len(self.df), dtype=bool)
        for period in periods:
            if period <= 0:
                raise ValueError("MA 周期必须大于 0")
            ma = talib.SMA(self.df['close'].values, timeperiod=period)
            ma_values[period] = ma
            valid_mask &= ~np.isnan(ma)
        timestamps = self._timestamp_ms.values[valid_mask]
        result = {'timestamp': timestamps.tolist()}
        
        for period in periods:
            result[f'ma_{period}'] = ma_values[period][valid_mask].tolist()
            
        return result
