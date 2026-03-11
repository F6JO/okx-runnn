import json
from typing import Dict, List, Any
import math


class OkxFormat:

    def format_balance(input):
        output = {}
        for key, value in input['total'].items():
            if value < 0.000001:  # 非常小的数值
                output[key] = f"{value:.10f}"
            elif value < 0.001:   # 小数值
                output[key] = f"{value:.6f}"
            else:                   # 较大数值
                output[key] = f"{value:.4f}"
        output['total_USDT'] = input['info']['data'][0]['totalEq']
        return output

    def format_order_book(input: Dict[str, Any], accuracy: float = 0.1) -> Dict[str, Any]:
        """
        格式化订单簿数据，按指定精度聚合
        
        参数:
            input: 原始订单簿数据，格式为：
                {
                    'symbol': 交易对名称,
                    'bids': [[price, amount, count], ...],
                    'asks': [[price, amount, count], ...],
                    'timestamp': 时间戳,
                    'datetime': 日期时间,
                    'nonce': None
                }
            accuracy: 价格精度，直接表示价格单位
                     如 accuracy=1 表示按1聚合
                     accuracy=0.1 表示按0.1聚合
                     accuracy=0.5 表示按0.5聚合
                     accuracy=10 表示按10聚合
            
        返回:
            与输入格式完全相同的订单簿数据，只是价格按精度聚合
        """
        def round_price(price: float, accuracy: float) -> float:
            """按指定精度向下取整，处理浮点数精度问题"""
            # 将价格和精度转换为整数计算，避免浮点数精度问题
            scale = 10 ** (len(str(accuracy).split('.')[-1]) if '.' in str(accuracy) else 0)
            price_scaled = int(price * scale)
            accuracy_scaled = int(accuracy * scale)
            
            # 使用整数计算
            rounded = (price_scaled // accuracy_scaled) * accuracy_scaled
            
            # 转回原始精度
            return rounded / scale

        def aggregate_orders(orders: List[List[float]], accuracy: float) -> List[List[float]]:
            """聚合订单"""
            price_map = {}
            count_map = {}
            
            # 聚合相同价格的订单
            for price, amount, count in orders:
                rounded_price = round_price(float(price), accuracy)
                if rounded_price in price_map:
                    price_map[rounded_price] += float(amount)
                    count_map[rounded_price] += int(count)
                else:
                    price_map[rounded_price] = float(amount)
                    count_map[rounded_price] = int(count)
            
            # 转换回列表格式
            aggregated = [[price, amount, count_map[price]] 
                         for price, amount in price_map.items()]
            return aggregated

        # 创建与输入完全相同的结构
        result = input.copy()

        # 处理买单(bids)，保持降序
        aggregated_bids = aggregate_orders(input['bids'], accuracy)
        aggregated_bids.sort(reverse=True)
        result['bids'] = aggregated_bids

        # 处理卖单(asks)，保持升序
        aggregated_asks = aggregate_orders(input['asks'], accuracy)
        aggregated_asks.sort()
        result['asks'] = aggregated_asks

        return result
        