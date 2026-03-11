from datetime import datetime
from lib.logger import okx_logger

def is_in_time_range() -> bool:
    """
    判断当前时间是否在指定的时间区间内
    
    工作日（周一到周五）：0:00-9:30
    周末（周六周日）：2:00-9:30
    
    返回:
        bool: True表示在时间区间内，False表示不在
    """
    now = datetime.now()
    current_hour = now.hour
    current_minute = now.minute
    current_weekday = now.weekday()  # 0-6表示周一到周日
    
    # 将当前时间转换为分钟数，方便比较
    current_time_in_minutes = current_hour * 60 + current_minute
    nine_thirty_in_minutes = 9 * 60 + 30
    
    # 工作日（周一到周五）
    if current_weekday < 5:  # 0-4表示周一到周五
        return 0 <= current_time_in_minutes < nine_thirty_in_minutes
        
    # 周末（周六周日）
    else:  # 5-6表示周六周日
        two_am_in_minutes = 2 * 60
        return two_am_in_minutes <= current_time_in_minutes < nine_thirty_in_minutes

# 使用示例
def check_time():
    now = datetime.now()
    weekday_names = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
    
    result = is_in_time_range()
    okx_logger.info(f"当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')} {weekday_names[now.weekday()]}")
    okx_logger.info(f"是否在休息时间: {'是' if result else '否'}")
    
    return result

def print_time(text):
    now = datetime.now()
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {text}")
