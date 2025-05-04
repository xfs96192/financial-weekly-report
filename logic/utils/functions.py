"""
通用工具函数模块，提供各种辅助函数
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os


def format_currency(value, decimal_places=2):
    """
    将数值格式化为货币格式（带千位分隔符）
    
    Args:
        value: 数值
        decimal_places: 小数位数，默认为2
        
    Returns:
        格式化后的字符串
    """
    if pd.isna(value):
        return "0.00"
    
    try:
        # 四舍五入到指定小数位
        rounded_value = round(float(value), decimal_places)
        
        # 格式化为带千位分隔符的字符串
        formatted_value = "{:,}".format(rounded_value)
        
        return formatted_value
    except (ValueError, TypeError):
        return "0.00"


def calculate_percentage_change(current_value, previous_value):
    """
    计算两个值之间的百分比变化
    
    Args:
        current_value: 当前值
        previous_value: 先前值
        
    Returns:
        百分比变化，若先前值为0或None则返回None
    """
    try:
        if pd.isna(previous_value) or previous_value == 0:
            return None
        
        current_value = float(current_value) if not pd.isna(current_value) else 0
        previous_value = float(previous_value)
        
        change = (current_value - previous_value) / previous_value
        return change
    except (ValueError, TypeError):
        return None


def format_percentage(value, decimal_places=2, include_sign=True):
    """
    将数值格式化为百分比格式
    
    Args:
        value: 要格式化的数值（例如0.05表示5%）
        decimal_places: 小数位数，默认为2
        include_sign: 是否包含正负号，默认为True
        
    Returns:
        格式化后的百分比字符串
    """
    if pd.isna(value):
        return "0.00%"
    
    try:
        # 转换为浮点数并乘以100
        percentage = float(value) * 100
        
        # 四舍五入到指定小数位
        rounded_percentage = round(percentage, decimal_places)
        
        # 添加百分号
        if include_sign and rounded_percentage > 0:
            return "+{:.{prec}f}%".format(rounded_percentage, prec=decimal_places)
        else:
            return "{:.{prec}f}%".format(rounded_percentage, prec=decimal_places)
    except (ValueError, TypeError):
        return "0.00%"


def calculate_date_range(base_date, period_type='week', periods_ago=1):
    """
    计算相对于基准日期的历史日期范围
    
    Args:
        base_date: 基准日期（字符串或datetime对象）
        period_type: 周期类型，可选值：'day', 'week', 'month', 'year'
        periods_ago: 往前推多少个周期，默认为1
        
    Returns:
        历史日期字符串，格式为'YYYY-MM-DD'
    """
    if isinstance(base_date, str):
        base_date = datetime.strptime(base_date, '%Y-%m-%d')
    
    if period_type == 'day':
        history_date = base_date - timedelta(days=periods_ago)
    elif period_type == 'week':
        history_date = base_date - timedelta(days=7*periods_ago)
    elif period_type == 'month':
        # 使用替换月份的方法，确保日期有效
        month = base_date.month - periods_ago % 12
        year = base_date.year - periods_ago // 12
        if month <= 0:
            month += 12
            year -= 1
        
        # 确保日期有效（例如，2月没有30日）
        day = min(base_date.day, [31, 29 if (year % 4 == 0 and year % 100 != 0) or year % 400 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month-1])
        history_date = datetime(year, month, day)
    elif period_type == 'year':
        # 处理闰年2月29日的情况
        if base_date.month == 2 and base_date.day == 29:
            # 检查目标年份是否闰年
            target_year = base_date.year - periods_ago
            if (target_year % 4 == 0 and target_year % 100 != 0) or target_year % 400 == 0:
                history_date = datetime(target_year, 2, 29)
            else:
                history_date = datetime(target_year, 2, 28)
        else:
            history_date = datetime(base_date.year - periods_ago, base_date.month, base_date.day)
    else:
        raise ValueError(f"不支持的周期类型: {period_type}")
    
    return history_date.strftime('%Y-%m-%d')


def group_and_sum(df, group_by_cols, sum_cols):
    """
    按指定列分组并对指定列求和
    
    Args:
        df: 输入DataFrame
        group_by_cols: 用于分组的列名列表
        sum_cols: 要求和的列名列表
        
    Returns:
        分组并求和后的DataFrame
    """
    try:
        # 检查列是否存在
        for col in group_by_cols + sum_cols:
            if col not in df.columns:
                raise ValueError(f"列 '{col}' 不存在于DataFrame中")
        
        # 使用分组和聚合
        grouped_df = df.groupby(group_by_cols)[sum_cols].sum().reset_index()
        return grouped_df
    except Exception as e:
        print(f"分组求和失败: {e}")
        return df


def safe_divide(numerator, denominator, default_value=0):
    """
    安全除法，防止除数为零的错误
    
    Args:
        numerator: 分子
        denominator: 分母
        default_value: 当分母为零或None时的默认返回值
        
    Returns:
        除法结果，或在分母为零或None时返回default_value
    """
    try:
        if pd.isna(denominator) or denominator == 0:
            return default_value
        
        numerator = float(numerator) if not pd.isna(numerator) else 0
        denominator = float(denominator)
        
        return numerator / denominator
    except (ValueError, TypeError):
        return default_value


def merge_with_history(current_df, historical_df, on_columns, value_columns):
    """
    合并当前数据和历史数据，计算变化
    
    Args:
        current_df: 当前数据DataFrame
        historical_df: 历史数据DataFrame
        on_columns: 用于合并的键列列表
        value_columns: 要计算变化的值列列表
        
    Returns:
        合并后的DataFrame，包含原始列和变化列
    """
    if historical_df is None:
        return current_df
    
    try:
        # 给历史数据的列添加后缀
        historical_df = historical_df.copy()
        rename_dict = {col: f"{col}_历史" for col in value_columns}
        historical_df.rename(columns=rename_dict, inplace=True)
        
        # 合并数据
        merged_df = pd.merge(
            current_df, 
            historical_df[on_columns + [f"{col}_历史" for col in value_columns]],
            on=on_columns,
            how='left'
        )
        
        # 计算变化
        for col in value_columns:
            merged_df[f"{col}_变化"] = merged_df.apply(
                lambda x: calculate_percentage_change(x[col], x[f"{col}_历史"]), 
                axis=1
            )
        
        return merged_df
    except Exception as e:
        print(f"合并历史数据失败: {e}")
        return current_df


def style_dataframe(df, currency_cols=None, percentage_cols=None, highlight_cols=None):
    """
    给DataFrame应用样式
    
    Args:
        df: 输入DataFrame
        currency_cols: 需要格式化为货币的列列表
        percentage_cols: 需要格式化为百分比的列列表
        highlight_cols: 需要突出显示的列列表
        
    Returns:
        样式化后的DataFrame
    """
    styled_df = df.copy()
    
    # 格式化货币列
    if currency_cols:
        for col in currency_cols:
            if col in styled_df.columns:
                styled_df[col] = styled_df[col].apply(format_currency)
    
    # 格式化百分比列
    if percentage_cols:
        for col in percentage_cols:
            if col in styled_df.columns:
                styled_df[col] = styled_df[col].apply(format_percentage)
    
    return styled_df