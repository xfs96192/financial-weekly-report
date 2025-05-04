"""
数据处理模块，负责处理数据加载后的预处理和转换
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime

from config import REPORT_DATE
from logic.utils.functions import (
    calculate_percentage_change, 
    merge_with_history, 
    group_and_sum, 
    safe_divide
)

# 设置日志
logger = logging.getLogger(__name__)

def process_operation_overview(operation_overview_df):
    """
    处理运作概览数据
    
    Args:
        operation_overview_df: 运作概览原始数据DataFrame
        
    Returns:
        处理后的运作概览DataFrame
    """
    try:
        # 复制数据，避免修改原始数据
        df = operation_overview_df.copy()
        
        # 确保日期列格式正确
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
        
        # 筛选出当前报表日期的数据
        report_date = pd.to_datetime(REPORT_DATE)
        df = df[df['日期'] == report_date]
        
        # 计算周收益率（如果有上周净值）
        if '上周净值' in df.columns and '净值' in df.columns:
            df['周收益率'] = df.apply(
                lambda x: (x['净值'] / x['上周净值'] - 1) if pd.notna(x['上周净值']) and x['上周净值'] > 0 else np.nan, 
                axis=1
            )
        
        # 计算每个产品的规模占总规模的比例
        total_scale = df['规模'].sum()
        df['规模占比'] = df['规模'] / total_scale
        
        logger.info("运作概览数据处理完成")
        return df
    
    except Exception as e:
        logger.error(f"运作概览数据处理失败: {e}")
        return operation_overview_df


def process_holdings(holdings_df, operation_overview_df=None):
    """
    处理持仓明细数据
    
    Args:
        holdings_df: 持仓明细原始数据DataFrame
        operation_overview_df: 运作概览数据，用于关联产品规模
        
    Returns:
        处理后的持仓明细DataFrame
    """
    try:
        # 复制数据，避免修改原始数据
        df = holdings_df.copy()
        
        # 确保日期列格式正确
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
        
        # 筛选出当前报表日期的数据
        report_date = pd.to_datetime(REPORT_DATE)
        df = df[df['日期'] == report_date]
        
        # 如果提供了运作概览数据，将产品规模关联到持仓明细
        if operation_overview_df is not None and '产品代码' in df.columns and '产品代码' in operation_overview_df.columns:
            # 创建产品代码和规模的映射
            product_scale_map = dict(
                zip(operation_overview_df['产品代码'], operation_overview_df['规模'])
            )
            
            # 为持仓明细添加产品规模列
            df['产品规模'] = df['产品代码'].map(product_scale_map)
            
            # 计算持仓金额（持仓比例 * 产品规模）
            if '持仓比例' in df.columns:
                df['持仓金额'] = df['持仓比例'] * df['产品规模']
        
        # 标准化持仓类型
        if '资产类型' in df.columns:
            # 将持仓类型映射为标准类型
            asset_type_map = {
                '股票': '股票',
                'STOCK': '股票',
                'A股': '股票',
                '债券': '债券',
                'BOND': '债券',
                '基金': '基金',
                'FUND': '基金',
                '货币基金': '基金',
                '股票型基金': '基金',
                '债券型基金': '基金',
                '混合型基金': '基金',
                '定期存款': '现金及其他',
                '存款': '现金及其他',
                '活期存款': '现金及其他',
                '现金': '现金及其他',
                '其他': '现金及其他'
            }
            
            # 使用映射表标准化资产类型，未匹配的保持原值
            df['标准资产类型'] = df['资产类型'].map(
                lambda x: asset_type_map.get(x, '其他') if pd.notna(x) else '其他'
            )
        
        logger.info("持仓明细数据处理完成")
        return df
    
    except Exception as e:
        logger.error(f"持仓明细数据处理失败: {e}")
        return holdings_df


def process_channel_data(channel_df, operation_overview_df=None):
    """
    处理渠道数据
    
    Args:
        channel_df: 渠道数据原始DataFrame
        operation_overview_df: 运作概览数据，用于校对规模
        
    Returns:
        处理后的渠道数据DataFrame
    """
    try:
        # 复制数据，避免修改原始数据
        df = channel_df.copy()
        
        # 确保日期列格式正确
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
        
        # 筛选出当前报表日期的数据
        report_date = pd.to_datetime(REPORT_DATE)
        if '日期' in df.columns:
            df = df[df['日期'] == report_date]
        
        # 标准化渠道名称
        if '渠道名称' in df.columns:
            # 将不同表述的相同渠道统一命名
            channel_name_map = {
                '外部渠道1': '外部渠道A',
                '外部渠道2': '外部渠道B',
                '渠道A': '外部渠道A',
                '渠道B': '外部渠道B',
                '自营渠道': '自营',
                '自有渠道': '自营'
            }
            
            # 使用映射表标准化渠道名称，未匹配的保持原值
            df['标准渠道名称'] = df['渠道名称'].map(
                lambda x: channel_name_map.get(x, x) if pd.notna(x) else '其他'
            )
        
        # 如果提供了运作概览数据，校对产品规模
        if (operation_overview_df is not None and 
            '产品代码' in df.columns and 
            '产品代码' in operation_overview_df.columns and 
            '规模' in df.columns):
            
            # 创建产品代码和规模的映射
            product_scale_map = dict(
                zip(operation_overview_df['产品代码'], operation_overview_df['规模'])
            )
            
            # 校对渠道产品规模
            df['校对产品规模'] = df['产品代码'].map(product_scale_map)
            
            # 如果渠道数据中的规模与运作概览中的规模差异较大，则使用运作概览的规模
            df['调整后规模'] = df.apply(
                lambda x: x['校对产品规模'] if pd.notna(x['校对产品规模']) and abs(x['规模'] - x['校对产品规模']) / x['校对产品规模'] > 0.1 else x['规模'],
                axis=1
            )
        
        logger.info("渠道数据处理完成")
        return df
    
    except Exception as e:
        logger.error(f"渠道数据处理失败: {e}")
        return channel_df


def merge_operation_with_history(current_df, historical_dfs):
    """
    将当前运作概览数据与历史数据合并
    
    Args:
        current_df: 当前报表日期的运作概览DataFrame
        historical_dfs: 历史运作概览数据字典，键为'last_week', 'last_month', 'last_year'
        
    Returns:
        合并了历史数据的DataFrame
    """
    try:
        # 复制当前数据，避免修改原始数据
        result_df = current_df.copy()
        
        # 定义合并的键列和值列
        key_columns = ['产品代码', '产品名称']
        value_columns = ['规模', '净值']
        
        # 依次与各历史数据合并
        for period, hist_df in historical_dfs.items():
            if hist_df is not None:
                # 添加后缀以区分不同时期的历史数据
                period_suffix = {
                    'last_week': '上周',
                    'last_month': '上月',
                    'last_year': '去年同期'
                }.get(period, '')
                
                # 为历史数据列重命名
                rename_dict = {col: f"{col}_{period_suffix}" for col in value_columns}
                hist_df_renamed = hist_df.copy()
                hist_df_renamed.rename(columns=rename_dict, inplace=True)
                
                # 合并数据
                result_df = pd.merge(
                    result_df,
                    hist_df_renamed[key_columns + list(rename_dict.values())],
                    on=key_columns,
                    how='left'
                )
                
                # 计算变化率
                for col in value_columns:
                    hist_col = f"{col}_{period_suffix}"
                    change_col = f"{col}变化_{period_suffix}"
                    
                    result_df[change_col] = result_df.apply(
                        lambda x: calculate_percentage_change(x[col], x[hist_col]),
                        axis=1
                    )
        
        logger.info("运作概览与历史数据合并完成")
        return result_df
    
    except Exception as e:
        logger.error(f"运作概览与历史数据合并失败: {e}")
        return current_df


def merge_channel_with_history(current_df, historical_dfs):
    """
    将当前渠道数据与历史数据合并
    
    Args:
        current_df: 当前报表日期的渠道数据DataFrame
        historical_dfs: 历史渠道数据字典，键为'last_week', 'last_month'
        
    Returns:
        合并了历史数据的DataFrame
    """
    try:
        # 复制当前数据，避免修改原始数据
        result_df = current_df.copy()
        
        # 定义合并的键列和值列
        key_columns = ['产品代码', '标准渠道名称'] if '标准渠道名称' in current_df.columns else ['产品代码', '渠道名称']
        value_columns = ['规模'] if '调整后规模' not in current_df.columns else ['调整后规模']
        
        # 依次与各历史数据合并
        for period, hist_df in historical_dfs.items():
            if hist_df is not None:
                # 添加后缀以区分不同时期的历史数据
                period_suffix = {
                    'last_week': '上周',
                    'last_month': '上月'
                }.get(period, '')
                
                # 标准化历史数据的渠道名称
                if '渠道名称' in hist_df.columns and '标准渠道名称' not in hist_df.columns:
                    hist_df = process_channel_data(hist_df)
                
                # 调整历史数据的键列以匹配当前数据
                hist_key_columns = key_columns.copy()
                if '标准渠道名称' in key_columns and '标准渠道名称' not in hist_df.columns:
                    hist_key_columns = ['产品代码', '渠道名称']
                
                # 为历史数据列重命名
                rename_dict = {col: f"{col}_{period_suffix}" for col in value_columns}
                hist_df_renamed = hist_df.copy()
                hist_df_renamed.rename(columns=rename_dict, inplace=True)
                
                # 合并数据
                result_df = pd.merge(
                    result_df,
                    hist_df_renamed[hist_key_columns + list(rename_dict.values())],
                    left_on=key_columns,
                    right_on=hist_key_columns,
                    how='left'
                )
                
                # 计算变化率
                for col in value_columns:
                    hist_col = f"{col}_{period_suffix}"
                    change_col = f"{col}变化_{period_suffix}"
                    
                    result_df[change_col] = result_df.apply(
                        lambda x: calculate_percentage_change(x[col], x[hist_col]),
                        axis=1
                    )
        
        logger.info("渠道数据与历史数据合并完成")
        return result_df
    
    except Exception as e:
        logger.error(f"渠道数据与历史数据合并失败: {e}")
        return current_df