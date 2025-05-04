"""
产品规模计算模块
计算不同维度的产品规模及其变化情况
"""

import pandas as pd
import numpy as np
import logging

from logic.utils.functions import (
    format_currency, 
    format_percentage, 
    group_and_sum,
    safe_divide,
    style_dataframe
)

# 设置日志
logger = logging.getLogger(__name__)

def calculate_product_scale(current_data, historical_data):
    """
    计算产品规模报表
    
    Args:
        current_data: 当前运作概览数据
        historical_data: 历史运作概览数据字典，键为'last_week', 'last_month', 'last_year'
        
    Returns:
        产品规模分析DataFrame
    """
    try:
        # 复制当前数据，避免修改原始数据
        df = current_data.copy()
        
        # 确保存在必要的列
        required_columns = ['产品代码', '产品名称', '产品类型', '规模']
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"产品规模计算缺少必要列: {col}")
                df[col] = np.nan
        
        # 计算总规模
        total_scale = df['规模'].sum()
        logger.info(f"当前产品总规模: {total_scale}")
        
        # 计算各产品类型的规模及占比
        product_type_scale = group_and_sum(df, ['产品类型'], ['规模'])
        product_type_scale['占比'] = product_type_scale['规模'] / total_scale
        
        # 如果存在历史数据，计算变化情况
        if historical_data.get('last_week') is not None:
            last_week_data = historical_data['last_week']
            
            # 按产品类型汇总上周规模
            last_week_type_scale = group_and_sum(last_week_data, ['产品类型'], ['规模'])
            
            # 合并当前和上周数据
            product_type_scale = pd.merge(
                product_type_scale,
                last_week_type_scale.rename(columns={'规模': '上周规模'}),
                on='产品类型',
                how='left'
            )
            
            # 计算环比变化
            product_type_scale['环比变化'] = product_type_scale.apply(
                lambda x: safe_divide(x['规模'] - x['上周规模'], x['上周规模']),
                axis=1
            )
        
        if historical_data.get('last_month') is not None:
            last_month_data = historical_data['last_month']
            
            # 按产品类型汇总上月规模
            last_month_type_scale = group_and_sum(last_month_data, ['产品类型'], ['规模'])
            
            # 合并当前和上月数据
            product_type_scale = pd.merge(
                product_type_scale,
                last_month_type_scale.rename(columns={'规模': '上月规模'}),
                on='产品类型',
                how='left'
            )
            
            # 计算月度变化
            product_type_scale['月度变化'] = product_type_scale.apply(
                lambda x: safe_divide(x['规模'] - x['上月规模'], x['上月规模']),
                axis=1
            )
        
        if historical_data.get('last_year') is not None:
            last_year_data = historical_data['last_year']
            
            # 按产品类型汇总去年同期规模
            last_year_type_scale = group_and_sum(last_year_data, ['产品类型'], ['规模'])
            
            # 合并当前和去年同期数据
            product_type_scale = pd.merge(
                product_type_scale,
                last_year_type_scale.rename(columns={'规模': '去年同期规模'}),
                on='产品类型',
                how='left'
            )
            
            # 计算同比变化
            product_type_scale['同比变化'] = product_type_scale.apply(
                lambda x: safe_divide(x['规模'] - x['去年同期规模'], x['去年同期规模']),
                axis=1
            )
        
        # 添加总计行
        total_row = {'产品类型': '总计', '规模': total_scale, '占比': 1.0}
        
        # 如果有历史数据，计算总计行的变化情况
        if 'last_week' in historical_data and historical_data['last_week'] is not None:
            total_row['上周规模'] = historical_data['last_week']['规模'].sum()
            total_row['环比变化'] = safe_divide(total_scale - total_row['上周规模'], total_row['上周规模'])
        
        if 'last_month' in historical_data and historical_data['last_month'] is not None:
            total_row['上月规模'] = historical_data['last_month']['规模'].sum()
            total_row['月度变化'] = safe_divide(total_scale - total_row['上月规模'], total_row['上月规模'])
        
        if 'last_year' in historical_data and historical_data['last_year'] is not None:
            total_row['去年同期规模'] = historical_data['last_year']['规模'].sum()
            total_row['同比变化'] = safe_divide(total_scale - total_row['去年同期规模'], total_row['去年同期规模'])
        
        # 将总计行添加到DataFrame
        product_type_scale = pd.concat([product_type_scale, pd.DataFrame([total_row])], ignore_index=True)
        
        # 应用样式
        currency_cols = ['规模', '上周规模', '上月规模', '去年同期规模']
        percentage_cols = ['占比', '环比变化', '月度变化', '同比变化']
        styled_df = style_dataframe(product_type_scale, currency_cols, percentage_cols)
        
        logger.info("产品规模报表计算完成")
        return styled_df
    
    except Exception as e:
        logger.error(f"产品规模报表计算失败: {e}")
        # 返回空DataFrame
        return pd.DataFrame()


def calculate_product_scale_details(current_data, historical_data=None):
    """
    计算产品规模详情报表，展示每个产品的规模情况
    
    Args:
        current_data: 当前运作概览数据
        historical_data: 历史运作概览数据字典，可选
        
    Returns:
        产品规模详情DataFrame
    """
    try:
        # 复制当前数据，避免修改原始数据
        df = current_data.copy()
        
        # 确保存在必要的列
        required_columns = ['产品代码', '产品名称', '产品类型', '规模', '净值']
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"产品规模详情计算缺少必要列: {col}")
                df[col] = np.nan
        
        # 计算占总规模比例
        total_scale = df['规模'].sum()
        df['占总规模比例'] = df['规模'] / total_scale
        
        # 按产品类型计算占类型规模比例
        type_scale = df.groupby('产品类型')['规模'].sum().reset_index()
        type_scale.rename(columns={'规模': '类型总规模'}, inplace=True)
        
        # 合并回主DataFrame
        df = pd.merge(df, type_scale, on='产品类型', how='left')
        df['占类型规模比例'] = df['规模'] / df['类型总规模']
        
        # 如果存在历史数据，计算变化情况
        if historical_data and 'last_week' in historical_data and historical_data['last_week'] is not None:
            last_week_data = historical_data['last_week']
            
            # 按产品代码合并上周数据
            df = pd.merge(
                df,
                last_week_data[['产品代码', '规模', '净值']].rename(columns={'规模': '上周规模', '净值': '上周净值'}),
                on='产品代码',
                how='left'
            )
            
            # 计算周规模变化和净值变化
            df['周规模变化'] = df.apply(
                lambda x: safe_divide(x['规模'] - x['上周规模'], x['上周规模']),
                axis=1
            )
            
            df['周净值变化'] = df.apply(
                lambda x: safe_divide(x['净值'] - x['上周净值'], x['上周净值']),
                axis=1
            )
        
        # 选择输出列并排序
        output_columns = ['产品代码', '产品名称', '产品类型', '规模', '占总规模比例', '占类型规模比例', '净值']
        
        # 添加历史数据列（如果存在）
        if historical_data and 'last_week' in historical_data and historical_data['last_week'] is not None:
            output_columns.extend(['上周规模', '周规模变化', '上周净值', '周净值变化'])
        
        # 按规模降序排序
        df = df[output_columns].sort_values(by=['产品类型', '规模'], ascending=[True, False])
        
        # 应用样式
        currency_cols = ['规模', '上周规模'] if '上周规模' in df.columns else ['规模']
        percentage_cols = ['占总规模比例', '占类型规模比例']
        
        if '周规模变化' in df.columns:
            percentage_cols.extend(['周规模变化', '周净值变化'])
            
        styled_df = style_dataframe(df, currency_cols, percentage_cols)
        
        logger.info("产品规模详情报表计算完成")
        return styled_df
    
    except Exception as e:
        logger.error(f"产品规模详情报表计算失败: {e}")
        # 返回空DataFrame
        return pd.DataFrame()