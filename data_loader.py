"""
金融周报自动生成系统数据加载模块
负责从Excel文件和历史数据文件夹中加载原始数据
"""

import os
import pandas as pd
from datetime import datetime
import logging

from config import (
    DATA_PATH, PORTFOLIO_FILE, CHANNEL_DATA_FILE, 
    NEW_PRODUCT_FILE, HISTORICAL_DATA_PATH, REPORT_DATE
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_portfolio_data():
    """加载底层数据汇总Excel文件中的各个工作表"""
    file_path = os.path.join(DATA_PATH, PORTFOLIO_FILE)
    
    try:
        # 读取文件中的所有工作表
        excel_file = pd.ExcelFile(file_path)
        
        # 产品基本信息工作表
        product_info_df = pd.read_excel(excel_file, sheet_name='产品信息')
        
        # 运作概览工作表
        operation_overview_df = pd.read_excel(excel_file, sheet_name='运作概览')
        
        # 持仓明细工作表
        holdings_df = pd.read_excel(excel_file, sheet_name='持仓明细')
        
        # 投资经理映射工作表
        manager_mapping_df = pd.read_excel(excel_file, sheet_name='投资经理映射')
        
        logger.info(f"成功加载底层数据汇总文件: {file_path}")
        
        return {
            'product_info': product_info_df,
            'operation_overview': operation_overview_df,
            'holdings': holdings_df,
            'manager_mapping': manager_mapping_df
        }
    except Exception as e:
        logger.error(f"加载底层数据汇总文件失败: {e}")
        raise


def load_channel_data():
    """加载渠道数据Excel文件"""
    file_path = os.path.join(DATA_PATH, CHANNEL_DATA_FILE)
    
    try:
        channel_df = pd.read_excel(file_path)
        logger.info(f"成功加载渠道数据文件: {file_path}")
        return channel_df
    except Exception as e:
        logger.error(f"加载渠道数据文件失败: {e}")
        raise


def load_new_product_data():
    """加载新发产品统计表"""
    file_path = os.path.join(DATA_PATH, NEW_PRODUCT_FILE)
    
    try:
        new_product_df = pd.read_excel(file_path)
        logger.info(f"成功加载新发产品统计表: {file_path}")
        return new_product_df
    except Exception as e:
        logger.error(f"加载新发产品统计表失败: {e}")
        raise


def load_historical_data(date_str, file_type='底层数据汇总'):
    """加载指定日期的历史数据文件"""
    try:
        # 构建历史数据文件路径
        historical_file_path = os.path.join(
            HISTORICAL_DATA_PATH, 
            f"{date_str}_{file_type}.xlsx"
        )
        
        # 检查文件是否存在
        if not os.path.exists(historical_file_path):
            logger.warning(f"历史数据文件不存在: {historical_file_path}")
            return None
        
        # 读取文件中的所有工作表
        excel_file = pd.ExcelFile(historical_file_path)
        
        # 根据文件类型加载不同的工作表
        if file_type == '底层数据汇总':
            # 加载运作概览和持仓明细工作表
            operation_overview_df = pd.read_excel(excel_file, sheet_name='运作概览')
            holdings_df = pd.read_excel(excel_file, sheet_name='持仓明细')
            
            logger.info(f"成功加载历史数据文件: {historical_file_path}")
            
            return {
                'operation_overview': operation_overview_df,
                'holdings': holdings_df
            }
        elif file_type == '渠道数据':
            # 加载渠道数据
            channel_df = pd.read_excel(excel_file)
            
            logger.info(f"成功加载历史渠道数据文件: {historical_file_path}")
            
            return channel_df
        
    except Exception as e:
        logger.error(f"加载历史数据文件失败: {e}")
        return None


def load_all_data():
    """加载所有必要的数据"""
    try:
        data = {
            'current': {
                'portfolio': load_portfolio_data(),
                'channel': load_channel_data(),
                'new_product': load_new_product_data()
            },
            'historical': {}
        }
        
        # 尝试加载上周、上月、上年同期的历史数据
        from config import LAST_WEEK_DATE, LAST_MONTH_DATE, LAST_YEAR_DATE
        
        historical_dates = {
            'last_week': LAST_WEEK_DATE,
            'last_month': LAST_MONTH_DATE,
            'last_year': LAST_YEAR_DATE
        }
        
        for period, date_str in historical_dates.items():
            portfolio_data = load_historical_data(date_str, '底层数据汇总')
            channel_data = load_historical_data(date_str, '渠道数据')
            
            data['historical'][period] = {
                'portfolio': portfolio_data,
                'channel': channel_data
            }
        
        logger.info("所有数据加载完成")
        return data
        
    except Exception as e:
        logger.error(f"加载所有数据失败: {e}")
        raise


# 数据清洗函数
def clean_data(data):
    """
    对加载的数据进行初步清洗
    - 填充空值
    - 日期格式转换
    - 去除重复数据
    """
    try:
        # 当前数据清洗
        portfolio = data['current']['portfolio']
        
        # 清洗产品信息数据
        if 'product_info' in portfolio:
            df = portfolio['product_info']
            # 填充空值
            df.fillna({'产品名称': '', '产品类型': '未分类'}, inplace=True)
            # 日期格式转换
            if '成立日期' in df.columns:
                df['成立日期'] = pd.to_datetime(df['成立日期'], errors='coerce')
            if '到期日期' in df.columns:
                df['到期日期'] = pd.to_datetime(df['到期日期'], errors='coerce')
            # 去除重复记录
            df.drop_duplicates(subset=['产品代码'], keep='first', inplace=True)
            
        # 清洗运作概览数据
        if 'operation_overview' in portfolio:
            df = portfolio['operation_overview']
            # 填充空值
            df.fillna({'净值': 1.0, '规模': 0.0}, inplace=True)
            # 日期格式转换
            if '日期' in df.columns:
                df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
        
        # 清洗持仓明细数据
        if 'holdings' in portfolio:
            df = portfolio['holdings']
            # 填充空值
            df.fillna({'持仓比例': 0.0, '市值': 0.0}, inplace=True)
            
        # 清洗渠道数据
        if 'channel' in data['current']:
            df = data['current']['channel']
            # 填充空值
            df.fillna({'渠道名称': '未知渠道', '规模': 0.0}, inplace=True)
            
        logger.info("数据清洗完成")
        return data
        
    except Exception as e:
        logger.error(f"数据清洗失败: {e}")
        return data  # 返回原始数据，避免中断处理流程


# 当直接运行此脚本时，执行测试加载
if __name__ == "__main__":
    try:
        # 加载并清洗所有数据
        all_data = load_all_data()
        clean_data = clean_data(all_data)
        
        # 显示加载的数据概览
        for data_type, data_dict in clean_data['current'].items():
            if data_type == 'portfolio':
                for sheet_name, df in data_dict.items():
                    print(f"{sheet_name} 数据形状: {df.shape}")
            else:
                print(f"{data_type} 数据形状: {data_dict.shape}")
                
        print("数据加载测试成功")
        
    except Exception as e:
        print(f"数据加载测试失败: {e}")