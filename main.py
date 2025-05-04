"""
金融周报自动生成系统主程序入口
"""

import os
import logging
import pandas as pd
from datetime import datetime

# 导入配置
from config import (
    OUTPUT_PATH, REPORT_DATE, REPORT_SHEETS,
    LAST_WEEK_DATE, LAST_MONTH_DATE, LAST_YEAR_DATE
)

# 导入数据加载模块
from data_loader import load_all_data, clean_data

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=os.path.join(OUTPUT_PATH, f'log_{datetime.now().strftime("%Y%m%d")}.log'),
    filemode='w'
)
logger = logging.getLogger(__name__)

# 确保输出目录存在
os.makedirs(OUTPUT_PATH, exist_ok=True)

def init_excel_writer():
    """初始化Excel写入器"""
    output_file = os.path.join(OUTPUT_PATH, f'周报_{REPORT_DATE}.xlsx')
    return pd.ExcelWriter(output_file, engine='openpyxl')

def process_product_scale(data, writer):
    """处理产品规模报表"""
    try:
        from logic.calculations.product_scale import calculate_product_scale
        
        # 获取当前数据和历史数据
        current_data = data['current']['portfolio']['operation_overview']
        historical_data = {
            'last_week': data['historical']['last_week']['portfolio']['operation_overview'] if data['historical']['last_week']['portfolio'] else None,
            'last_month': data['historical']['last_month']['portfolio']['operation_overview'] if data['historical']['last_month']['portfolio'] else None,
            'last_year': data['historical']['last_year']['portfolio']['operation_overview'] if data['historical']['last_year']['portfolio'] else None
        }
        
        # 计算产品规模
        df_product_scale = calculate_product_scale(current_data, historical_data)
        
        # 将结果写入Excel
        df_product_scale.to_excel(writer, sheet_name='产品规模', index=True)
        logger.info("产品规模报表生成完成")
        
    except Exception as e:
        logger.error(f"产品规模报表生成失败: {e}")

def process_channel_analysis(data, writer):
    """处理渠道分析报表"""
    try:
        from logic.calculations.channel_analysis import calculate_channel_analysis
        
        # 获取当前数据和历史数据
        current_channel = data['current']['channel']
        current_portfolio = data['current']['portfolio']['operation_overview']
        
        historical_channel = {
            'last_week': data['historical']['last_week']['channel'] if data['historical']['last_week']['channel'] else None,
            'last_month': data['historical']['last_month']['channel'] if data['historical']['last_month']['channel'] else None
        }
        
        # 计算渠道分析
        df_channel = calculate_channel_analysis(current_channel, current_portfolio, historical_channel)
        
        # 将结果写入Excel
        df_channel.to_excel(writer, sheet_name='渠道分析', index=True)
        logger.info("渠道分析报表生成完成")
        
    except Exception as e:
        logger.error(f"渠道分析报表生成失败: {e}")

def process_manager_aum(data, writer):
    """处理投资经理规模报表"""
    try:
        from logic.calculations.manager_aum import calculate_manager_aum
        
        # 获取数据
        manager_mapping = data['current']['portfolio']['manager_mapping']
        operation_overview = data['current']['portfolio']['operation_overview']
        
        # 计算投资经理规模
        df_manager_aum = calculate_manager_aum(manager_mapping, operation_overview)
        
        # 将结果写入Excel
        df_manager_aum.to_excel(writer, sheet_name='投资经理规模', index=True)
        logger.info("投资经理规模报表生成完成")
        
    except Exception as e:
        logger.error(f"投资经理规模报表生成失败: {e}")

def process_asset_allocation(data, writer):
    """处理大类资产配置报表"""
    try:
        from logic.calculations.asset_allocation import calculate_asset_allocation
        
        # 获取数据
        holdings = data['current']['portfolio']['holdings']
        
        # 计算大类资产配置
        df_asset_allocation = calculate_asset_allocation(holdings)
        
        # 将结果写入Excel
        df_asset_allocation.to_excel(writer, sheet_name='大类资产配置', index=True)
        logger.info("大类资产配置报表生成完成")
        
    except Exception as e:
        logger.error(f"大类资产配置报表生成失败: {e}")

def process_high_volatility_products(data, writer):
    """处理高波动产品报表"""
    try:
        from logic.calculations.high_volatility_products import identify_high_volatility_products
        
        # 获取数据
        current_data = data['current']['portfolio']['operation_overview']
        last_week_data = data['historical']['last_week']['portfolio']['operation_overview'] if data['historical']['last_week']['portfolio'] else None
        
        # 识别高波动产品
        df_high_volatility = identify_high_volatility_products(current_data, last_week_data)
        
        # 将结果写入Excel
        df_high_volatility.to_excel(writer, sheet_name='高波动产品', index=True)
        logger.info("高波动产品报表生成完成")
        
    except Exception as e:
        logger.error(f"高波动产品报表生成失败: {e}")

def process_expiring_products(data, writer):
    """处理产品到期情况报表"""
    try:
        from logic.calculations.expiring_products import calculate_expiring_products
        
        # 获取数据
        product_info = data['current']['portfolio']['product_info']
        operation_overview = data['current']['portfolio']['operation_overview']
        
        # 计算到期产品情况
        df_expiring = calculate_expiring_products(product_info, operation_overview)
        
        # 将结果写入Excel
        df_expiring.to_excel(writer, sheet_name='产品到期情况', index=True)
        logger.info("产品到期情况报表生成完成")
        
    except Exception as e:
        logger.error(f"产品到期情况报表生成失败: {e}")

def main():
    """主函数"""
    logger.info(f"开始生成 {REPORT_DATE} 的周报")
    
    try:
        # 1. 加载所有数据
        logger.info("正在加载数据...")
        raw_data = load_all_data()
        
        # 2. 数据清洗
        logger.info("正在清洗数据...")
        data = clean_data(raw_data)
        
        # 3. 初始化Excel写入器
        writer = init_excel_writer()
        
        # 4. 生成各个报表
        logger.info("正在生成报表...")
        
        # 检查需要生成的报表
        if '产品规模' in REPORT_SHEETS:
            process_product_scale(data, writer)
            
        if '渠道分析' in REPORT_SHEETS:
            process_channel_analysis(data, writer)
            
        if '投资经理规模' in REPORT_SHEETS:
            process_manager_aum(data, writer)
            
        if '大类资产配置' in REPORT_SHEETS:
            process_asset_allocation(data, writer)
            
        if '高波动产品' in REPORT_SHEETS:
            process_high_volatility_products(data, writer)
            
        if '产品到期情况' in REPORT_SHEETS:
            process_expiring_products(data, writer)
        
        # 5. 保存Excel文件
        writer.save()
        
        logger.info(f"周报生成完成，已保存至: {os.path.join(OUTPUT_PATH, f'周报_{REPORT_DATE}.xlsx')}")
        print(f"周报生成完成，已保存至: {os.path.join(OUTPUT_PATH, f'周报_{REPORT_DATE}.xlsx')}")
        
    except Exception as e:
        logger.error(f"周报生成失败: {e}")
        print(f"周报生成失败: {e}")
        
if __name__ == "__main__":
    main()