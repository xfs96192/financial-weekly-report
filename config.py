"""
金融周报自动生成系统配置文件
配置数据路径、报告日期和其他参数
"""

import os
from datetime import datetime, timedelta

# 基础路径配置
DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')

# 报告日期配置（默认为上周五）
today = datetime.now()
days_to_friday = (today.weekday() - 4) % 7
last_friday = today - timedelta(days=days_to_friday if days_to_friday > 0 else 7)
REPORT_DATE = last_friday.strftime('%Y-%m-%d')

# 数据文件名配置
PORTFOLIO_FILE = '底层数据汇总.xlsx'
CHANNEL_DATA_FILE = '渠道数据.xlsx'
NEW_PRODUCT_FILE = '新发统计表.xlsx'
HISTORICAL_DATA_PATH = os.path.join(DATA_PATH, '历史数据')

# 报表配置
REPORT_SHEETS = [
    '产品规模',
    '渠道分析',
    '投资经理规模',
    '大类资产配置',
    '中收监控',
    '产品到期情况',
    '高波动产品',
    '委外投资风险',
    '公共策略专户',
    '投资经理中收',
    '周期产品规模变化',
    '客户周期产品'
]

# 波动率阈值配置
HIGH_VOLATILITY_THRESHOLD = 0.03  # 3%

# 历史数据对比日期
LAST_WEEK_DATE = (last_friday - timedelta(days=7)).strftime('%Y-%m-%d')
LAST_MONTH_DATE = (last_friday - timedelta(days=30)).strftime('%Y-%m-%d')
LAST_YEAR_DATE = (last_friday - timedelta(days=365)).strftime('%Y-%m-%d')

# 日志配置
LOG_FILE = os.path.join(OUTPUT_PATH, f'log_{datetime.now().strftime("%Y%m%d")}.log')