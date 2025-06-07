import time
from datetime import datetime
import json
import pandas as pd
import requests
from config import MYSQL_URL
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine(MYSQL_URL)
Session = sessionmaker(bind=engine)

def get_stock_list():
    return pd.read_csv("data/stock_list.csv")

def get_fundamental_data(ts_code):
    """
    从东方财富获取基本面数据
    """
    # 请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': 'https://data.eastmoney.com/',
        'Connection': 'keep-alive'
    }
    
    # 构建请求URL
    url = f"https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        'reportName': 'RPT_CUSTOM_F10_FINANCE_GDATA',
        'columns': 'ALL',
        'sortColumns': 'REPORT_DATE',
        'sortTypes': '-1',
        'source': 'WEB',
        'client': 'WEB',
        'pageSize': '5',
        'filter': f'(SECURITY_CODE="{ts_code}")',
        '_': int(time.time() * 1000)
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        # 处理jQuery回调函数
        text = response.text
        try:
            # 尝试直接解析JSON
            data = json.loads(text)
        except:
            try:
                # 如果失败，尝试处理jQuery回调
                json_str = text[text.index('(') + 1:text.rindex(')')]
                data = json.loads(json_str)
            except:
                print(f"❌ 解析数据失败：{ts_code}, {text[:100]}")  # 打印前100个字符用于调试
                return None
        
        if not data.get('success'):
            print(f"❌ 获取数据失败：{ts_code}, {data.get('message', '未知错误')}")
            return None
            
        if not data.get('result', {}).get('data'):
            print(f"⚠️ 无数据：{ts_code}")
            return None
            
        # 转换为DataFrame
        df = pd.DataFrame(data['result']['data'])
        
        # 重命名列
        column_mapping = {
            # 基本信息
            'SECUCODE': 'ts_code',                    # 股票代码（如：603306.SH）
            'REPORT_DATE': 'trade_date',              # 报告期（如：2025-03-31）
            'REPORT_TYPE': 'report_type',             # 报告类型（如：一季报、中报、三季报、年报）
            'NOTICE_DATE': 'notice_date',             # 公告日期
            'UPDATE_DATE': 'update_date',             # 更新日期
            
            # 每股指标
            'EPSJB': 'eps',                          # 基本每股收益(元)
            'BPS': 'net_asset_per_share',            # 每股净资产(元)
            'MGJYXJJE': 'cash_flow_per_share',       # 每股现金流(元)
            
            # 成长性指标
            'TOTALOPERATEREVETZ': 'revenue_yoy',     # 营收同比增长率(%)
            'PARENTNETPROFITTZ': 'profit_yoy',       # 净利润同比增长率(%)
            
            # 盈利能力
            'ROEJQ': 'roe',                          # 净资产收益率(%)
            'XSMLL': 'gross_margin',                 # 毛利率(%)
            
            # 资产负债表
            'TOTAL_ASSETS': 'total_assets',          # 总资产(元)
            'TOTAL_LIABILITIES': 'total_liabilities', # 总负债(元)
            'TOTAL_EQUITY': 'total_equity',          # 股东权益合计(元)
            
            # 利润表
            'TOTAL_OPERATE_INCOME': 'total_revenue', # 总营收(元)
            'TOTAL_PROFIT': 'total_profit',          # 总利润(元)
            'PARENT_NETPROFIT': 'net_profit',        # 净利润(元)
            
            # 现金流量表
            'NETCASH_OPERATE': 'operating_cash_flow',    # 营业性现金流(元)
            'NETCASH_INVEST': 'investing_cash_flow',     # 投资性现金流(元)
            'NETCASH_FINANCE': 'financing_cash_flow'     # 融资性现金流(元)
        }

        # 只保留需要的列
        needed_columns = list(column_mapping.keys())
        df = df[needed_columns]
        
        df = df.rename(columns=column_mapping)
        
        # 处理日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        df['notice_date'] = pd.to_datetime(df['notice_date']).dt.date
        df['update_date'] = pd.to_datetime(df['update_date']).dt.date
        
        # 拆分股票代码和交易所代码
        df['exch_code'] = df['ts_code'].str.split('.').str[1]
        df['ts_code'] = df['ts_code'].str.split('.').str[0]
        
        # 添加更新时间
        df['update_time'] = datetime.now()
        
        return df
        
    except Exception as e:
        print(f"❌ 错误：{ts_code}, {e}")
        return None

def update_fundamental(ts_code):
    """
    更新股票基本面数据
    """
    session = Session()
    try:
        df = get_fundamental_data(ts_code)
        if df is None or df.empty:
            return
            
        # 检查是否已存在数据
        existing_dates = pd.read_sql(
            f"SELECT trade_date FROM stock_fundamental WHERE ts_code = '{ts_code}'",
            engine
        )['trade_date'].tolist()
        
        # 只保留新数据
        df = df[~df['trade_date'].isin(existing_dates)]
        
        if df.empty:
            print(f"⚠️ 无新增数据：{ts_code}")
            return
            
        # 保存到数据库
        df.to_sql('stock_fundamental', con=engine, if_exists='append', index=False)
        print(f"✅ 更新基本面数据：{ts_code}，新增 {len(df)} 条记录")
        time.sleep(0.3)  # 避免频繁请求
        
    except Exception as e:
        print(f"❌ 错误：{ts_code}, {e}")
    finally:
        session.close()

def run():
    df = get_stock_list()
    for ts_code in df["ts_code"]:
        update_fundamental(ts_code[:6])

if __name__ == "__main__":
    run()
    # update_fundamental('603306')