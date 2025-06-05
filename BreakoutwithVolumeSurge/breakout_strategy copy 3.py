import time
from datetime import timedelta

import pandas as pd
from config import MYSQL_URL
from get_realtime import get_realtime_info
from sqlalchemy import create_engine

engine = create_engine(MYSQL_URL)


def check_breakout(ts_code):
    """
    替换为 T+1 异动捕捉策略判断
    """
    sql = """
    SELECT trade_date, open, close, high, low, pre_close, vol
    FROM stock_daily
    WHERE ts_code = %(ts_code)s
    ORDER BY trade_date DESC
    LIMIT 10
    """
    df = pd.read_sql(sql, engine, params={"ts_code": ts_code})

    if len(df) < 5:
        return None

    df = df.sort_values("trade_date").reset_index(drop=True)

    t = df.iloc[-3]     # T，前两天
    t1 = df.iloc[-2]    # T+1，昨日
    t2 = df.iloc[-1]    # T+2，今天（今天跑策略）

    # Step 1: T 涨停 or 涨幅 ≥ 7%
    pct_chg_t = (t["close"] - t["pre_close"]) / t["pre_close"] * 100
    if not (pct_chg_t >= 7 or (t["close"] == t["high"] and t["close"] > t["pre_close"])):
        return None

    # Step 2: T+1 高开低走 + 阴线
    if not (t1["open"] > t1["close"] and t1["close"] < t1["pre_close"]):
        return None

    # Step 3: T+2 没破前低 + 收出反包阳线
    if t2["low"] < t1["low"]:
        return None
    if not (t2["close"] > t1["open"] and t2["close"] > t2["open"]):
        return None

    # Step 4: 放量（今日量 > 5日均量）
    df["avg_vol_5"] = df["vol"].rolling(5).mean()
    if t2["vol"] < df["avg_vol_5"].iloc[-1]:
        return None

    return t2  # 满足条件，返回今天数据


def get_yesterday_close(ts_code, trade_date):
    """
    获取指定股票某交易日前一个交易日的收盘价
    :param ts_code: 股票代码
    :param trade_date: 交易日，格式'yyyymmdd'
    :return: 收盘价或 None
    """
    sql = """
    SELECT close FROM stock_daily
    WHERE ts_code = %(ts_code)s AND trade_date < %(trade_date)s
    ORDER BY trade_date DESC
    LIMIT 1
    """
    df = pd.read_sql(sql, engine, params={"ts_code": ts_code, "trade_date": trade_date})
    if not df.empty:
        return df.iloc[0]["close"]
    return None


def confirm_buy_with_realtime(ts_code, trade_date):
    """
    确认买入：实时价格判断
    - 实时价 > 昨收
    - 实时价 > 今开
    """
    yesterday_close = get_yesterday_close(ts_code, trade_date)
    today_date = (trade_date + timedelta(days=1)).strftime("%Y%m%d")

    try:
        today_info = get_realtime_info(ts_code, today_date)
    except Exception as e:
        print(f"{ts_code} 获取实时行情失败: {e}")
        return False

    today_open = today_info["今开"]
    realtime_price = today_info["当前"]
    print(f"{ts_code} 实时价: {realtime_price}")

    if yesterday_close is None or today_open is None or realtime_price is None:
        print(f"{ts_code} 缺少必要数据，跳过")
        return False

    if today_open < yesterday_close * 0.95:
        print(f"{ts_code} 开盘价跌幅过大，不买入")
        return False

    if realtime_price > today_open and realtime_price > yesterday_close:
        print(f"{ts_code} 实时价格符合买入条件，确认买入")
        return True
    else:
        print(f"{ts_code} 实时价格未满足条件，放弃买入")
        return False