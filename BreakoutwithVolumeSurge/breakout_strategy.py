import time
from datetime import timedelta

import pandas as pd
from config import MYSQL_URL
from get_realtime import get_realtime_info
from sqlalchemy import create_engine

engine = create_engine(MYSQL_URL)


def check_breakout(ts_code):
    sql = """
    SELECT trade_date, open, close, pre_close, vol
    FROM stock_daily
    WHERE ts_code = %(ts_code)s
    ORDER BY trade_date DESC
    LIMIT 60
    """
    df = pd.read_sql(sql, engine, params={"ts_code": ts_code})

    if len(df) < 30:
        return None

    df = df.sort_values("trade_date").reset_index(drop=True)

    # 计算涨跌幅、成交量均值、20日最高收盘价
    df["pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100
    df["avg_vol_5"] = df["vol"].rolling(window=5).mean()
    df["max_close_20"] = df["close"].rolling(window=20).max()

    # 均线排列确认：连续 3 天多头排列（ma5 > ma10 > ma20）
    df["ma5"] = df["close"].rolling(5).mean()
    df["ma10"] = df["close"].rolling(10).mean()
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma_bullish"] = (df["ma5"] > df["ma10"]) & (df["ma10"] > df["ma20"])
    ma_bullish = df["ma_bullish"].iloc[-3:].all()

    # 最新一天的数据
    last = df.iloc[-1]

    # 突破条件判断
    volume_price_breakout = (
        last["pct_chg"] >= 5
        and last["vol"] >= 2 * last["avg_vol_5"]
        and last["close"] >= last["max_close_20"]
        and last["close"] > last["open"]  # 防止冲高回落
    )

    if volume_price_breakout and ma_bullish:
        return last

    return None


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
    简化版：只通过一次实时价格判断是否确认买入
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
        # 更强势，价格继续创新高或站稳昨日高点
        print(f"{ts_code} 实时价格符合买入条件，确认买入")
        return True
    else:
        print(f"{ts_code} 实时价格未满足条件，放弃买入")
        return False
