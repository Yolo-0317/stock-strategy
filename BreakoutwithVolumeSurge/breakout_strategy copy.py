import time

import pandas as pd
from config import MYSQL_URL
from get_realtime import get_realtime_info
from sqlalchemy import create_engine

engine = create_engine(MYSQL_URL)


def check_breakout(ts_code):
    """
    检查指定股票是否满足量价突破买入信号条件
    :param ts_code: 股票代码，如 '300001'
    :return: 满足信号时返回最近一天的行情数据，否则返回 None
    """
    sql = """
    SELECT trade_date, open, close, pre_close, vol
    FROM stock_daily
    WHERE ts_code = %(ts_code)s
    ORDER BY trade_date DESC
    LIMIT 60
    """
    # 从数据库读取近60个交易日数据
    df = pd.read_sql(sql, engine, params={"ts_code": ts_code})

    if len(df) < 30:
        # 数据不足30天无法判断，返回None
        return None

    df = df.sort_values("trade_date").reset_index(drop=True)

    # 计算每日涨跌幅(%)
    df["pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100
    # 计算5日平均成交量
    df["avg_vol_5"] = df["vol"].rolling(window=5).mean()
    # 计算20日最高收盘价
    df["max_close_20"] = df["close"].rolling(window=20).max()

    # 计算MACD指标（DIF、DEA）
    ema12 = df["close"].ewm(span=12).mean()
    ema26 = df["close"].ewm(span=26).mean()
    df["dif"] = ema12 - ema26
    df["dea"] = df["dif"].ewm(span=9).mean()
    # 判断MACD金叉（DIF线由下向上穿过DEA线，且DIF>0）
    macd_cross = (
        df["dif"].iloc[-2] < df["dea"].iloc[-2] and df["dif"].iloc[-1] > df["dea"].iloc[-1] and df["dif"].iloc[-1] > 0
    )

    # 计算5、10、20日均线并判断多头排列
    df["ma5"] = df["close"].rolling(5).mean()
    df["ma10"] = df["close"].rolling(10).mean()
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma_bullish"] = (df["ma5"] > df["ma10"]) & (df["ma10"] > df["ma20"])
    # 连续3天均线多头排列
    ma_bullish = df["ma_bullish"].iloc[-3:].all()

    last = df.iloc[-1]
    # 判断量价突破条件：涨幅>=5%，成交量>=5日均量2倍，收盘价创20日新高且收盘价大于开盘价
    volume_price_breakout = (
        last["pct_chg"] >= 5
        and last["vol"] >= 2 * last["avg_vol_5"]
        and last["close"] >= last["max_close_20"]
        and last["close"] > last["open"]
    )

    if volume_price_breakout and macd_cross and ma_bullish:
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
    根据买入信号产生日的行情数据，结合实时行情轮询确认买入时机
    :param ts_code: 股票代码
    :param trade_date: 买入信号产生日，格式 'yyyymmdd'
    :return: 是否确认买入（True/False）
    """
    yesterday_close = get_yesterday_close(ts_code, trade_date)
    # 简单计算下一交易日为买入日（这里加1，真实情况需用交易日历）
    today_date = str(int(trade_date) + 1)

    try:
        # 获取买入日开盘价（通过实时接口）
        today_info = get_realtime_info(ts_code, today_date)
    except Exception as e:
        print(f"{ts_code} 获取实时行情失败: {e}")
        return False

    today_open = today_info["今开"]
    if yesterday_close is None or today_open is None:
        print(f"{ts_code} 缺少必要行情数据，跳过")
        return False

    # 开盘价跌破昨日收盘价95%则不买入
    if today_open < yesterday_close * 0.95:
        print(f"{ts_code} T日开盘价跌幅过大，不买入")
        return False

    # 轮询10次（每次间隔1分钟）实时价，判断是否满足开盘价的95%
    for i in range(10):
        try:
            realtime_info = get_realtime_info(ts_code, today_date)
            realtime_price = realtime_info["当前"]
            print(f"[{i+1}/10] {ts_code} 实时价: {realtime_price}")

            if realtime_price >= today_open * 0.95:
                print(f"{ts_code} 实时价格符合买入条件，确认买入")
                return True
        except Exception as e:
            print(f"{ts_code} 第{i+1}次获取实时行情失败: {e}")

        time.sleep(60)

    print(f"{ts_code} 未满足实时价格买入条件，放弃买入")
    return False
