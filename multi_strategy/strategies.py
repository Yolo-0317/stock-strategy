import os
import sys

sys.path.insert(0, os.path.abspath("../"))

from typing import List

import pandas as pd
from config import MYSQL_URL
from sqlalchemy import create_engine

from utils.logger import logger

engine = create_engine(MYSQL_URL)


def strategy_check_breakout_batch(trade_date: str):
    """
    策略1
    选出满足突破+放量+涨幅≥5%的股票列表
    适合 T+1 卖出
    原因：
        •	涨幅 ≥5% 且放量突破，通常是短线资金介入，次日有惯性。
        •	突破后，次日惯性冲高概率大，可以 T+1 卖出止盈。
    """
    sql = """
    SELECT ts_code, trade_date, open, close, pre_close, vol
    FROM stock_daily
    WHERE trade_date <= %(date)s
    ORDER BY ts_code, trade_date DESC
    """
    try:
        df_all = pd.read_sql(sql, engine, params={"date": trade_date})
        logger.info("成功从数据库获取数据")
    except Exception as e:
        logger.error(f"数据库查询失败: {e}")
        return pd.DataFrame()

    # 按股票分组分析
    results = []

    grouped = df_all.groupby("ts_code")

    for ts_code, df in grouped:
        df = df.sort_values("trade_date").reset_index(drop=True)
        if len(df) < 30:
            continue

        df["pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100
        df["avg_vol_5"] = df["vol"].rolling(window=5).mean()
        df["max_close_20"] = df["close"].rolling(window=20).max()
        df["ma5"] = df["close"].rolling(5).mean()
        df["ma10"] = df["close"].rolling(10).mean()
        df["ma20"] = df["close"].rolling(20).mean()
        df["ma_bullish"] = (df["ma5"] > df["ma10"]) & (df["ma10"] > df["ma20"])

        last = df.iloc[-1]
        ma_bullish = df["ma_bullish"].iloc[-3:].all()

        volume_price_breakout = (
            last["pct_chg"] >= 5
            and last["vol"] >= 2 * last["avg_vol_5"]
            and last["close"] >= last["max_close_20"]
            and last["close"] > last["open"]
        )

        if volume_price_breakout and ma_bullish:
            results.append({"ts_code": ts_code})

    return pd.DataFrame(results)


def strategy_top_gainers(trade_date: str) -> pd.DataFrame:
    """
    策略2：
    涨幅前5% + 成交额大 + 放量
        •	逻辑：
        •	选取当日成交额 > 1亿的股票。
        •	成交量是前一日的1.5倍以上。
        •	从中提取涨幅前5%的个股。
    适合 T+1 卖出
        •	原因：
        •	涨幅前5%、放量+成交额大，往往为主力资金进场。
        •	次日有短线资金跟风炒作预期，适合次日冲高卖出。
    """
    sql_today = """
    SELECT ts_code, trade_date, close, pre_close, vol, amount
    FROM stock_daily
    WHERE trade_date = %(trade_date)s
    """
    df_today = pd.read_sql(sql_today, engine, params={"trade_date": trade_date})
    df_today["pct_chg"] = (df_today["close"] - df_today["pre_close"]) / df_today["pre_close"] * 100

    sql_prev = """
    SELECT ts_code, vol
    FROM stock_daily
    WHERE trade_date = (
        SELECT MAX(trade_date) FROM stock_daily
        WHERE trade_date < %(trade_date)s
    )
    """
    df_prev = pd.read_sql(sql_prev, engine, params={"trade_date": trade_date})
    df_prev.rename(columns={"vol": "vol_prev"}, inplace=True)

    df = pd.merge(df_today, df_prev, on="ts_code")
    df["vol_increase_ratio"] = df["vol"] / df["vol_prev"]
    df = df[(df["amount"] >= 1e4) & (df["vol_increase_ratio"] >= 1.5)]

    top5pct = int(len(df) * 0.05)
    df = df.sort_values("pct_chg", ascending=False).head(top5pct)

    return df.reset_index(drop=True)


def strategy_plate_breakout_post_close(trade_date: str) -> pd.DataFrame:
    """
    策略3：平台突破 + 放量启动（盘后版）
    这里的今日就是最近的已结束的交易日
    逻辑条件：
        1. 过去10日高点构成平台
        2. 今日收盘价突破10日最高价
        3. 今日涨幅 > 4%
        4. 今日成交量 > 过去5日均量的1.5倍
    """
    sql = """
    SELECT ts_code, trade_date, close, high, vol
    FROM stock_daily
    WHERE trade_date <= %(trade_date)s
    ORDER BY ts_code, trade_date
    """
    df = pd.read_sql(sql, engine, params={"trade_date": trade_date})
    df = df.sort_values(["ts_code", "trade_date"])

    result = []
    for ts_code, group in df.groupby("ts_code"):
        group = group.tail(20).copy()

        if len(group) < 11:
            continue

        group["avg_vol_5"] = group["vol"].rolling(5).mean()

        last = group.iloc[-1]
        prev = group.iloc[-2]
        max_high_10 = group["high"].iloc[:-1].max()

        pct_change = (last["close"] - prev["close"]) / prev["close"] * 100

        if (
            last["close"] > max_high_10 and
            last["vol"] > 1.5 * last["avg_vol_5"] and
            pct_change > 4
        ):
            result.append({"ts_code": ts_code, "strategy": "plate_breakout_post_close"})

    return pd.DataFrame(result)


def strategy_macd_golden_cross(trade_date: str) -> pd.DataFrame:
    """
    策略4：MACD金叉 + 均线上行（5日 > 10日 > 20日）
    适合：趋势刚启动阶段买入
        •	DIF 上穿 DEA（MACD 金叉）
        •	当前收盘价 > MA5、MA10、MA20（多头排列）
        •	可配合量能判断（MACD 低位金叉更佳）
    不建议 T+1 卖出，适合持有 3~5 天以上
        •	原因：
        •	MACD 金叉是趋势启动的信号，属于“中短线趋势追踪”策略。
        •	T+1 卖出太早，可能吃不到主升浪的肉。
        •	建议配合移动止盈或持有若干天后再择机卖出。
    """
    sql = """
    SELECT ts_code, trade_date, close
    FROM stock_daily
    WHERE trade_date <= %(trade_date)s
    ORDER BY ts_code, trade_date
    """
    df = pd.read_sql(sql, engine, params={"trade_date": trade_date})
    df = df.sort_values(["ts_code", "trade_date"])

    result = []
    for ts_code, group in df.groupby("ts_code"):
        group = group.tail(35).copy()
        if len(group) < 26:
            continue

        # 计算EMA和MACD
        group["ema12"] = group["close"].ewm(span=12).mean()
        group["ema26"] = group["close"].ewm(span=26).mean()
        group["diff"] = group["ema12"] - group["ema26"]
        group["dea"] = group["diff"].ewm(span=9).mean()
        group["macd"] = 2 * (group["diff"] - group["dea"])

        # 计算均线
        group["ma5"] = group["close"].rolling(5).mean()
        group["ma10"] = group["close"].rolling(10).mean()
        group["ma20"] = group["close"].rolling(20).mean()

        last = group.iloc[-1]
        prev = group.iloc[-2]

        macd_cross = prev["macd"] < 0 and last["macd"] > 0  # 金叉
        ma_upward = last["ma5"] > last["ma10"] > last["ma20"]

        if macd_cross and ma_upward:
            result.append({"ts_code": ts_code, "strategy": "macd_golden_cross"})
    return pd.DataFrame(result)


def strategy_first_limit_up_low_position(trade_date: str) -> pd.DataFrame:
    """
    策略5：低位涨停首板
    适合：捕捉主力打板启动的第一波
        •	最近60日未涨停
        •	当日涨停（收盘价接近涨停价）
        •	成交量是5日均量的2倍，表明非一字板而是有效换手
    不稳定，T+1 卖出风险较高
        •	原因：
        •	次日走势有较大不确定性：有的直接一字连板，有的直接低开调整。
        •	如果非一字板放量，T+1 开盘冲高出货是可行的，但风险波动较大。
        •	建议视次日开盘盘口和换手率决定是否 T+1 卖出。
    """
    sql = """
    SELECT ts_code, trade_date, close, pre_close, high
    FROM stock_daily
    WHERE trade_date <= %(trade_date)s
    ORDER BY ts_code, trade_date
    """
    df = pd.read_sql(sql, engine, params={"trade_date": trade_date})
    df = df.sort_values(["ts_code", "trade_date"])

    result = []
    for ts_code, group in df.groupby("ts_code"):
        group = group.tail(40).copy()
        if len(group) < 20:
            continue
        group["pct_chg"] = (group["close"] - group["pre_close"]) / group["pre_close"] * 100

        last = group.iloc[-1]
        if last["pct_chg"] < 9.5:  # 未到涨停
            continue

        prev_limit_ups = group.iloc[:-1]["pct_chg"] > 9.5
        if prev_limit_ups.any():
            continue  # 排除已经涨停过的

        max_close_30 = group.iloc[:-1]["close"].max()
        low_position = last["close"] < max_close_30 * 0.8

        if low_position:
            result.append({"ts_code": ts_code, "strategy": "first_limit_up_low"})
    return pd.DataFrame(result)


# 策略注册表
ALL_STRATEGIES = [
    strategy_check_breakout_batch,
    strategy_top_gainers,
    strategy_plate_breakout_post_close,
    # 将长线暂停
    # strategy_macd_golden_cross,
    # strategy_first_limit_up_low_position,
]
