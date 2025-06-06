import os
import sys

sys.path.insert(0, os.path.abspath("../"))

import time
from datetime import datetime, timedelta

import pandas as pd
import schedule
from config import MYSQL_URL
from get_realtime import get_realtime_info

# 加载表元信息
from models import StockDaily  # 假设你已定义 ORM 类
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from strategies import ALL_STRATEGIES

from utils.logger import logger

engine = create_engine(MYSQL_URL)


def get_yesterday_close(ts_code, trade_date):
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


def record_realtime_ticks(trade_date: str):
    df = pd.read_csv(f"confirmed_stocks/confirmed_stocks_{trade_date}.csv")
    ts_codes = df["股票代码"].tolist()
    now = datetime.now()

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for ts_code in ts_codes:
            try:
                info = get_realtime_info(ts_code, trade_date)
                if not info:
                    continue

                session.execute(
                    text(
                        """
                    INSERT IGNORE INTO realtime_ticks 
                    (ts_code, trade_date, timestamp, price, volume, amount, high, low, open, close)
                    VALUES 
                    (:ts_code, :trade_date, :timestamp, :price, :volume, :amount, :high, :low, :open, :close)
                    """
                    ),
                    {
                        "ts_code": ts_code,
                        "trade_date": trade_date,
                        "timestamp": now,
                        "price": info.get("当前"),
                        "volume": info.get("成交量"),
                        "amount": info.get("成交额"),
                        "high": info.get("最高"),
                        "low": info.get("最低"),
                        "open": info.get("今开"),
                        "close": info.get("昨收"),
                    },
                )
            except Exception as e:
                print(f"[{ts_code}] 记录实时数据失败: {e}")

        session.commit()  # ⚠️ 记得提交事务
    except Exception as e:
        session.rollback()
        print(f"执行过程中出现错误，已回滚: {e}")
    finally:
        session.close()


def is_rising_in_recent_ticks(ts_code: str, minutes: int = 5) -> bool:
    since = datetime.now() - timedelta(minutes=minutes)
    sql = """
    SELECT timestamp, price FROM realtime_ticks
    WHERE ts_code = %(ts_code)s AND timestamp >= %(since)s
    ORDER BY timestamp ASC
    """
    df = pd.read_sql(sql, engine, params={"ts_code": ts_code, "since": since})
    if len(df) < 3:
        logger.info(f"{ts_code} 最近 {minutes} 分钟数据不足，无法判断是否上涨")
        return False

    # 连续上涨的判断：最后价格比最前高，且中间不能大幅回落
    increasing = all(df["price"].iloc[i] <= df["price"].iloc[i + 1] for i in range(len(df) - 1))
    return increasing


def get_pct_change_in_last_n_minutes(ts_code: str, minutes: int) -> float:
    """
    获取最近 N 分钟涨幅
    """
    now = datetime.now()
    start_time = now - timedelta(minutes=minutes)
    df = pd.read_sql(
        """
        SELECT timestamp, price
        FROM realtime_ticks
        WHERE ts_code = %s AND timestamp >= %s
        ORDER BY timestamp ASC
        """,
        con=engine,
        params=(ts_code, start_time),
    )

    if df.empty or len(df) < 2:
        return 0.0

    start_price = df.iloc[0]["price"]
    end_price = df.iloc[-1]["price"]
    if start_price == 0:
        return 0.0

    return (end_price - start_price) / start_price * 100


def get_platform_breakout_price(ts_code: str, trade_date: str, window: int = 20) -> float | None:
    """
    获取某只股票最近window日内的最高价（即平台突破参考价）
    """
    sql = """
    SELECT MAX(high) as breakout_price
    FROM stock_daily
    WHERE ts_code = %(ts_code)s AND trade_date < %(trade_date)s
    ORDER BY trade_date DESC
    LIMIT %(window)s
    """
    df = pd.read_sql(
        sql,
        engine,
        params={"ts_code": ts_code, "trade_date": trade_date, "window": window},
    )
    if not df.empty and df.iloc[0]["breakout_price"]:
        return df.iloc[0]["breakout_price"]
    return None


def get_volume_ratio(ts_code: str, current_minutes: int, compare_minutes: int) -> float:
    """
    成交量放大倍数（当前 N 分钟均量 vs 前 M 分钟均量）
    """
    now = datetime.now()
    current_start = now - timedelta(minutes=current_minutes)
    compare_start = now - timedelta(minutes=current_minutes + compare_minutes)

    # 当前时间段
    df_current = pd.read_sql(
        """
        SELECT volume
        FROM realtime_ticks
        WHERE ts_code = %s AND timestamp >= %s
        """,
        con=engine,
        params=(ts_code, current_start),
    )

    # 比较时间段
    df_compare = pd.read_sql(
        """
        SELECT volume
        FROM realtime_ticks
        WHERE ts_code = %s AND timestamp >= %s AND timestamp < %s
        """,
        con=engine,
        params=(ts_code, compare_start, current_start),
    )

    if df_current.empty or df_compare.empty:
        return 1.0  # 无数据视为未放量

    current_avg = df_current["volume"].mean()
    compare_avg = df_compare["volume"].mean()

    if compare_avg == 0:
        return 1.0

    return current_avg / compare_avg


def is_kline_up_trending(ts_code: str, minutes: int) -> bool:
    """
    即时 K 线趋势识别（判断是否上涨趋势）
    """
    now = datetime.now()
    start_time = now - timedelta(minutes=minutes)

    df = pd.read_sql(
        """
        SELECT timestamp, price
        FROM realtime_ticks
        WHERE ts_code = %s AND timestamp >= %s
        ORDER BY timestamp ASC
        """,
        con=engine,
        params=(ts_code, start_time),
    )

    if df.empty or len(df) < 3:
        return False

    # 简单趋势判断：至少 2 次连续上升
    price_series = df["price"].tolist()
    up_trend_count = sum(price_series[i] < price_series[i + 1] for i in range(len(price_series) - 1))

    return up_trend_count >= len(price_series) // 2  # 超过一半为上涨则认为趋势向上


def confirm_buy_with_realtime(ts_code: str, trade_date: str) -> bool:
    """
    实时行情确认是否满足买入条件。
    用于在前期选股基础上，实时判断是否值得进场。
    """

    # 获取昨收价、今日实时行情
    yesterday_close = get_yesterday_close(ts_code, trade_date)
    try:
        today_info = get_realtime_info(ts_code, trade_date)
    except Exception as e:
        logger.warning(f"⚠️{ts_code} 获取实时行情失败: {e}")
        return False

    today_open = today_info.get("今开")
    realtime_price = today_info.get("当前")
    today_high = today_info.get("最高")

    # 基础校验
    if any(x is None for x in [yesterday_close, today_open, realtime_price, today_high]):
        logger.info(f"{ts_code} 缺少必要数据，跳过")
        return False

    # 条件1：避免开盘暴跌（情绪或利空）
    if today_open < yesterday_close * 0.95:
        # logger.info(f"{ts_code} 开盘跌幅过大（今开 < 昨收 * 0.95），不买入")
        return False

    # 条件2：当前价格必须高于“今开”和“昨收” —— 趋势向上确认
    if realtime_price <= today_open or realtime_price <= yesterday_close:
        # logger.info(f"{ts_code} 实时价未高于今开和昨收，不买入")
        return False

    # 计算日内价格区间及实时价位置
    price_range = today_high - today_open
    if price_range <= 0:
        # logger.info(f"{ts_code} 今开 >= 最高，行情异常，不买入")
        return False

    position = (realtime_price - today_open) / price_range
    pct_change = get_pct_change_in_last_n_minutes(ts_code, 5)

    # 条件3+4：实时动能评分（满分 2，至少得1分）
    score = 0
    if position >= 0.6:
        score += 1
    else:
        logger.info(f"{ts_code} 当前价位置较低: {position:.2%}")

    if pct_change >= 0.5:
        score += 1
    else:
        logger.info(f"{ts_code} 5分钟涨幅较小: {pct_change:.2f}%")

    if score < 1:
        logger.info(f"{ts_code} 实时动能不足，不买入")
        return False
    else:
        logger.info(f"{ts_code} 满足实时动能买入条件，position={position:.2%}, pct_change={pct_change:.2f}%")

    # 条件5：近5分钟是否持续上涨（tick级别）
    if not is_rising_in_recent_ticks(ts_code, minutes=5):
        logger.info(f"{ts_code} 最近5分钟没有持续上涨信号，谨慎不买入")
        return False

    # 条件6：成交量放大（资金确认）
    vol_ratio = get_volume_ratio(ts_code, 5, 20)
    if vol_ratio < 2.0:
        logger.info(f"{ts_code} 成交量未明显放大（倍数: {vol_ratio:.2f}），不买入")
        # return False  # 可选放宽此限制

    # 条件7：K线趋势判断（走势确认）
    if not is_kline_up_trending(ts_code, 5):
        logger.info(f"{ts_code} K线走势不明显向上，谨慎不买入")
        return False

    # 条件8：平台突破价限制，避免追高
    breakout_price = get_platform_breakout_price(ts_code, trade_date)
    if breakout_price and realtime_price > breakout_price * 1.08:
        logger.info(f"{ts_code} 当前价格 ({realtime_price:.2f}) 已远离平台突破价 ({breakout_price:.2f})，追高风险大，不买入")
        return False

    # 所有主要条件满足
    logger.info(f"{ts_code} 满足所有实时买入条件 + 连续上涨，确认买入")
    logger.info(f"{ts_code} 实时价: {realtime_price}，今开: {today_open}，昨收: {yesterday_close}，最高: {today_high}")
    return True
