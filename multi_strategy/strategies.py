import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(sys.path)

from typing import List

import pandas as pd
from config import MYSQL_URL
from sqlalchemy import create_engine

from utils.logger import logger

engine = create_engine(MYSQL_URL)


def strategy_check_breakout_batch(trade_date: str):
    """
    策略1（优化版）
    选出满足突破+放量+涨幅≥5%的股票列表，并增加连续开盘强势条件
    适合 T+1 卖出
    原因：
        •	涨幅 ≥5% 且放量突破，通常是短线资金介入，次日有惯性。
        •	突破后，次日惯性冲高概率大，可以 T+1 卖出止盈。
        •	连续开盘价高于前日收盘价，表明资金持续看好，上涨动能更强。
    """
    sql = """
    SELECT ts_code, trade_date, open, close, pre_close, vol
    FROM stock_daily
    WHERE trade_date <= %(date)s
    ORDER BY ts_code, trade_date DESC
    """
    try:
        df_all = pd.read_sql(sql, engine, params={"date": trade_date})
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

        # 新增：检查连续开盘强势条件
        # 检查最近3天的开盘价是否都高于前一天的收盘价
        df["open_above_prev_close"] = df["open"] > df["close"].shift(1)

        last = df.iloc[-1]
        ma_bullish = df["ma_bullish"].iloc[-3:].all()

        # 检查最近3天的开盘强势情况
        recent_open_strength = df["open_above_prev_close"].iloc[-3:].all()

        volume_price_breakout = (
            last["pct_chg"] >= 5
            and last["vol"] >= 2 * last["avg_vol_5"]
            and last["close"] >= last["max_close_20"]
            and last["close"] > last["open"]
        )

        # 增加开盘强势条件
        if volume_price_breakout and ma_bullish and recent_open_strength:
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

        if last["close"] > max_high_10 and last["vol"] > 1.5 * last["avg_vol_5"] and pct_change > 4:
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
        •	MACD 金叉是趋势启动的信号，属于"中短线趋势追踪"策略。
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


def strategy_consolidation_breakout_preparation(trade_date: str) -> pd.DataFrame:
    """
    策略6：缩量整理后放量突破预备
    技术指标：
        • 最近10日振幅小于5%，成交量逐步缩小
        • 最新一日放量，突破前高
    持有期：3-5天
    """
    sql = """
    SELECT ts_code, trade_date, close, high, low, vol
    FROM stock_daily
    WHERE trade_date <= %(trade_date)s
    """
    df = pd.read_sql(sql, engine, params={"trade_date": trade_date})
    df = df.sort_values(["ts_code", "trade_date"])

    result = []
    for ts_code, group in df.groupby("ts_code"):
        group = group.tail(15).copy()
        if len(group) < 11:
            continue

        group["amplitude"] = (group["high"] - group["low"]) / group["low"] * 100
        consolidation = group.iloc[-11:-1]
        breakout = group.iloc[-1]

        amplitude_max = consolidation["amplitude"].max()
        amplitude_mean = consolidation["amplitude"].mean()
        vol_mean = consolidation["vol"].mean()
        volume_ratio = breakout["vol"] / vol_mean if vol_mean > 0 else 0
        breakout_level = consolidation["high"].max()

        if (
            amplitude_max < 5
            and amplitude_mean < 3.5
            and consolidation["vol"].iloc[-1] < consolidation["vol"].max()
            and volume_ratio > 1.5
            and breakout["close"] > breakout_level
        ):
            result.append(
                {
                    "ts_code": ts_code,
                    "strategy": "consolidation_breakout_pre",
                    "close": breakout["close"],
                    "volume_ratio": round(volume_ratio, 2),
                    "breakout_pct": round((breakout["close"] - breakout_level) / breakout_level * 100, 2),
                    "amplitude_mean": round(amplitude_mean, 2),
                }
            )

    return pd.DataFrame(result)


def strategy_box_bottom_rebound(trade_date: str):
    """
    策略7：箱体底部反弹
    技术指标：
        • 近20日价格维持箱体
        • 最近一天收盘价接近最低点
    持有期：3-5天
    """
    sql = """
    SELECT ts_code, trade_date, close
    FROM stock_daily
    WHERE trade_date <= %(trade_date)s
    """
    df = pd.read_sql(sql, engine, params={"trade_date": trade_date})
    df = df.sort_values(["ts_code", "trade_date"])

    result = []
    for ts_code, group in df.groupby("ts_code"):
        group = group.tail(21).copy()
        if len(group) < 20:
            continue

        close_min = group["close"].min()
        close_max = group["close"].max()
        last_close = group.iloc[-1]["close"]
        box_width = close_max - close_min
        box_width_pct = box_width / close_min * 100 if close_min > 0 else 0
        close_position_pct = (last_close - close_min) / box_width * 100 if box_width > 0 else 100

        # 条件1：振幅小于15%，视为箱体震荡
        # 条件2：当前价格接近箱体底部，处于低于20%的位置
        if box_width_pct < 15 and close_position_pct <= 20:
            result.append(
                {
                    "ts_code": ts_code,
                    "strategy": "box_bottom_rebound",
                    "close": last_close,
                    "box_width_pct": round(box_width_pct, 2),
                    "close_position_pct": round(close_position_pct, 2),
                }
            )

    return pd.DataFrame(result)


def strategy_ma_convergence_start(trade_date: str):
    """
    策略8：均线粘合预启动
    技术指标：
        • MA5、MA10、MA20粘合，且MA5上拐
    持有期：3-5天
    """
    sql = """
    SELECT ts_code, trade_date, close
    FROM stock_daily
    WHERE trade_date <= %(trade_date)s
    """

    df = pd.read_sql(sql, engine, params={"trade_date": trade_date})
    df = df.sort_values(["ts_code", "trade_date"])

    result = []
    for ts_code, group in df.groupby("ts_code"):
        group = group.tail(25).copy()
        if len(group) < 20:
            continue

        group["ma5"] = group["close"].rolling(5).mean()
        group["ma10"] = group["close"].rolling(10).mean()
        group["ma20"] = group["close"].rolling(20).mean()

        last = group.iloc[-1]
        prev = group.iloc[-2]
        if pd.isna(last["ma5"]) or pd.isna(last["ma10"]) or pd.isna(last["ma20"]):
            continue

        # 粘合度判断
        ma5_10_diff = abs(last["ma5"] - last["ma10"]) / last["close"]
        ma10_20_diff = abs(last["ma10"] - last["ma20"]) / last["close"]
        ma5_slope = last["ma5"] - prev["ma5"]

        if ma5_10_diff < 0.015 and ma10_20_diff < 0.015 and ma5_slope > 0:
            result.append(
                {
                    "ts_code": ts_code,
                    "strategy": "ma_convergence_start",
                    "ma5_10_diff_pct": round(ma5_10_diff * 100, 2),
                    "ma10_20_diff_pct": round(ma10_20_diff * 100, 2),
                    "ma5_slope": round(ma5_slope, 3),
                    "close": last["close"],
                }
            )

    return pd.DataFrame(result)


def strategy_macd_divergent_gold_cross(trade_date: str):
    """
    策略9：MACD金叉背离
    技术指标：
        • MACD低位金叉，且价格不创新低
    持有期：3-5天
    """
    sql = """
    SELECT ts_code, trade_date, close
    FROM stock_daily
    WHERE trade_date <= %(trade_date)s
    """

    df = pd.read_sql(sql, engine, params={"trade_date": trade_date})
    df = df.sort_values(["ts_code", "trade_date"])

    result = []
    for ts_code, group in df.groupby("ts_code"):
        group = group.tail(50).copy()
        if len(group) < 35:
            continue

        group["ema12"] = group["close"].ewm(span=12, adjust=False).mean()
        group["ema26"] = group["close"].ewm(span=26, adjust=False).mean()
        group["diff"] = group["ema12"] - group["ema26"]
        group["dea"] = group["diff"].ewm(span=9, adjust=False).mean()
        group["macd"] = 2 * (group["diff"] - group["dea"])

        last = group.iloc[-1]
        prev = group.iloc[-2]

        # 金叉 & 价格背离判定
        macd_cross = prev["macd"] < 0 and last["macd"] > 0
        recent_lows = group["close"].iloc[-20:-1]
        price_divergence = last["close"] > recent_lows.min() * 1.02  # 背离确认加容忍度

        # MACD低位确认（避免顶部金叉）
        macd_low_zone = last["macd"] < 0.1 and last["macd"] > -0.3

        if macd_cross and price_divergence and macd_low_zone:
            result.append(
                {
                    "ts_code": ts_code,
                    "strategy": "macd_divergence",
                    "macd": round(last["macd"], 4),
                    "diff": round(last["diff"], 4),
                    "dea": round(last["dea"], 4),
                    "min_recent_low": round(recent_lows.min(), 2),
                    "last_close": round(last["close"], 2),
                }
            )

    return pd.DataFrame(result)


def strategy_annual_line_breakout(trade_date: str):
    """
    策略10：低位放量突破年线
    技术指标：
        • 当前收盘价首次上穿年线
        • 当前放量 > 5日均量1.5倍
    持有期：3-5天
    """
    sql = """
    SELECT ts_code, trade_date, close, vol
    FROM stock_daily
    WHERE trade_date <= %(trade_date)s
    """

    df = pd.read_sql(sql, engine, params={"trade_date": trade_date})
    df = df.sort_values(["ts_code", "trade_date"])

    result = []
    for ts_code, group in df.groupby("ts_code"):
        if len(group) < 260:
            continue
        group = group.tail(260).copy()

        group["ma250"] = group["close"].rolling(250).mean()
        group["vol5"] = group["vol"].rolling(5).mean()

        last = group.iloc[-1]
        prev = group.iloc[-2]

        # 判断是否首次突破年线
        below_ma = group["close"] < group["ma250"]
        crossed_today = prev["close"] < prev["ma250"] and last["close"] > last["ma250"]
        first_cross = crossed_today and below_ma.iloc[:-1].all()

        # 放量突破判断
        vol_breakout = last["vol"] > 1.5 * last["vol5"]

        if first_cross and vol_breakout:
            result.append(
                {
                    "ts_code": ts_code,
                    "strategy": "annual_line_breakout",
                    "last_close": round(last["close"], 2),
                    "ma250": round(last["ma250"], 2),
                    "vol": int(last["vol"]),
                    "vol5": int(last["vol5"]),
                }
            )

    return pd.DataFrame(result)


# 策略注册表
ALL_STRATEGIES = [
    strategy_check_breakout_batch,
    strategy_top_gainers,
    strategy_plate_breakout_post_close,
    # 将长线暂停
    # strategy_macd_golden_cross,
    # strategy_first_limit_up_low_position,
    # 新增5个潜伏类策略
    strategy_consolidation_breakout_preparation,
    strategy_box_bottom_rebound,
    strategy_ma_convergence_start,
    strategy_macd_divergent_gold_cross,
    strategy_annual_line_breakout,
]


def filter_by_fundamentals(df: pd.DataFrame) -> pd.DataFrame:
    """
    根据基本面指标过滤股票列表。
    筛选条件示例：
        - ROE ≥ 5%
        - EPS > 0
        - 净利润同比增长率 ≥ -10%
        - 负债率 ≤ 70%
        - 经营性现金流为正
        - 营业收入同比增长率 ≥ 10%
        - 毛利率稳定或上升
        - 研发投入占比 ≥ 5%
    """
    # 获取每只股票最新的基本面数据
    ts_codes = df["ts_code"].unique().tolist()
    sql = """
        SELECT sf.ts_code,
               sf.trade_date,
               sf.roe,
               sf.eps,
               sf.profit_yoy,
               sf.revenue_yoy,
               sf.gross_margin,
               sf.total_liabilities,
               sf.total_assets,
               sf.operating_cash_flow,
               sf.total_revenue,
               sf.total_profit,
               sf.net_profit,
               sf.investing_cash_flow,
               sf.financing_cash_flow
        FROM stock_fundamental sf
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_date
            FROM stock_fundamental
            GROUP BY ts_code
        ) latest
        ON sf.ts_code = latest.ts_code AND sf.trade_date = latest.max_date
        WHERE sf.ts_code IN %(ts_codes)s
    """
    fundamental_df = pd.read_sql(sql, engine, params={"ts_codes": tuple(ts_codes)})

    # 计算负债率（%）
    fundamental_df["debt_ratio"] = fundamental_df["total_liabilities"] / fundamental_df["total_assets"] * 100

    # 合并基本面数据
    df = df.merge(fundamental_df, on="ts_code", how="left")

    # 应用筛选条件
    filtered_df = df[
        (df["roe"] >= 5)
        & (df["eps"] > 0)
        & (df["profit_yoy"] >= -10)
        & (df["debt_ratio"] <= 70)
        & (df["operating_cash_flow"] > 0)
        & (df["revenue_yoy"] >= 10)
        & (df["gross_margin"].notnull())  # 假设毛利率稳定或上升的判断需要历史数据，这里仅示例
        # 研发投入占比字段未在提供的表结构中，需根据实际字段添加
    ]

    return filtered_df
