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
    策略1 放宽版：
    - 技术面放宽：
        • 涨幅 ≥ 3% （降低门槛，允许略小涨幅）
        • 成交量 ≥ 1.5倍5日均量 （降低放量要求）
        • 收盘价达到过去20日最高价的98%以上（允许略低于最高价）
        • 收盘价高于开盘价（当天上涨）
        • MA多头排列连续2天（减少天数要求）
    - 基本面放宽：
        • ROE > 10%
        • 毛利率 > 15%
        • 营收同比增长 > 10%
        • 净利润同比增长 > 15%
        • 经营性现金流净额 > 0

    适合 T+1 卖出，适当增加股票池数量，提高策略的包容性。
    """

    sql = """
    WITH daily_data AS (
        SELECT ts_code, trade_date, open, close, pre_close, vol
        FROM stock_daily
        WHERE trade_date <= %(trade_date)s
    ),
    latest_fundamental AS (
        SELECT f1.*
        FROM stock_fundamental f1
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) as max_date
            FROM stock_fundamental
            WHERE trade_date <= %(trade_date)s
            GROUP BY ts_code
        ) f2 ON f1.ts_code = f2.ts_code AND f1.trade_date = f2.max_date
    )
    SELECT d.*, f.roe, f.gross_margin, f.revenue_yoy, f.profit_yoy, f.operating_cash_flow
    FROM daily_data d
    LEFT JOIN (
        SELECT f.*
        FROM stock_fundamental f
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_date
            FROM stock_fundamental
            WHERE trade_date <= %(trade_date)s
            GROUP BY ts_code
        ) fm ON f.ts_code = fm.ts_code AND f.trade_date = fm.max_date
    ) f ON d.ts_code = f.ts_code
    """

    try:
        df_all = pd.read_sql(sql, engine, params={"trade_date": trade_date})
        logger.info("成功从数据库获取数据")
    except Exception as e:
        logger.error(f"数据库查询失败: {e}")
        return pd.DataFrame()

    results = []
    grouped = df_all.groupby("ts_code")

    for ts_code, df in grouped:
        df = df.sort_values("trade_date").reset_index(drop=True)

        # 数据量不足30天，跳过
        if len(df) < 30:
            continue

        df = df.copy()

        # 计算每日涨幅百分比
        df.loc[:, "pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100

        # 计算5日均量，反映短期成交量平均水平
        df.loc[:, "avg_vol_5"] = df["vol"].rolling(window=5).mean()

        # 计算过去20日最高收盘价，作为突破参考
        df.loc[:, "max_close_20"] = df["close"].rolling(window=20).max()

        # 计算5日、10日、20日均线，判断多头排列趋势
        df.loc[:, "ma5"] = df["close"].rolling(5).mean()
        df.loc[:, "ma10"] = df["close"].rolling(10).mean()
        df.loc[:, "ma20"] = df["close"].rolling(20).mean()

        # 判断多头排列：5日均线 > 10日均线 > 20日均线
        df.loc[:, "ma_bullish"] = (df["ma5"] > df["ma10"]) & (df["ma10"] > df["ma20"])

        last = df.iloc[-1]

        # MA多头排列连续2天
        ma_bullish_2days = df["ma_bullish"].iloc[-2:].all()

        # 技术面突破判断条件
        volume_price_breakout = (
            (last["pct_chg"] >= 3) and  # 当日涨幅至少3%
            (last["vol"] >= 1.5 * last["avg_vol_5"]) and  # 成交量至少1.5倍5日均量
            (last["close"] >= 0.98 * last["max_close_20"]) and  # 收盘价达到近20日最高价的98%
            (last["close"] > last["open"])  # 当日收盘高于开盘
        )

        # 满足技术面和趋势条件才进一步判断基本面
        if volume_price_breakout and ma_bullish_2days:
            fundamental = df.iloc[-1]

            # 基本面多条件筛选
            if (
                (fundamental['roe'] > 10) and
                (fundamental['gross_margin'] > 15) and
                (fundamental['revenue_yoy'] > 10) and
                (fundamental['profit_yoy'] > 15) and
                (fundamental['operating_cash_flow'] > 0)
            ):
                results.append({"ts_code": ts_code})

    return pd.DataFrame(results)


def strategy_top_gainers(trade_date: str) -> pd.DataFrame:
    """
    放宽版策略：
    - 技术面同原策略（涨幅前5%、成交额大、放量）
    - 基本面5项指标过滤，至少满足3条条件
    - 阈值较宽松

    基本面指标：
        • ROE > 8%
        • 毛利率 > 15%
        • 营收同比增长 > 5%
        • 净利润同比增长 > 5%
        • 资产负债率 < 70%
    """
    sql = """
    WITH daily_data AS (
        SELECT ts_code, trade_date, close, pre_close, vol, amount
        FROM stock_daily
        WHERE trade_date = %(trade_date)s
    )
    SELECT 
        d.ts_code, d.trade_date, d.close, d.pre_close, d.vol, d.amount,
        f.roe, f.gross_margin, f.revenue_yoy, f.profit_yoy, f.total_liabilities, f.total_assets
    FROM daily_data d
    LEFT JOIN (
        SELECT f.*
        FROM stock_fundamental f
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_date
            FROM stock_fundamental
            WHERE trade_date <= %(trade_date)s
            GROUP BY ts_code
        ) fm ON f.ts_code = fm.ts_code AND f.trade_date = fm.max_date
    ) f ON d.ts_code = f.ts_code
    """
    df = pd.read_sql(sql, engine, params={"trade_date": trade_date})
    df["pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100

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

    df = pd.merge(df, df_prev, on="ts_code")
    df["vol_increase_ratio"] = df["vol"] / df["vol_prev"]

    # 技术面筛选
    df = df[(df["amount"] >= 1e4) & (df["vol_increase_ratio"] >= 1.5)]

    # 基本面条件计数
    cond1 = df["roe"] > 8
    cond2 = df["gross_margin"] > 15
    cond3 = df["revenue_yoy"] > 5
    cond4 = df["profit_yoy"] > 5
    cond5 = (df["total_liabilities"] / df["total_assets"] * 100) < 70

    df["basic_conditions_met"] = cond1.astype(int) + cond2.astype(int) + cond3.astype(int) + cond4.astype(int) + cond5.astype(int)

    # 至少满足3条基本面条件
    df = df[df["basic_conditions_met"] >= 3]

    top5pct = int(len(df) * 0.05)
    df = df.sort_values("pct_chg", ascending=False).head(top5pct)

    return df.reset_index(drop=True)


def strategy_plate_breakout_post_close(trade_date: str) -> pd.DataFrame:
    """
    策略3 放宽版：平台突破 + 放量启动 + 基本面较优
    逻辑条件：
        1. 过去10日高点构成平台
        2. 今日收盘价突破10日最高价的98%（允许略低）
        3. 今日涨幅 > 3% （降低涨幅门槛）
        4. 今日成交量 > 过去5日均量的1.2倍 （降低放量要求）
    基本面筛选：
        • ROE > 8%
        • 毛利率 > 12%
        • 营收同比增长 > 8%
        • 净利润同比增长 > 12%
        • 经营性现金流净额 > 0
    """

    sql = """
    WITH daily_data AS (
        SELECT ts_code, trade_date, close, high, vol
        FROM stock_daily
        WHERE trade_date <= %(trade_date)s
    )
    SELECT d.*, f.eps, f.roe, f.gross_margin, f.revenue_yoy, f.profit_yoy, f.operating_cash_flow
    FROM daily_data d
    LEFT JOIN (
        SELECT f.*
        FROM stock_fundamental f
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_date
            FROM stock_fundamental
            WHERE trade_date <= %(trade_date)s
            GROUP BY ts_code
        ) fm ON f.ts_code = fm.ts_code AND f.trade_date = fm.max_date
    ) f ON d.ts_code = f.ts_code
    """
    df = pd.read_sql(sql, engine, params={"trade_date": trade_date})
    df = df.sort_values(["ts_code", "trade_date"])

    result = []

    for ts_code, group in df.groupby("ts_code"):
        group = group.tail(20).copy()
        # 数据不足11天，不处理
        if len(group) < 11:
            continue

        # 计算5日均量，用于成交量放大判断
        group["avg_vol_5"] = group["vol"].rolling(5).mean()

        # 历史数据为最近10天（不包括今天）
        history = group.iloc[:-1]
        today = group.iloc[-1]
        yesterday = group.iloc[-2]

        # 跳过缺失或异常数据
        if pd.isna(today["avg_vol_5"]) or yesterday["close"] == 0:
            continue

        max_high_10 = history["high"].iloc[-10:].max()
        pct_change = (today["close"] - yesterday["close"]) / yesterday["close"] * 100
        vol_ratio = today["vol"] / today["avg_vol_5"]

        # 判断是否突破平台且涨幅和放量满足要求
        if (
            today["close"] >= 0.98 * max_high_10 and  # 收盘价达到10日高点的98%
            pct_change > 3 and                        # 涨幅超过3%
            vol_ratio > 1.2                          # 成交量放大超过1.2倍
        ):
            f = today
            # 基本面指标放宽筛选
            if (
                f["roe"] > 8 and
                f["gross_margin"] > 12 and
                f["revenue_yoy"] > 8 and
                f["profit_yoy"] > 12 and
                f["operating_cash_flow"] > 0
            ):
                result.append({
                    "ts_code": ts_code,
                    "pct_change": round(pct_change, 2),
                    "vol_ratio": round(vol_ratio, 2),
                    "close": today["close"],
                    "max_high_10": max_high_10
                })

    return pd.DataFrame(result)


def strategy_macd_golden_cross(trade_date: str) -> pd.DataFrame:
    """
    策略4：MACD金叉 + 均线上行 + 基本面优质
    适合：趋势刚启动阶段买入
    技术指标：
        • DIF 上穿 DEA（MACD 金叉）
        • 当前收盘价 > MA5、MA10、MA20（多头排列）
        • 可配合量能判断（MACD 低位金叉更佳）
    持有期：3~5天
    原因：
        • MACD 金叉是趋势启动的信号，属于"中短线趋势追踪"策略
        • T+1 卖出太早，可能吃不到主升浪的肉
        • 建议配合移动止盈或持有若干天后再择机卖出
    基本面筛选：
        • ROE > 8%：确保公司有稳定的盈利能力
        • 毛利率 > 15%：确保公司有基本的利润空间
        • 营收同比增长 > 5%：确保公司业务稳定增长
        • 净利润同比增长 > 10%：确保公司利润稳定增长
        • 经营性现金流净额 > 0：确保公司经营现金流健康
    """
    sql = """
    WITH daily_data AS (
        SELECT ts_code, trade_date, close
        FROM stock_daily
        WHERE trade_date <= %(trade_date)s
    )
    SELECT d.*, f.roe, f.gross_margin, f.revenue_yoy, f.profit_yoy, f.operating_cash_flow
    FROM daily_data d
    LEFT JOIN (
        SELECT f.*
        FROM stock_fundamental f
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_date
            FROM stock_fundamental
            WHERE trade_date <= %(trade_date)s
            GROUP BY ts_code
        ) fm ON f.ts_code = fm.ts_code AND f.trade_date = fm.max_date
    ) f ON d.ts_code = f.ts_code
    """
    df = pd.read_sql(sql, engine, params={"trade_date": trade_date})
    df = df.sort_values(["ts_code", "trade_date"])

    result = []

    for ts_code, group in df.groupby("ts_code"):
        group = group.tail(35).copy()
        if len(group) < 26:
            continue

        group["ema12"] = group["close"].ewm(span=12).mean()
        group["ema26"] = group["close"].ewm(span=26).mean()
        group["diff"] = group["ema12"] - group["ema26"]
        group["dea"] = group["diff"].ewm(span=9).mean()
        group["macd"] = 2 * (group["diff"] - group["dea"])

        group["ma5"] = group["close"].rolling(5).mean()
        group["ma10"] = group["close"].rolling(10).mean()
        group["ma20"] = group["close"].rolling(20).mean()

        last = group.iloc[-1]
        prev = group.iloc[-2]

        if pd.isna(prev["macd"]) or pd.isna(last["macd"]):
            continue

        macd_cross = prev["macd"] < 0 and last["macd"] > 0
        ma_upward = last["ma5"] > last["ma10"] > last["ma20"]

        if macd_cross and ma_upward:
            f = last
            if (
                f["roe"] > 8 and
                f["gross_margin"] > 15 and
                f["revenue_yoy"] > 5 and
                f["profit_yoy"] > 10 and
                f["operating_cash_flow"] > 0
            ):
                result.append({
                    "ts_code": ts_code,
                    "strategy": "macd_golden_cross",
                    "macd": round(last["macd"], 4),
                    "close": last["close"],
                    "ma5": round(last["ma5"], 2),
                    "ma10": round(last["ma10"], 2),
                    "ma20": round(last["ma20"], 2)
                })

    return pd.DataFrame(result)


def strategy_first_limit_up_low_position(trade_date: str):
    """
    策略5：低位涨停首板 + 基本面优质
    适合：捕捉主力打板启动的第一波
    技术指标：
        • 最近60日未涨停
        • 当日涨停（收盘价接近涨停价）
        • 成交量是5日均量的2倍，表明非一字板而是有效换手
    风险提示：
        • 次日走势有较大不确定性：有的直接一字连板，有的直接低开调整
        • 如果非一字板放量，T+1 开盘冲高出货是可行的，但风险波动较大
        • 建议视次日开盘盘口和换手率决定是否 T+1 卖出
    基本面筛选：
        • ROE > 10%：确保公司有基本的盈利能力
        • 毛利率 > 20%：确保公司有足够的利润空间
        • 营收同比增长 > 0：确保公司业务不萎缩
        • 净利润同比增长 > 0：确保公司利润不亏损
        • 资产负债率 < 70%：控制财务风险
    """
    sql = """
    WITH daily_data AS (
        SELECT ts_code, trade_date, close, pre_close, high, vol
        FROM stock_daily
        WHERE trade_date <= %(trade_date)s
    )
    SELECT d.*, f.roe, f.gross_margin, f.revenue_yoy, f.profit_yoy,
        f.total_liabilities, f.total_assets
    FROM daily_data d
    LEFT JOIN (
        SELECT f.*
        FROM stock_fundamental f
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_date
            FROM stock_fundamental
            WHERE trade_date <= %(trade_date)s
            GROUP BY ts_code
        ) fm ON f.ts_code = fm.ts_code AND f.trade_date = fm.max_date
    ) f ON d.ts_code = f.ts_code
    """
    df = pd.read_sql(sql, engine, params={"trade_date": trade_date})
    df = df.sort_values(["ts_code", "trade_date"])

    result = []
    for ts_code, group in df.groupby("ts_code"):
        group = group.tail(60).copy()
        if len(group) < 30:
            continue

        group["pct_chg"] = (group["close"] - group["pre_close"]) / group["pre_close"] * 100
        group["vol5"] = group["vol"].rolling(5).mean()

        last = group.iloc[-1]
        if last["pct_chg"] < 9.5:
            continue

        limit_up_price = round(last["pre_close"] * 1.10, 2)
        if abs(last["close"] - limit_up_price) > 0.01:
            continue  # 未真正封板

        prev_limit_ups = group.iloc[:-1]["pct_chg"] > 9.5
        if prev_limit_ups.any():
            continue  # 排除已涨停过的

        max_close_30 = group.iloc[:-1]["close"].max()
        low_position = last["close"] < max_close_30 * 0.8

        if not low_position:
            continue

        if last["vol5"] > 0 and last["vol"] < 2 * last["vol5"]:
            continue  # 未放量

        f = last
        if (
            f["roe"] > 10 and
            f["gross_margin"] > 20 and
            f["revenue_yoy"] > 0 and
            f["profit_yoy"] > 0 and
            f["total_liabilities"] / f["total_assets"] * 100 < 70
        ):
            result.append({
                "ts_code": ts_code,
                "strategy": "first_limit_up_low",
                "close": last["close"],
                "pct_chg": round(last["pct_chg"], 2),
                "volume_ratio": round(last["vol"] / last["vol5"], 2),
                "low_position": round(last["close"] / max_close_30, 2)
            })

    return pd.DataFrame(result)


def strategy_consolidation_breakout_preparation(trade_date: str) -> pd.DataFrame:
    """
    策略6：缩量整理后放量突破预备 + 基本面优质
    技术指标：
        • 最近10日振幅小于5%，成交量逐步缩小
        • 最新一日放量，突破前高
    持有期：3-5天
    基本面筛选：
        • ROE > 12%：确保公司有良好的盈利能力
        • 毛利率 > 20%：确保公司有足够的利润空间
        • 营收同比增长 > 15%：确保公司业务在增长
        • 净利润同比增长 > 20%：确保公司利润在增长
        • 经营性现金流净额 > 0：确保公司经营现金流健康
    """
    sql = """
    WITH daily_data AS (
        SELECT ts_code, trade_date, close, high, low, vol
        FROM stock_daily
        WHERE trade_date <= %(trade_date)s
    )
    SELECT d.*, f.roe, f.gross_margin, f.revenue_yoy, f.profit_yoy, f.operating_cash_flow
    FROM daily_data d
    LEFT JOIN (
        SELECT f.*
        FROM stock_fundamental f
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_date
            FROM stock_fundamental
            WHERE trade_date <= %(trade_date)s
            GROUP BY ts_code
        ) fm ON f.ts_code = fm.ts_code AND f.trade_date = fm.max_date
    ) f ON d.ts_code = f.ts_code
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
            amplitude_max < 5 and amplitude_mean < 3.5
            and consolidation["vol"].iloc[-1] < consolidation["vol"].max()
            and volume_ratio > 1.5
            and breakout["close"] > breakout_level
        ):
            f = breakout
            if (
                f["roe"] > 12 and
                f["gross_margin"] > 20 and
                f["revenue_yoy"] > 15 and
                f["profit_yoy"] > 20 and
                f["operating_cash_flow"] > 0
            ):
                result.append({
                    "ts_code": ts_code,
                    "strategy": "consolidation_breakout_pre",
                    "close": breakout["close"],
                    "volume_ratio": round(volume_ratio, 2),
                    "breakout_pct": round((breakout["close"] - breakout_level) / breakout_level * 100, 2),
                    "amplitude_mean": round(amplitude_mean, 2),
                })

    return pd.DataFrame(result)


def strategy_box_bottom_rebound(trade_date: str):
    """
    策略7：箱体底部反弹 + 基本面优质
    技术指标：
        • 近20日价格维持箱体
        • 最近一天收盘价接近最低点
    持有期：3-5天
    基本面筛选：
        • ROE > 8%：确保公司有基本的盈利能力
        • 毛利率 > 15%：确保公司有基本的利润空间
        • 营收同比增长 > 5%：确保公司业务稳定增长
        • 净利润同比增长 > 10%：确保公司利润稳定增长
        • 资产负债率 < 60%：控制财务风险
    """
    sql = """
    WITH daily_data AS (
        SELECT ts_code, trade_date, close
        FROM stock_daily
        WHERE trade_date <= %(trade_date)s
    )
    SELECT d.*, f.roe, f.gross_margin, f.revenue_yoy, f.profit_yoy,
        f.total_liabilities, f.total_assets
    FROM daily_data d
    LEFT JOIN (
        SELECT f.*
        FROM stock_fundamental f
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_date
            FROM stock_fundamental
            WHERE trade_date <= %(trade_date)s
            GROUP BY ts_code
        ) fm ON f.ts_code = fm.ts_code AND f.trade_date = fm.max_date
    ) f ON d.ts_code = f.ts_code
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
        box_width_pct = box_width / close_min * 100
        close_position_pct = (last_close - close_min) / box_width * 100 if box_width > 0 else 100

        # 条件1：振幅小于15%，视为箱体震荡
        # 条件2：当前价格接近箱体底部，处于低于20%的位置
        if box_width_pct < 15 and close_position_pct <= 20:
            f = group.iloc[-1]
            debt_ratio = f["total_liabilities"] / f["total_assets"] * 100 if f["total_assets"] > 0 else 100

            if (
                f["roe"] > 8 and
                f["gross_margin"] > 15 and
                f["revenue_yoy"] > 5 and
                f["profit_yoy"] > 10 and
                debt_ratio < 60
            ):
                result.append({
                    "ts_code": ts_code,
                    "strategy": "box_bottom_rebound",
                    "close": last_close,
                    "box_width_pct": round(box_width_pct, 2),
                    "close_position_pct": round(close_position_pct, 2)
                })

    return pd.DataFrame(result)


def strategy_ma_convergence_start(trade_date: str):
    """
    策略8：均线粘合预启动 + 基本面优质
    技术指标：
        • MA5、MA10、MA20粘合，且MA5上拐
    持有期：3-5天
    基本面筛选：
        • ROE > 10%：确保公司有基本的盈利能力
        • 毛利率 > 18%：确保公司有基本的利润空间
        • 营收同比增长 > 8%：确保公司业务稳定增长
        • 净利润同比增长 > 15%：确保公司利润稳定增长
        • 经营性现金流净额 > 0：确保公司经营现金流健康
    """
    sql = """
    WITH daily_data AS (
        SELECT ts_code, trade_date, close
        FROM stock_daily
        WHERE trade_date <= %(trade_date)s
    )
    SELECT d.*, f.roe, f.gross_margin, f.revenue_yoy, f.profit_yoy, f.operating_cash_flow
    FROM daily_data d
    LEFT JOIN (
        SELECT f.*
        FROM stock_fundamental f
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_date
            FROM stock_fundamental
            WHERE trade_date <= %(trade_date)s
            GROUP BY ts_code
        ) fm ON f.ts_code = fm.ts_code AND f.trade_date = fm.max_date
    ) f ON d.ts_code = f.ts_code
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

        if (
            ma5_10_diff < 0.015 and
            ma10_20_diff < 0.015 and
            ma5_slope > 0
        ):
            f = last
            if (
                f["roe"] > 10 and
                f["gross_margin"] > 18 and
                f["revenue_yoy"] > 8 and
                f["profit_yoy"] > 15 and
                f["operating_cash_flow"] > 0
            ):
                result.append({
                    "ts_code": ts_code,
                    "strategy": "ma_convergence_start",
                    "ma5_10_diff_pct": round(ma5_10_diff * 100, 2),
                    "ma10_20_diff_pct": round(ma10_20_diff * 100, 2),
                    "ma5_slope": round(ma5_slope, 3),
                    "close": last["close"]
                })

    return pd.DataFrame(result)


def strategy_macd_divergent_gold_cross(trade_date: str):
    """
    策略9：MACD金叉背离 + 基本面优质
    技术指标：
        • MACD低位金叉，且价格不创新低
    持有期：3-5天
    基本面筛选：
        • ROE > 9%：确保公司有基本的盈利能力
        • 毛利率 > 16%：确保公司有基本的利润空间
        • 营收同比增长 > 6%：确保公司业务稳定增长
        • 净利润同比增长 > 12%：确保公司利润稳定增长
        • 资产负债率 < 65%：控制财务风险
    """
    sql = """
    WITH daily_data AS (
        SELECT ts_code, trade_date, close
        FROM stock_daily
        WHERE trade_date <= %(trade_date)s
    )
    SELECT d.*, f.roe, f.gross_margin, f.revenue_yoy, f.profit_yoy, f.total_liabilities, f.total_assets
    FROM daily_data d
    LEFT JOIN (
        SELECT f.*
        FROM stock_fundamental f
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_date
            FROM stock_fundamental
            WHERE trade_date <= %(trade_date)s
            GROUP BY ts_code
        ) fm ON f.ts_code = fm.ts_code AND f.trade_date = fm.max_date
    ) f ON d.ts_code = f.ts_code
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
            f = last
            if (
                f["roe"] > 9 and
                f["gross_margin"] > 16 and
                f["revenue_yoy"] > 6 and
                f["profit_yoy"] > 12 and
                f["total_liabilities"] / f["total_assets"] * 100 < 65
            ):
                result.append({
                    "ts_code": ts_code,
                    "strategy": "macd_divergence",
                    "macd": round(last["macd"], 4),
                    "diff": round(last["diff"], 4),
                    "dea": round(last["dea"], 4),
                    "min_recent_low": round(recent_lows.min(), 2),
                    "last_close": round(last["close"], 2),
                })
    return pd.DataFrame(result)


def strategy_annual_line_breakout(trade_date: str):
    """
    策略10：低位放量突破年线 + 基本面优质
    技术指标：
        • 当前收盘价首次上穿年线
        • 当前放量 > 5日均量1.5倍
    持有期：3-5天
    基本面筛选：
        • ROE > 12%：确保公司有良好的盈利能力
        • 毛利率 > 22%：确保公司有较高的利润空间
        • 营收同比增长 > 15%：确保公司业务在增长
        • 净利润同比增长 > 20%：确保公司利润在增长
        • 经营性现金流净额 > 0：确保公司经营现金流健康
    """
    sql = """
    WITH daily_data AS (
        SELECT ts_code, trade_date, close, vol
        FROM stock_daily
        WHERE trade_date <= %(trade_date)s
    )
    SELECT d.*, f.roe, f.gross_margin, f.revenue_yoy, f.profit_yoy, f.operating_cash_flow
    FROM daily_data d
    LEFT JOIN (
        SELECT f.*
        FROM stock_fundamental f
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_date
            FROM stock_fundamental
            WHERE trade_date <= %(trade_date)s
            GROUP BY ts_code
        ) fm ON f.ts_code = fm.ts_code AND f.trade_date = fm.max_date
    ) f ON d.ts_code = f.ts_code
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
            f = last
            if (
                f["roe"] > 12 and
                f["gross_margin"] > 22 and
                f["revenue_yoy"] > 15 and
                f["profit_yoy"] > 20 and
                f["operating_cash_flow"] > 0
            ):
                result.append({
                    "ts_code": ts_code,
                    "strategy": "annual_line_breakout",
                    "last_close": round(last["close"], 2),
                    "ma250": round(last["ma250"], 2),
                    "vol": int(last["vol"]),
                    "vol5": int(last["vol5"]),
                })
    return pd.DataFrame(result)


# 策略配置字典，包含持有期和权重信息
STRATEGY_CONFIG = {
    "strategy_check_breakout_batch": {
        "holding_days": 1,
        "weight": 1.5,  # 提高权重，因为基本面筛选最严格
        "description": "突破+放量策略 (ROE>15%, 毛利率>25%, 营收增长>20%, 利润增长>30%)"
    },
    "strategy_top_gainers": {
        "holding_days": 1,
        "weight": 1.3,
        "description": "涨幅前5%策略 (ROE>12%, 毛利率>20%, 营收增长>15%, 利润增长>20%)"
    },
    "strategy_plate_breakout_post_close": {
        "holding_days": 1,
        "weight": 1.2,
        "description": "平台突破策略 (ROE>10%, 毛利率>15%, 营收增长>10%, 利润增长>15%)"
    },
    "strategy_macd_golden_cross": {
        "holding_days": 5,
        "weight": 1.0,
        "description": "MACD金叉策略 (ROE>8%, 毛利率>15%, 营收增长>5%, 利润增长>10%)"
    },
    "strategy_first_limit_up_low_position": {
        "holding_days": 1,
        "weight": 1.4,
        "description": "低位涨停首板策略 (ROE>10%, 毛利率>20%, 营收和利润>0)"
    },
    "strategy_consolidation_breakout_preparation": {
        "holding_days": 5,
        "weight": 1.3,
        "description": "缩量整理突破策略 (ROE>12%, 毛利率>20%, 营收增长>15%, 利润增长>20%)"
    },
    "strategy_box_bottom_rebound": {
        "holding_days": 5,
        "weight": 1.0,
        "description": "箱体底部反弹策略 (ROE>8%, 毛利率>15%, 营收增长>5%, 利润增长>10%)"
    },
    "strategy_ma_convergence_start": {
        "holding_days": 5,
        "weight": 1.1,
        "description": "均线粘合预启动策略 (ROE>10%, 毛利率>18%, 营收增长>8%, 利润增长>15%)"
    },
    "strategy_macd_divergent_gold_cross": {
        "holding_days": 5,
        "weight": 1.0,
        "description": "MACD金叉背离策略 (ROE>9%, 毛利率>16%, 营收增长>6%, 利润增长>12%)"
    },
    "strategy_annual_line_breakout": {
        "holding_days": 5,
        "weight": 1.2,
        "description": "年线突破策略 (ROE>12%, 毛利率>22%, 营收增长>15%, 利润增长>20%)"
    }
}

# 策略注册表
ALL_STRATEGIES = [
    # strategy_check_breakout_batch,
    # strategy_top_gainers,
    strategy_plate_breakout_post_close,
    # # 将长线暂停
    # # strategy_macd_golden_cross,
    # # strategy_first_limit_up_low_position,
    # # 新增5个潜伏类策略
    # strategy_consolidation_breakout_preparation,
    # strategy_box_bottom_rebound,
    # strategy_ma_convergence_start,
    # strategy_macd_divergent_gold_cross,
    # strategy_annual_line_breakout,
]
