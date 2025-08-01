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


def to_date8(date_str):
    """把2025-06-27或2025/06/27转成20250627"""
    return date_str.replace("-", "").replace("/", "")


def get_previous_trading_date(trade_date: str):
    """
    获取指定日期之前的最近一个交易日
    """
    import datetime

    trade_date_obj = datetime.datetime.strptime(trade_date, "%Y%m%d")

    # 从T-1日开始往前找，最多找10天
    for i in range(1, 11):
        check_date = trade_date_obj - datetime.timedelta(days=i)
        check_date_str = check_date.strftime("%Y-%m-%d")

        # 查询这个日期是否有股票数据
        sql = """
        SELECT COUNT(*) as count
        FROM stock_daily
        WHERE trade_date = %(check_date)s
        LIMIT 1
        """

        try:
            result = pd.read_sql(sql, engine, params={"check_date": check_date_str})
            if result.iloc[0]["count"] > 0:
                logger.info(f"找到最近交易日: {check_date_str}")
                return check_date_str
        except Exception as e:
            logger.error(f"查询日期 {check_date_str} 失败: {e}")
            continue

    logger.error(f"未找到 {trade_date} 之前的交易日")
    return None


def strategy_limit_up_continuation_prediction(trade_date: str):
    """
    涨停连板预测策略
    从T-1日涨停且股价低于13元的股票中，预测T日是否会连板
    重点识别：高开低走、一字板、获利盘抛压等风险信号
    """
    # 获取T-1日（最近一个交易日）的日期
    yesterday = get_previous_trading_date(trade_date)

    if not yesterday:
        logger.error(f"无法获取 {trade_date} 之前的交易日")
        return pd.DataFrame()

    logger.info(f"预测日期: {trade_date} -> T-1日: {yesterday}")

    # 先获取T-1日所有股票数据，然后计算涨跌幅筛选涨停股票
    sql = """
    SELECT ts_code, trade_date, open, close, pre_close, vol, high, low, amount
    FROM stock_daily
    WHERE trade_date = %(yesterday)s
    """

    try:
        # 获取T-1日所有股票数据
        df_yesterday = pd.read_sql(sql, engine, params={"yesterday": yesterday})

        if df_yesterday.empty:
            logger.info(f"在 {yesterday} 没有找到股票数据")
            return pd.DataFrame()

        # 计算涨跌幅
        df_yesterday["pct_chg"] = (df_yesterday["close"] - df_yesterday["pre_close"]) / df_yesterday["pre_close"] * 100

        # 更严格的涨停判断条件
        # 1. 涨幅 >= 9.5%
        # 2. 收盘价 >= 最高价的95%（避免冲高回落）
        # 3. 成交量 > 0（确保有交易）
        limit_up_stocks = df_yesterday[
            (df_yesterday["pct_chg"] >= 9.5)
            & (df_yesterday["close"] >= df_yesterday["high"] * 0.95)
            & (df_yesterday["vol"] > 0)
        ]

        if limit_up_stocks.empty:
            logger.info(f"在 {yesterday} 没有找到涨停股票")
            return pd.DataFrame()

        logger.info(f"在 {yesterday} 找到 {len(limit_up_stocks)} 只涨停股票")

        # 筛选股价低于13元的涨停股票
        low_price_limit_up = limit_up_stocks[limit_up_stocks["close"] < 13.0]

        if low_price_limit_up.empty:
            logger.info(f"在 {yesterday} 没有找到股价低于13元的涨停股票")
            return pd.DataFrame()

        logger.info(f"在 {yesterday} 找到 {len(low_price_limit_up)} 只股价低于13元的涨停股票")

        # 获取这些股票的详细历史数据用于预测
        limit_up_codes = low_price_limit_up["ts_code"].tolist()

        # 构建IN查询 - 获取T-1日及之前的数据
        if len(limit_up_codes) == 1:
            detail_sql = """
            SELECT ts_code, trade_date, open, close, pre_close, vol, high, low, amount
            FROM stock_daily
            WHERE ts_code = %(ts_code)s AND trade_date <= %(yesterday)s
            ORDER BY ts_code, trade_date DESC
            """
            params = {"ts_code": limit_up_codes[0], "yesterday": yesterday}
        else:
            placeholders = ",".join([f"%(ts_code_{i})s" for i in range(len(limit_up_codes))])
            detail_sql = f"""
            SELECT ts_code, trade_date, open, close, pre_close, vol, high, low, amount
            FROM stock_daily
            WHERE ts_code IN ({placeholders}) AND trade_date <= %(yesterday)s
            ORDER BY ts_code, trade_date DESC
            """
            params = {"yesterday": yesterday}
            for i, code in enumerate(limit_up_codes):
                params[f"ts_code_{i}"] = code

        try:
            df_all = pd.read_sql(detail_sql, engine, params=params)
        except Exception as e:
            logger.error(f"数据库查询失败: {e}")
            return pd.DataFrame()

        results = []

        # 添加调试统计
        debug_stats = {
            "total_candidates": len(limit_up_codes),
            "data_insufficient": 0,
            "score_too_low": 0,
            "passed_all": 0,
            "filtered_risk_signals": 0,
        }

        for ts_code, df in df_all.groupby("ts_code"):
            df = df.sort_values("trade_date").reset_index(drop=True)
            if len(df) < 10:  # 需要至少10天数据
                debug_stats["data_insufficient"] += 1
                continue

            # 计算技术指标
            df["ma5"] = df["close"].rolling(5).mean()
            df["ma10"] = df["close"].rolling(10).mean()
            df["ma20"] = df["close"].rolling(20).mean()
            df["pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100
            df["avg_vol_5"] = df["vol"].rolling(5).mean()
            df["vol_ratio"] = df["vol"] / df["avg_vol_5"]

            # T-1日（昨天）是最后一天数据
            current_idx = len(df) - 1

            # 1. 再次确认T-1日是否为涨停
            yesterday_pct_chg = df["pct_chg"].iloc[current_idx]
            yesterday_close = df["close"].iloc[current_idx]
            yesterday_high = df["high"].iloc[current_idx]
            yesterday_open = df["open"].iloc[current_idx]
            yesterday_low = df["low"].iloc[current_idx]
            yesterday_pre_close = df["pre_close"].iloc[current_idx]

            # 更严格的涨停确认
            if yesterday_pct_chg < 9.5 or yesterday_close < yesterday_high * 0.95:
                continue

            # 2. 高开低走风险识别（但不直接过滤，只作为评分参考）
            risk_score = 0
            risk_details = []

            # 风险1：一字板或接近一字板
            limit_up_price = yesterday_pre_close * 1.1
            if yesterday_open >= limit_up_price * 0.98:
                risk_score += 20
                risk_details.append("一字板风险")

            # 风险2：高开幅度过大
            open_pct_chg = (yesterday_open - yesterday_pre_close) / yesterday_pre_close * 100
            if open_pct_chg > 7:
                risk_score += 15
                risk_details.append("高开幅度过大")
            elif open_pct_chg > 5:
                risk_score += 10
                risk_details.append("高开幅度较大")

            # 风险3：振幅过大
            amplitude = (yesterday_high - yesterday_low) / yesterday_pre_close * 100
            if amplitude > 6:
                risk_score += 10
                risk_details.append("振幅过大")

            # 风险4：成交量异常
            if df["vol_ratio"].iloc[current_idx] < 0.8:
                risk_score += 15
                risk_details.append("成交量过小")
            elif df["vol_ratio"].iloc[current_idx] > 4:
                risk_score += 10
                risk_details.append("成交量过大")

            # 风险5：连续涨停后获利盘抛压
            consecutive_limit_up = 0
            for i in range(current_idx, max(0, current_idx - 5), -1):
                if df["pct_chg"].iloc[i] >= 9.5:
                    consecutive_limit_up += 1
                else:
                    break

            # 3. 改进的预测评分系统（基于T-1日及之前的数据）
            score = 0
            score_details = {}

            # 涨停强度评分（35分）
            yesterday_vol_ratio = df["vol_ratio"].iloc[current_idx]
            if yesterday_vol_ratio > 2.0:
                score += 35
                score_details["放量2倍以上"] = 35
            elif yesterday_vol_ratio > 1.5:
                score += 25
                score_details["放量1.5倍以上"] = 25
            elif yesterday_vol_ratio > 1.2:
                score += 15
                score_details["放量1.2倍以上"] = 15
            elif yesterday_vol_ratio > 1.0:
                score += 8
                score_details["放量正常"] = 8

            # 技术面评分（30分）
            ma5_yesterday = df["ma5"].iloc[current_idx]
            ma10_yesterday = df["ma10"].iloc[current_idx]
            ma20_yesterday = df["ma20"].iloc[current_idx]

            # 价格突破均线
            if yesterday_close > ma5_yesterday:
                score += 10
                score_details["突破MA5"] = 10
            if yesterday_close > ma10_yesterday:
                score += 10
                score_details["突破MA10"] = 10
            if yesterday_close > ma20_yesterday:
                score += 10
                score_details["突破MA20"] = 10

            # 连续涨停天数评分（20分）
            if consecutive_limit_up >= 3:
                score += 20
                score_details["连续3板以上"] = 20
            elif consecutive_limit_up >= 2:
                score += 15
                score_details["连续2板"] = 15
            elif consecutive_limit_up >= 1:
                score += 10
                score_details["首板"] = 10

            # 市场环境评分（15分）
            recent_5_days_avg_change = df["pct_chg"].iloc[current_idx - 4 : current_idx + 1].mean()
            if recent_5_days_avg_change > 1.0:
                score += 15
                score_details["市场强势"] = 15
            elif recent_5_days_avg_change > 0.5:
                score += 10
                score_details["市场偏强"] = 10
            elif recent_5_days_avg_change > 0:
                score += 5
                score_details["市场平稳"] = 5

            # 4. 最终评分筛选（降低阈值）
            if score >= 60:  # 降低评分阈值到60分
                debug_stats["passed_all"] += 1
                results.append(
                    {
                        "ts_code": ts_code,
                        "trade_date": trade_date,
                        "close": round(yesterday_close, 2),
                        "open": round(yesterday_open, 2),
                        "pct_chg": round(yesterday_pct_chg, 2),
                        "open_pct_chg": round(open_pct_chg, 2),
                        "vol_ratio": round(yesterday_vol_ratio, 2),
                        "ma5": round(ma5_yesterday, 2),
                        "ma10": round(ma10_yesterday, 2),
                        "ma20": round(ma20_yesterday, 2),
                        "score": score,
                        "score_details": str(score_details),
                        "risk_score": risk_score,
                        "risk_details": str(risk_details),
                        "consecutive_limit_up": consecutive_limit_up,
                        "strategy": "limit_up_continuation_prediction",
                    }
                )
            else:
                debug_stats["score_too_low"] += 1

        # 输出调试信息
        logger.info(f"涨停连板策略调试信息: {debug_stats}")

        if results:
            df_result = pd.DataFrame(results)
            # 按评分排序，选择最优质的股票
            df_result = df_result.sort_values("score", ascending=False)
            return df_result
        else:
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"查询涨停股票失败: {e}")
        return pd.DataFrame()


# 策略注册表
ALL_STRATEGIES = [
    strategy_limit_up_continuation_prediction,  # 涨停连板预测策略
]
