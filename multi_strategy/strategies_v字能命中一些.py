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


def strategy_v_shape_rebound_early_detection(trade_date: str):
    """
    V字反弹初期识别策略（基于600166优化版）
    专门识别股票正处于V字反弹初期的股票，预测T+1日会强势上涨

    基于600166股票分析优化的策略逻辑：
    1. 多维度评分系统（总分100分，阈值70分）
    2. 均线系统权重：35分（MA5上升15分 + MA10上升20分）
    3. 价格突破权重：25分（价量齐升15分 + 价格位置10分）
    4. 成交量确认权重：20分（放量突破）
    5. 其他指标权重：20分（开盘强势、连续上涨、技术指标等）

    核心改进：
    1. 采用量化评分系统，更客观
    2. 基于600166实际数据调整权重
    3. 增加价格位置和连续上涨判断
    4. 优化信号阈值和验证机制
    """
    sql = """
    SELECT ts_code, trade_date, open, close, pre_close, vol, high, low
    FROM stock_daily
    WHERE trade_date <= %(date)s
    ORDER BY ts_code, trade_date DESC
    """
    try:
        df_all = pd.read_sql(sql, engine, params={"date": trade_date})
    except Exception as e:
        logger.error(f"数据库查询失败: {e}")
        return pd.DataFrame()

    results = []
    trade_date_8 = to_date8(str(trade_date))

    for ts_code, df in df_all.groupby("ts_code"):
        df = df.sort_values("trade_date").reset_index(drop=True)
        if len(df) < 25:  # 需要至少25天数据
            continue

        # 计算技术指标
        df["ma5"] = df["close"].rolling(5).mean()
        df["ma10"] = df["close"].rolling(10).mean()
        df["ma20"] = df["close"].rolling(20).mean()
        df["pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100
        df["avg_vol_5"] = df["vol"].rolling(5).mean()
        df["vol_ratio"] = df["vol"] / df["avg_vol_5"]
        df["max_close_10"] = df["close"].rolling(10).max()
        df["min_close_10"] = df["close"].rolling(10).min()
        df["price_position"] = (df["close"] - df["min_close_10"]) / (df["max_close_10"] - df["min_close_10"])

        # 计算MACD指标
        df["ema12"] = df["close"].ewm(span=12).mean()
        df["ema26"] = df["close"].ewm(span=26).mean()
        df["diff"] = df["ema12"] - df["ema26"]
        df["dea"] = df["diff"].ewm(span=9).mean()
        df["macd"] = 2 * (df["diff"] - df["dea"])

        # 计算KDJ指标
        df["low_9"] = df["low"].rolling(9).min()
        df["high_9"] = df["high"].rolling(9).max()
        df["rsv"] = (df["close"] - df["low_9"]) / (df["high_9"] - df["low_9"]) * 100
        df["k"] = df["rsv"].ewm(com=2).mean()
        df["d"] = df["k"].ewm(com=2).mean()
        df["j"] = 3 * df["k"] - 2 * df["d"]

        # 确保日期格式一致
        df["trade_date"] = df["trade_date"].astype(str).str.replace("-", "").str.replace("/", "")

        # 只分析T日（今天）的数据
        if df["trade_date"].iloc[-1] != trade_date_8:
            continue

        current_idx = len(df) - 1  # T日索引

        # 检查数据是否足够
        if current_idx < 20:
            continue

        # 1. 检查T-2日是否为5日均线最低点（最近15天）
        ma5_window = df["ma5"].iloc[current_idx - 14 : current_idx + 1]  # T-14到T日
        ma5_min_idx = ma5_window.idxmin()
        ma5_min_relative_idx = ma5_min_idx - (current_idx - 14)  # 在窗口中的相对位置

        # T-2日应该是窗口中的第12个位置
        if ma5_min_relative_idx != 12:  # T-2日不是最低点
            continue

        # 2. 检查T-15到T-3期间是否下跌（累计跌幅≥5%）
        start_price = df["close"].iloc[current_idx - 15]  # T-15日收盘价
        end_price = df["close"].iloc[current_idx - 3]  # T-3日收盘价
        total_decline = (start_price - end_price) / start_price * 100

        if total_decline < 5:  # 累计跌幅≥5%
            continue

        # 3. 基于600166优化的多维度评分系统
        score = 0
        score_details = {}

        # 均线系统评分（权重：35分）
        ma5_today = df["ma5"].iloc[current_idx]
        ma5_yesterday = df["ma5"].iloc[current_idx - 1]
        ma10_today = df["ma10"].iloc[current_idx]
        ma10_yesterday = df["ma10"].iloc[current_idx - 1]

        ma5_up = ma5_today > ma5_yesterday
        ma10_up = ma10_today > ma10_yesterday
        ma5_gt_ma10 = ma5_today > ma10_today

        if ma5_up:
            score += 15
            score_details["ma5_up"] = 15
        if ma10_up:
            score += 20
            score_details["ma10_up"] = 20
        if ma5_gt_ma10:
            score += 5
            score_details["ma5_gt_ma10"] = 5

        # 价格突破评分（权重：25分）
        today_close = df["close"].iloc[current_idx]
        today_open = df["open"].iloc[current_idx]
        close_up = today_close > df["close"].iloc[current_idx - 1]
        close_above_open = today_close > today_open
        price_position = df["price_position"].iloc[current_idx]

        if close_up and close_above_open:
            score += 15
            score_details["price_volume_up"] = 15
        elif close_up:
            score += 10
            score_details["price_up"] = 10

        if price_position > 0.6:
            score += 10
            score_details["price_position"] = 10

        # 成交量确认评分（权重：20分）
        today_vol_ratio = df["vol_ratio"].iloc[current_idx]
        if today_vol_ratio > 1.5:
            score += 20
            score_details["volume_breakout"] = 20
        elif today_vol_ratio > 1.2:
            score += 15
            score_details["volume_good"] = 15
        elif today_vol_ratio > 0.8:
            score += 5
            score_details["volume_normal"] = 5

        # 其他指标评分（权重：20分）
        # 开盘强势
        open_strong = today_open > df["close"].iloc[current_idx - 1]
        if open_strong:
            score += 5
            score_details["open_strength"] = 5

        # 连续上涨
        consecutive_up = 0
        for i in range(max(0, current_idx - 3), current_idx):
            if df["close"].iloc[i] > df["close"].iloc[i - 1]:
                consecutive_up += 1
        if consecutive_up >= 2:
            score += 5
            score_details["consecutive_up"] = 5

        # 技术指标
        macd_today = df["macd"].iloc[current_idx]
        macd_yesterday = df["macd"].iloc[current_idx - 1]
        diff_today = df["diff"].iloc[current_idx]
        dea_today = df["dea"].iloc[current_idx]

        macd_golden_cross = (diff_today > dea_today) or (macd_today > macd_yesterday)
        if macd_golden_cross:
            score += 5
            score_details["macd_golden"] = 5

        # KDJ指标
        k_today = df["k"].iloc[current_idx]
        d_today = df["d"].iloc[current_idx]
        kdj_golden_cross = k_today > d_today
        if kdj_golden_cross:
            score += 5
            score_details["kdj_golden"] = 5

        # 4. 反弹力度检查
        lowest_price = df["close"].iloc[current_idx - 15 : current_idx - 2].min()
        rebound_strength = (today_close - lowest_price) / lowest_price * 100

        if rebound_strength < 3:
            continue

        # 5. 涨幅合理性检查
        today_pct_chg = df["pct_chg"].iloc[current_idx]
        if today_pct_chg < 2.0 or today_pct_chg > 9.5:
            continue

        # 6. 最终评分筛选（基于600166分析调整阈值）
        if score >= 70:  # 提高评分门槛，确保信号质量
            results.append(
                {
                    "ts_code": ts_code,
                    "trade_date": df["trade_date"].iloc[current_idx],
                    "close": round(today_close, 2),
                    "open": round(today_open, 2),
                    "pct_chg": round(today_pct_chg, 2),
                    "vol_ratio": round(today_vol_ratio, 2),
                    "ma5": round(ma5_today, 2),
                    "ma10": round(ma10_today, 2),
                    "ma20": round(df["ma20"].iloc[current_idx], 2),
                    "price_position": round(price_position * 100, 1),
                    "score": score,
                    "score_details": str(score_details),
                    "total_decline": round(total_decline, 2),
                    "rebound_strength": round(rebound_strength, 2),
                    "macd": round(macd_today, 4),
                    "kdj_k": round(k_today, 2),
                    "kdj_d": round(d_today, 2),
                    "ma_trend_bullish": ma5_gt_ma10,
                    "consecutive_up_days": consecutive_up,
                    "strategy": "v_shape_rebound_early_detection_optimized",
                }
            )

    if results:
        df_result = pd.DataFrame(results)
        # 按评分排序，选择最优质的股票
        df_result = df_result.sort_values("score", ascending=False)
        return df_result
    else:
        return pd.DataFrame()


# 策略注册表
ALL_STRATEGIES = [
    strategy_v_shape_rebound_early_detection,  # 新增V字反弹初期识别策略
]
