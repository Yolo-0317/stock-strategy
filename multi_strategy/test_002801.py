import pandas as pd
from sqlalchemy import create_engine
from config import MYSQL_URL

engine = create_engine(MYSQL_URL)


# def analyze_002801_v_shape():
#     sql = """
#     SELECT ts_code, trade_date, open, close, pre_close, vol
#     FROM stock_daily
#     WHERE ts_code = '002801' AND trade_date >= '2025-05-15' AND trade_date <= '2025-07-01'
#     ORDER BY trade_date
#     """
#     df = pd.read_sql(sql, engine)
#     if df.empty:
#         print("无数据")
#         return

#     # 计算均线、涨幅、放量等
#     df["ma5"] = df["close"].rolling(5).mean()
#     df["ma10"] = df["close"].rolling(10).mean()
#     df["ma20"] = df["close"].rolling(20).mean()
#     df["pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100
#     df["avg_vol_5"] = df["vol"].rolling(5).mean()
#     df["max_close_20"] = df["close"].rolling(20).max()
#     df["ma_bullish"] = (df["ma5"] > df["ma10"]) & (df["ma10"] > df["ma20"])
#     df["open_above_prev_close"] = df["open"] > df["pre_close"]

#     print(
#         df[
#             [
#                 "trade_date",
#                 "open",
#                 "close",
#                 "pre_close",
#                 "vol",
#                 "ma5",
#                 "ma10",
#                 "ma20",
#                 "pct_chg",
#                 "avg_vol_5",
#                 "max_close_20",
#                 "ma_bullish",
#                 "open_above_prev_close",
#             ]
#         ]
#     )


# # analyze_002801_v_shape()


# def backtest_v_shape(ts_code, start_date, end_date):
#     sql = f"""
#     SELECT trade_date, open, close, pre_close, vol
#     FROM stock_daily
#     WHERE ts_code = '{ts_code}' AND trade_date >= '{start_date}' AND trade_date <= '{end_date}'
#     ORDER BY trade_date
#     """
#     df = pd.read_sql(sql, engine)
#     df["ma5"] = df["close"].rolling(5).mean()
#     df["ma10"] = df["close"].rolling(10).mean()
#     df["pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100
#     df["avg_vol_5"] = df["vol"].rolling(5).mean()
#     df["max_close_20"] = df["close"].rolling(20).max()
#     df["max_close_10"] = df["close"].rolling(10).max()

#     print(
#         "trade_date | close | ma5 | ma10 | pct_chg | avg_vol_5 | max_close_20 | open_above_prev_close | v_idx_5 | v_idx_10 | ma5_up | ma10_up | ma5>ma10 | close_up | volume_breakout | price_breakout | open_strength | 命中"
#     )
#     for i in range(19, len(df)):  # 至少20天数据
#         row = df.iloc[i]
#         # 最近7天V型最低点
#         if i < 6:
#             continue
#         v_idx_5 = df["ma5"].iloc[i - 6 : i + 1].idxmin()
#         v_idx_10 = df["ma10"].iloc[i - 6 : i + 1].idxmin()
#         # 均线上升
#         ma5_up = df["ma5"].iloc[i] > df["ma5"].iloc[i - 1]
#         ma10_up = df["ma10"].iloc[i] > df["ma10"].iloc[i - 1]
#         ma5_gt_ma10 = df["ma5"].iloc[i] > df["ma10"].iloc[i]
#         close_up = df["close"].iloc[i] > df["close"].iloc[i - 1]
#         volume_breakout = (df["vol"].iloc[i - 1] >= 0.95 * df["avg_vol_5"].iloc[i - 1]) or (
#             df["vol"].iloc[i] >= 0.95 * df["avg_vol_5"].iloc[i]
#         )
#         # price_breakout允许接近新高
#         price_breakout = df["close"].iloc[i] >= 0.98 * df["max_close_10"].iloc[i]
#         # 开盘强势
#         open_above_prev_close = df["open"].iloc[i] > df["close"].iloc[i - 1]
#         # 最近3天有一天开盘强势
#         open_strength = (df["open"].iloc[i - 2 : i + 1].values > df["close"].iloc[i - 3 : i].values).any()
#         # V型最低点在最近7天
#         v5_in_window = i - 6 <= v_idx_5 <= i
#         v10_in_window = i - 6 <= v_idx_10 <= i
#         # 命中判定
#         hit = (
#             v5_in_window
#             and v10_in_window
#             and ma5_up
#             and ma10_up
#             and ma5_gt_ma10
#             and close_up
#             and volume_breakout
#             and price_breakout
#             and open_strength
#             and df["close"].iloc[i] > df["open"].iloc[i]
#         )
#         print(
#             f"{row['trade_date']} | {row['close']:.2f} | {row['ma5']:.2f} | {row['ma10']:.2f} | {row['pct_chg']:.2f} | {row['avg_vol_5']:.2f} | {row['max_close_20']:.2f} | {open_above_prev_close} | {v_idx_5} | {v_idx_10} | {ma5_up} | {ma10_up} | {ma5_gt_ma10} | {close_up} | {volume_breakout} | {price_breakout} | {open_strength} | {'✅' if hit else '❌'}"
#         )


# # 用法示例
# backtest_v_shape("600166", "2025-06-05", "2025-07-07")


# def analyze_600166_v_shape_detailed():
#     """详细分析600166的V字反弹特征"""
#     sql = """
#     SELECT trade_date, open, close, pre_close, vol, high, low
#     FROM stock_daily
#     WHERE ts_code = '600166' AND trade_date >= '2025-06-05' AND trade_date <= '2025-07-07'
#     ORDER BY trade_date
#     """
#     df = pd.read_sql(sql, engine)

#     if df.empty:
#         print("无数据")
#         return

#     # 计算技术指标
#     df["ma5"] = df["close"].rolling(5).mean()
#     df["ma10"] = df["close"].rolling(10).mean()
#     df["ma20"] = df["close"].rolling(20).mean()
#     df["pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100
#     df["avg_vol_5"] = df["vol"].rolling(5).mean()
#     df["vol_ratio"] = df["vol"] / df["avg_vol_5"]
#     df["max_close_10"] = df["close"].rolling(10).max()
#     df["min_close_10"] = df["close"].rolling(10).min()
#     df["price_position"] = (df["close"] - df["min_close_10"]) / (df["max_close_10"] - df["min_close_10"])

#     print("=== 600166 V字反弹详细分析 ===")
#     print(f"分析期间: {df['trade_date'].min()} 到 {df['trade_date'].max()}")
#     print(f"总交易日数: {len(df)}")
#     print(f"起始价格: {df['close'].iloc[0]:.2f}")
#     print(f"结束价格: {df['close'].iloc[-1]:.2f}")
#     print(f"总涨幅: {((df['close'].iloc[-1] / df['close'].iloc[0]) - 1) * 100:.2f}%")

#     # 找到最低点
#     min_idx = df["close"].idxmin()
#     min_date = df.loc[min_idx, "trade_date"]
#     min_price = df.loc[min_idx, "close"]

#     print(f"\n=== V字反弹关键点 ===")
#     print(f"最低点日期: {min_date}")
#     print(f"最低点价格: {min_price:.2f}")

#     # 分析反弹特征
#     print(f"\n=== 反弹特征分析 ===")
#     for i, row in df.iterrows():
#         if i < 5:  # 跳过前5天，等待指标稳定
#             continue

#         # 计算反弹强度指标
#         bounce_strength = 0
#         conditions = []

#         # 1. 均线系统
#         if row["ma5"] > df["ma5"].iloc[i - 1] and row["ma10"] > df["ma10"].iloc[i - 1]:
#             bounce_strength += 1
#             conditions.append("均线上升")

#         if row["ma5"] > row["ma10"]:
#             bounce_strength += 1
#             conditions.append("MA5>MA10")

#         # 2. 价格突破
#         if row["close"] > row["open"]:
#             bounce_strength += 1
#             conditions.append("收盘>开盘")

#         if row["price_position"] > 0.7:
#             bounce_strength += 1
#             conditions.append("价格位置>70%")

#         # 3. 成交量确认
#         if row["vol_ratio"] > 1.2:
#             bounce_strength += 1
#             conditions.append("放量>120%")

#         # 4. 连续上涨
#         if i > 0 and row["close"] > df["close"].iloc[i - 1]:
#             bounce_strength += 1
#             conditions.append("价格上涨")

#         # 5. 开盘强势
#         if row["open"] > df["close"].iloc[i - 1]:
#             bounce_strength += 1
#             conditions.append("开盘强势")

#         # 输出高反弹强度的日期
#         if bounce_strength >= 4:
#             print(
#                 f"{row['trade_date']} | 强度:{bounce_strength}/6 | 价格:{row['close']:.2f} | 涨幅:{row['pct_chg']:.2f}% | 量比:{row['vol_ratio']:.2f} | 条件:{', '.join(conditions)}"
#             )

#     return df


# 运行详细分析
# analyze_600166_v_shape_detailed()


def optimized_v_shape_strategy(ts_code, start_date, end_date):
    """优化的V字反弹策略"""
    sql = f"""
    SELECT trade_date, open, close, pre_close, vol, high, low
    FROM stock_daily
    WHERE ts_code = '{ts_code}' AND trade_date >= '{start_date}' AND trade_date <= '{end_date}'
    ORDER BY trade_date
    """
    df = pd.read_sql(sql, engine)

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

    print(f"=== {ts_code} 优化V字反弹策略回测 ===")
    print("日期 | 收盘价 | MA5 | MA10 | 涨幅% | 量比 | 价格位置% | 反弹强度 | 信号 | 命中")
    print("-" * 100)

    signals = []
    hits = 0
    total_signals = 0

    for i in range(10, len(df)):  # 至少10天数据
        row = df.iloc[i]

        # 计算反弹强度指标
        strength = 0
        conditions = []

        # 1. 均线系统 (权重: 2)
        ma5_up = df["ma5"].iloc[i] > df["ma5"].iloc[i - 1]
        ma10_up = df["ma10"].iloc[i] > df["ma10"].iloc[i - 1]
        ma5_gt_ma10 = df["ma5"].iloc[i] > df["ma10"].iloc[i]

        if ma5_up and ma10_up:
            strength += 2
            conditions.append("均线双升")
        elif ma5_up:
            strength += 1
            conditions.append("MA5升")

        if ma5_gt_ma10:
            strength += 1
            conditions.append("MA5>MA10")

        # 2. 价格突破 (权重: 2)
        close_up = df["close"].iloc[i] > df["close"].iloc[i - 1]
        close_above_open = df["close"].iloc[i] > df["open"].iloc[i]

        if close_up and close_above_open:
            strength += 2
            conditions.append("价量齐升")
        elif close_up:
            strength += 1
            conditions.append("价格上涨")

        # 3. 成交量确认 (权重: 2)
        vol_breakout = row["vol_ratio"] > 1.2
        if vol_breakout:
            strength += 2
            conditions.append("放量突破")
        elif row["vol_ratio"] > 0.8:
            strength += 1
            conditions.append("量能正常")

        # 4. 价格位置 (权重: 1)
        if row["price_position"] > 0.6:
            strength += 1
            conditions.append("价格强势")

        # 5. 开盘强势 (权重: 1)
        open_strong = df["open"].iloc[i] > df["close"].iloc[i - 1]
        if open_strong:
            strength += 1
            conditions.append("开盘强势")

        # 6. 连续上涨趋势 (权重: 1)
        consecutive_up = 0
        for j in range(max(0, i - 3), i):
            if df["close"].iloc[j] > df["close"].iloc[j - 1]:
                consecutive_up += 1
        if consecutive_up >= 2:
            strength += 1
            conditions.append("连续上涨")

        # 信号生成
        signal = strength >= 6  # 至少6分才产生信号
        total_signals += 1 if signal else 0

        # 验证信号效果 (看后续3天是否上涨)
        future_return = 0
        if signal and i + 3 < len(df):
            future_return = (df["close"].iloc[i + 3] / df["close"].iloc[i] - 1) * 100
            if future_return > 2:  # 3天内上涨超过2%算命中
                hits += 1

        # 输出结果
        if signal or strength >= 4:  # 显示所有信号和强度>=4的情况
            hit_mark = "✅" if signal and future_return > 2 else "❌" if signal else ""
            print(
                f"{row['trade_date']} | {row['close']:.2f} | {row['ma5']:.2f} | {row['ma10']:.2f} | {row['pct_chg']:.2f}% | {row['vol_ratio']:.2f} | {row['price_position']*100:.0f}% | {strength}/8 | {'' if signal else ''} | {hit_mark}"
            )

    # 统计结果
    if total_signals > 0:
        hit_rate = hits / total_signals * 100
        print(f"\n=== 策略统计 ===")
        print(f"总信号数: {total_signals}")
        print(f"命中数: {hits}")
        print(f"命中率: {hit_rate:.2f}%")

    return df


# 运行优化策略
optimized_v_shape_strategy("600166", "2025-06-05", "2025-07-07")
