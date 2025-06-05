from datetime import datetime, timedelta

import pandas as pd
from config import MYSQL_URL
from get_realtime import get_realtime_info
from sqlalchemy import create_engine

engine = create_engine(MYSQL_URL)


def get_all_ts_codes():
    sql = "SELECT DISTINCT ts_code FROM stock_daily"
    df = pd.read_sql(sql, engine)
    return df["ts_code"].tolist()


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


def confirm_buy_with_realtime(ts_code, trade_date):
    yesterday_close = get_yesterday_close(ts_code, trade_date)
    today_date = (datetime.strptime(trade_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")

    try:
        today_info = get_realtime_info(ts_code, today_date)
    except Exception as e:
        print(f"{ts_code} 获取实时行情失败: {e}")
        return False

    today_open = today_info.get("今开")
    realtime_price = today_info.get("当前")
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


def check_breakout(ts_code, current_date):
    sql = """
    SELECT trade_date, open, close, pre_close, vol
    FROM stock_daily
    WHERE ts_code = %(ts_code)s AND trade_date <= %(date)s
    ORDER BY trade_date DESC
    LIMIT 60
    """
    df = pd.read_sql(sql, engine, params={"ts_code": ts_code, "date": current_date})

    if len(df) < 30:
        return None

    df = df.sort_values("trade_date").reset_index(drop=True)
    df["pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100
    df["avg_vol_5"] = df["vol"].rolling(window=5).mean()
    df["max_close_20"] = df["close"].rolling(window=20).max()
    df["ma5"] = df["close"].rolling(5).mean()
    df["ma10"] = df["close"].rolling(10).mean()
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma_bullish"] = (df["ma5"] > df["ma10"]) & (df["ma10"] > df["ma20"])
    ma_bullish = df["ma_bullish"].iloc[-3:].all()
    last = df.iloc[-1]

    volume_price_breakout = (
        last["pct_chg"] >= 5
        and last["vol"] >= 2 * last["avg_vol_5"]
        and last["close"] >= last["max_close_20"]
        and last["close"] > last["open"]
    )

    if volume_price_breakout and ma_bullish:
        return last
    return None


def select_top_gainers_with_volume_boost(trade_date: str):
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


def run_all_strategies_with_confirmation(trade_date):
    print(f"\n===== 执行选股和确认买入流程: {trade_date} =====")

    # 策略一：量价突破 + 多头排列
    breakout_hits = []
    ts_codes = get_all_ts_codes()
    for ts_code in ts_codes:
        result = check_breakout(ts_code, trade_date)
        if result is not None:
            breakout_hits.append({"ts_code": ts_code, "close": result["close"], "pct_chg": result["pct_chg"]})
    df_breakout = pd.DataFrame(breakout_hits)
    print(f"【策略1】命中数量: {len(df_breakout)}")

    # 策略二：涨幅前5% + 成交额大 + 放量
    df_top_gainers = select_top_gainers_with_volume_boost(trade_date)
    print(f"【策略2】命中数量: {len(df_top_gainers)}")

    # 合并两个策略命中股票（去重）
    all_candidates = pd.concat([df_breakout, df_top_gainers], ignore_index=True)
    all_candidates.drop_duplicates(subset="ts_code", inplace=True)

    # 实时确认买入
    confirmed_list = []
    for _, row in all_candidates.iterrows():
        ts_code = row["ts_code"]
        if confirm_buy_with_realtime(ts_code, trade_date):
            confirmed_list.append(ts_code)

    print(f"\n✅ 最终确认买入股票: {confirmed_list}")
    return confirmed_list


if __name__ == "__main__":
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    run_all_strategies_with_confirmation(yesterday)
