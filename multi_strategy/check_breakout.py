import pandas as pd
from config import MYSQL_URL
from sqlalchemy import create_engine

engine = create_engine(MYSQL_URL)


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
