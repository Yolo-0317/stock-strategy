from typing import List

import pandas as pd
from config import MYSQL_URL
from sqlalchemy import create_engine, text

engine = create_engine(MYSQL_URL)


def insert_stocks_to_sell_table(ts_code_list: List[str], buy_date: str):
    """
    将指定股票加入 stock_to_sell 表，作为买入记录（T日买入，用于 T+1 卖出）
    """
    if not ts_code_list:
        print("空股票列表，跳过写入。")
        return

    # 获取T日（买入日）行情
    sql_today = """
    SELECT ts_code, trade_date, close, pre_close, vol, amount
    FROM stock_daily
    WHERE trade_date = :buy_date
      AND ts_code IN :ts_codes
    """
    df_today = pd.read_sql(text(sql_today), engine, params={"buy_date": buy_date, "ts_codes": tuple(ts_code_list)})

    if df_today.empty:
        print(f"{buy_date} 没有符合条件的股票行情数据。")
        return

    # 获取T-1日成交量
    sql_prev = """
    SELECT ts_code, vol AS vol_prev
    FROM stock_daily
    WHERE trade_date = (
        SELECT MAX(trade_date) FROM stock_daily
        WHERE trade_date < :buy_date
    )
    AND ts_code IN :ts_codes
    """
    df_prev = pd.read_sql(text(sql_prev), engine, params={"buy_date": buy_date, "ts_codes": tuple(ts_code_list)})

    # 合并数据
    df = pd.merge(df_today, df_prev, on="ts_code", how="left")
    df["pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100
    df["vol_increase_ratio"] = df["vol"] / df["vol_prev"]
    df["buy_date"] = buy_date
    df["buy_price"] = df["close"]
    df["sell_price"] = None
    df["profit"] = None
    df["confirm"] = 0

    # 插入数据
    df_to_insert = df[
        [
            "ts_code",
            "trade_date",
            "close",
            "pre_close",
            "vol",
            "amount",
            "pct_chg",
            "vol_prev",
            "vol_increase_ratio",
            "confirm",
            "buy_date",
            "buy_price",
            "sell_price",
            "profit",
        ]
    ]

    # 写入数据库
    df_to_insert.to_sql("stock_to_sell", engine, if_exists="append", index=False)
    print(f"成功插入 {len(df_to_insert)} 条记录到 stock_to_sell 表。")


if __name__ == "__main__":
    stock_list = ["000001.SZ", "300750.SZ"]
    insert_stocks_to_sell_table(stock_list, "2025-06-05")
