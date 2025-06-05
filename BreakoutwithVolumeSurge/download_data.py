import time
from datetime import datetime

import pandas as pd
import tushare as ts
from config import MYSQL_URL, TUSHARE_TOKEN
from models import StockDaily
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()
engine = create_engine(MYSQL_URL)
Session = sessionmaker(bind=engine)


def get_stock_list():
    return pd.read_csv("data/stock_list.csv")


def update_stock(ts_code):
    session = Session()
    try:
        last_record = (
            session.query(StockDaily).filter_by(ts_code=ts_code).order_by(StockDaily.trade_date.desc()).first()
        )
        start_date = last_record.trade_date.strftime("%Y%m%d") if last_record else "20220101"

        df = pro.daily(ts_code=ts_code, start_date=start_date)
        if df.empty:
            return

        # 只保留这几个字段
        columns = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "vol", "amount"]
        df = df[columns]

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df.to_sql("stock_daily", con=engine, if_exists="append", index=False)
        print(f"✅ 更新：{ts_code}")
        time.sleep(0.3)
    except Exception as e:
        print(f"❌ 错误：{ts_code}, {e}")
    finally:
        session.close()


def run():
    df = get_stock_list()
    for ts_code in df["ts_code"]:
        update_stock(ts_code)


if __name__ == "__main__":
    run()
