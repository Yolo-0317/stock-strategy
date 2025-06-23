import time
from datetime import datetime, timedelta

import pandas as pd
import tushare as ts
from config import MYSQL_URL, TUSHARE_TOKEN

# 加载表元信息
from models import StockDaily  # 假设你已定义 ORM 类
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import sessionmaker

# 初始化 Tushare 和数据库连接
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()
engine = create_engine(MYSQL_URL)


def get_daily_by_trade_date(trade_date: str):
    """
    分页拉取指定交易日所有股票的日行情数据
    """
    all_data = []
    offset = 0
    limit = 1000

    while True:
        try:
            df = pro.daily(trade_date=trade_date, offset=offset, limit=limit)
            if df.empty:
                break
            all_data.append(df)
            offset += limit
            time.sleep(0.2)  # 防止频率过高
        except Exception as e:
            print(f"❌ 拉取数据出错：{e}")
            break

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()


def save_to_mysql(df: pd.DataFrame):
    """
    有则更新，无则插入
    """
    df = df[["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "vol", "amount"]].copy()
    # 拆分 ts_code 为股票代码和交易所代码
    df["exch_code"] = df["ts_code"].str.split(".").str[1]  # 提取交易所代码
    df["ts_code"] = df["ts_code"].str.split(".").str[0]  # 只保留股票代码

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["update_time"] = datetime.now()

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for _, row in df.iterrows():
            insert_stmt = insert(StockDaily).values(**row.to_dict())
            update_stmt = insert_stmt.on_duplicate_key_update(
                open=insert_stmt.inserted.open,
                high=insert_stmt.inserted.high,
                low=insert_stmt.inserted.low,
                close=insert_stmt.inserted.close,
                pre_close=insert_stmt.inserted.pre_close,
                vol=insert_stmt.inserted.vol,
                amount=insert_stmt.inserted.amount,
                update_time=datetime.now(),
            )
            session.execute(update_stmt)

        session.commit()
        print(f"✅ 已插入/更新 {len(df)} 条记录")
    except Exception as e:
        session.rollback()
        print(f"❌ 写入失败: {e}")
    finally:
        session.close()


def run(trade_date: str):
    """
    主函数：拉取并写入指定日期的所有股票日行情
    """
    print(f"📦 开始处理：{trade_date}")
    df = get_daily_by_trade_date(trade_date)
    if df.empty:
        print(f"⚠️ 当天无数据：{trade_date}")
        return
    save_to_mysql(df)


if __name__ == "__main__":
    # 示例：拉取今天的数据
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    today = datetime.today().strftime("%Y%m%d")
    run(yesterday)
    run(today)
    # run('')
