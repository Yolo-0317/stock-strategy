import time
from datetime import datetime, timedelta

from config import MYSQL_URL
from get_realtime import get_realtime_info
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

engine = create_engine(MYSQL_URL)
Session = sessionmaker(bind=engine)


def is_rising_in_recent_ticks(ts_code: str, minutes: int = 5) -> bool:
    since = datetime.now() - timedelta(minutes=minutes)
    with engine.connect() as conn:
        df = pd.read_sql(
            text(
                """
                SELECT timestamp, price FROM realtime_ticks
                WHERE ts_code = :ts_code AND timestamp >= :since
                ORDER BY timestamp ASC
                """
            ),
            conn,
            params={"ts_code": ts_code, "since": since},
        )
    if len(df) < 3:
        return False
    return all(df["price"].iloc[i] <= df["price"].iloc[i + 1] for i in range(len(df) - 1))


def check_sell_opportunity():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始检查持仓止盈止损条件")

    session = Session()
    try:
        holdings = ["604083", "601628", "603585", "605018", "603106"]

        for stock in holdings:
            ts_code = stock.ts_code
            buy_price = stock.buy_price

            try:
                info = get_realtime_info(ts_code, datetime.now().strftime("%Y-%m-%d"))
                if not info:
                    print(f"{ts_code} 实时行情获取失败，跳过")
                    continue
            except Exception as e:
                print(f"{ts_code} 获取实时行情异常: {e}")
                continue

            current_price = info.get("当前")
            if current_price is None:
                continue

            change_pct = (current_price - buy_price) / buy_price * 100

            # 基础止盈止损判断
            if change_pct >= 12:
                reason = "止盈（涨超12%）"
            elif change_pct <= -6:
                reason = "止损（跌超6%）"
            else:
                # 结合之前的策略判断卖出信号，比如：
                # 如果价格不再持续上涨或者成交量明显缩小，可以考虑卖出（示例逻辑）
                if not is_rising_in_recent_ticks(ts_code, 5):
                    reason = "价格不再持续上涨，建议止盈"
                else:
                    # 其他策略判断继续持有
                    continue

            print(f"[SELL] {ts_code} | {reason} | {current_price:.2f}")

    except Exception as e:
        session.rollback()
        print(f"检查卖出机会异常: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    import schedule

    print("启动实时卖出监控服务...")
    check_sell_opportunity()
    schedule.every(1).minutes.do(check_sell_opportunity)

    while True:
        schedule.run_pending()
        time.sleep(1)
