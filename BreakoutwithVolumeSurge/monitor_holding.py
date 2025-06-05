import time
from datetime import datetime

import pymysql
import requests
import schedule
from config import DB_CONFIG


def get_realtime_price(ts_code: str) -> float | None:
    market_prefix = "sz" if ts_code.endswith(".SZ") else "sh"
    code = market_prefix + ts_code[:6]
    url = f"https://qt.gtimg.cn/q={code}"
    try:
        resp = requests.get(url, timeout=5)
        data = resp.text.split("~")
        return float(data[3])  # 当前价格
    except Exception as e:
        print(f"实时获取 {ts_code} 失败：{e}")
        return None


def sell_stock(conn, stock, sell_price, reason):
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE holding_stock
        SET status = 'sold', sell_price = %s, sell_date = %s, reason = %s
        WHERE id = %s
    """,
        (sell_price, datetime.now().date(), reason, stock["id"]),
    )
    conn.commit()
    print(f"[SELL] {stock['ts_code']} | {reason} | {sell_price:.2f}")


def check_sell_opportunity():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始检查持仓止盈止损条件")
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    cursor.execute("SELECT * FROM holding_stock WHERE status = 'holding'")
    holdings = cursor.fetchall()

    for stock in holdings:
        price = get_realtime_price(stock["ts_code"])
        if not price:
            continue

        change_pct = (price - stock["buy_price"]) / stock["buy_price"] * 100

        if change_pct >= 12:
            sell_stock(conn, stock, price, "止盈")
        elif change_pct <= -6:
            sell_stock(conn, stock, price, "止损")

    conn.close()


# 每 N 分钟执行一次（比如 1 分钟）
schedule.every(30).seconds.do(check_sell_opportunity)

if __name__ == "__main__":
    print("启动实时监控服务...")
    check_sell_opportunity()  # 启动时先执行一次
    while True:
        schedule.run_pending()
        time.sleep(1)
