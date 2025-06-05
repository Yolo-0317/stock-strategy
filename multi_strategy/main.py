# main.py
from datetime import datetime, timedelta

import pandas as pd
from config import MYSQL_URL
from get_realtime import get_realtime_info
from sqlalchemy import create_engine
from strategies import ALL_STRATEGIES

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


def confirm_buy_with_realtime(ts_code: str, trade_date: str) -> bool:
    yesterday_close = get_yesterday_close(ts_code, trade_date)
    today_date = (datetime.strptime(trade_date, "%Y%m%d") + pd.Timedelta(days=1)).strftime("%Y%m%d")

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


def run_all_strategies_with_confirmation(trade_date: str, need_realtime_confirm: bool = True):
    print(f"\n===== 执行选股和{'实时确认' if need_realtime_confirm else '非实时'}买入流程: {trade_date} =====")

    all_hits = []
    for strategy_func in ALL_STRATEGIES:
        try:
            df = strategy_func(trade_date)
            if not df.empty:
                df["strategy"] = strategy_func.__name__  # 标记来源策略
                all_hits.append(df)
            print(f"【{strategy_func.__name__}】命中数量: {len(df)}")
        except Exception as e:
            print(f"策略 {strategy_func.__name__} 运行失败: {e}")

    if not all_hits:
        print("无策略命中，结束")
        return

    df_all = pd.concat(all_hits, ignore_index=True)

    # 过滤掉科创板（688开头）和创业板（300开头）
    df_all = df_all[~df_all["ts_code"].str.startswith(("300", "688"))]

    # 汇总每只股票命中策略
    df_grouped = df_all.groupby("ts_code")["strategy"].apply(list).reset_index()

    confirmed_list = []
    for _, row in df_grouped.iterrows():
        ts_code = row["ts_code"]
        strategies = row["strategy"]

        if need_realtime_confirm:
            if confirm_buy_with_realtime(ts_code, trade_date):
                confirmed_list.append({"ts_code": ts_code, "strategies": strategies, "strategy_count": len(strategies)})
        else:
            confirmed_list.append(
                {"ts_code": ts_code, "strategies": strategies, "strategy_count": len(strategies)}
            )  # 直接通过

    df_confirmed = pd.DataFrame(confirmed_list)

    # 获取实时现价和昨收
    current_prices = []
    yesterday_closes = []

    for _, row in df_confirmed.iterrows():
        ts_code = row["ts_code"]
        try:
            realtime_info = get_realtime_info(
                ts_code, (datetime.strptime(trade_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
            )
            current_price = realtime_info.get("当前")
        except Exception:
            current_price = None
        current_prices.append(current_price)

        # 获取昨收价格
        yesterday_close = get_yesterday_close(ts_code, trade_date)
        yesterday_closes.append(yesterday_close)

    df_confirmed["现价"] = current_prices
    df_confirmed["昨收"] = yesterday_closes
    df_confirmed["策略名称"] = df_confirmed["strategies"].apply(lambda x: ", ".join(x))

    # 准备保存文件
    # 构建排序价格列：优先用现价，没有就用昨收
    df_confirmed["排序价"] = df_confirmed["现价"].fillna(df_confirmed["昨收"]).infer_objects(copy=False)
    df_confirmed.sort_values(by=["排序价", "策略名称", "strategy_count"], ascending=[True, False, False], inplace=True)

    # 格式整理输出
    df_confirmed.rename(columns={"ts_code": "股票代码", "strategy_count": "策略数量"}, inplace=True)
    df_confirmed.drop(columns=["strategies", "排序价"], inplace=True)
    df_confirmed = df_confirmed[["股票代码", "现价", "昨收", "策略数量", "策略名称"]]

    filename = f"confirmed_stocks_{trade_date}.csv"
    df_confirmed.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"\n📁 最终确认买入股票列表，已保存为文件: {filename}")


if __name__ == "__main__":
    # 盘中
    # yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    # run_all_strategies_with_confirmation(yesterday, need_realtime_confirm=True)
    # 盘前
    run_all_strategies_with_confirmation("20250605", need_realtime_confirm=False)
