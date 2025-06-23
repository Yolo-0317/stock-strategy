import os
import sys

# sys.path.insert(0, os.path.abspath("../"))

import time
from datetime import datetime, timedelta

import pandas as pd
import schedule
from config import MYSQL_URL
from filter_with_realtime import confirm_buy_with_realtime, get_yesterday_close, record_realtime_ticks
from get_realtime import get_realtime_info

# 加载表元信息
from models import StockDaily  # 假设你已定义 ORM 类
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from strategies import ALL_STRATEGIES, filter_by_fundamentals

from utils.logger import logger

engine = create_engine(MYSQL_URL)


# 定义不进行基本面过滤的策略列表
strategies_without_fundamental_filter = [
    "strategy_check_breakout_batch",
    "strategy_top_gainers",
    "strategy_plate_breakout_post_close",
]


def run_all_strategies_with_confirmation(trade_date: str, need_realtime_confirm: bool = True):
    """
    执行选股和实时确认买入流程
    Args:
        trade_date: 交易日期
        need_realtime_confirm: 是否需要实时确认
    """
    logger.info(f"\n===== 执行选股和{'实时确认' if need_realtime_confirm else '非实时'}买入流程: {trade_date} =====")

    all_hits = []
    for strategy_func in ALL_STRATEGIES:
        try:
            # 给策略传前一天的数据
            pre_work_day = get_trade_date(trade_date)
            df = strategy_func(pre_work_day)
            if not df.empty:
                if strategy_func.__name__ not in strategies_without_fundamental_filter:
                    df = filter_by_fundamentals(df)  # 应用基本面过滤
                if not df.empty:
                    df["strategy"] = strategy_func.__name__
                    all_hits.append(df)
                    logger.info(f"【{strategy_func.__name__}】命中数量: {len(df)}")
        except Exception as e:
            logger.error(f"策略 {strategy_func.__name__} 运行失败: {e}")

    if not all_hits:
        logger.info("无策略命中，结束")
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
            confirmed_list.append({"ts_code": ts_code, "strategies": strategies, "strategy_count": len(strategies)})

    df_confirmed = pd.DataFrame(confirmed_list)

    # 获取实时现价和昨收
    current_prices = []
    yesterday_closes = []

    for _, row in df_confirmed.iterrows():
        ts_code = row["ts_code"]
        current_prices.append("")
        yesterday_close = get_yesterday_close(ts_code, trade_date)
        yesterday_closes.append(yesterday_close)

    df_confirmed["现价"] = current_prices
    df_confirmed["昨收"] = yesterday_closes
    df_confirmed["策略名称"] = df_confirmed["strategies"].apply(lambda x: ", ".join(x))

    # 准备保存文件
    df_confirmed["排序价"] = df_confirmed["现价"].fillna(df_confirmed["昨收"]).infer_objects(copy=False)
    df_confirmed.sort_values(by=["排序价", "策略名称", "strategy_count"], ascending=[True, False, False], inplace=True)

    # 格式整理输出
    df_confirmed.rename(columns={"ts_code": "股票代码", "strategy_count": "策略数量"}, inplace=True)
    df_confirmed.drop(columns=["strategies", "排序价"], inplace=True)
    df_confirmed = df_confirmed[["股票代码", "现价", "昨收", "策略数量", "策略名称"]]

    filename = f"confirmed_stocks/confirmed_stocks_{trade_date}.csv"
    df_confirmed.to_csv(filename, index=False, encoding="utf-8-sig")
    logger.info(f"\n📁 最终确认买入股票列表，已保存为文件: {filename}")


def save_dataframe_to_dated_folder(df: pd.DataFrame, base_dir: str = "reconfirmed_stocks"):
    """
    将 DataFrame 保存到以当前日期命名的文件夹中，文件名以小时和分钟为后缀。

    参数:
    - df: 要保存的 pandas DataFrame。
    - base_dir: 基础目录，默认为 'reconfirmed_stocks'。

    返回:
    - file_path: 保存的文件完整路径。
    """
    # 获取当前日期和时间
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")  # 日期格式：YYYY-MM-DD
    time_str = now.strftime("%H%M")  # 时间格式：HHMM

    # 构建文件夹路径
    folder_path = os.path.join(base_dir, date_str)
    os.makedirs(folder_path, exist_ok=True)  # 创建文件夹，如果已存在则忽略

    # 构建文件名和完整路径
    filename = f"reconfirmed_stocks_{time_str}.csv"
    file_path = os.path.join(folder_path, filename)

    # 保存 DataFrame 为 CSV 文件
    df.to_csv(file_path, index=False, encoding="utf-8-sig")

    return file_path


def reconfirm_from_file(trade_date: str):
    """
    从 confirmed_stocks_{trade_date}.csv 文件读取已确认股票，
    重新执行 confirm_buy_with_realtime 逻辑确认，并保存成功的股票列表。
    """
    filename = f"confirmed_stocks/confirmed_stocks_{trade_date}.csv"
    try:
        df = pd.read_csv(filename, encoding="utf-8-sig", dtype={"股票代码": str})
        df = df.sort_values(by=["策略数量"], ascending=[False])
        # 取前30个，防止请求实时信息来不及
        # df = df.iloc[:30]
    except FileNotFoundError:
        logger.error(f"❌ 文件未找到: {filename}")
        return

    reconfirmed_list = []

    for _, row in df.iterrows():
        ts_code = row["股票代码"]

        try:
            if confirm_buy_with_realtime(ts_code, trade_date):
                # 获取最新现价
                realtime_info = get_realtime_info(ts_code, trade_date)
                current_price = realtime_info.get("当前")
            else:
                continue  # 未通过实时确认，跳过
        except Exception as e:
            logger.error(f"⚠️ {ts_code} 实时确认失败: {e}")
            continue

        if current_price is not None:
            row["现价"] = current_price
            reconfirmed_list.append(row)

    if not reconfirmed_list:
        logger.info(f"实时复审无命中")
        return

    df_reconfirmed = pd.DataFrame(reconfirmed_list)
    file_path = save_dataframe_to_dated_folder(df_reconfirmed)
    logger.info(f"✅ 实时复审完成，已保存为: {file_path}")


def is_market_open():
    now = datetime.now()
    hm = now.hour * 100 + now.minute  # 例如930, 1130

    return (930 <= hm <= 1130) or (1300 <= hm <= 1500)


def run_schedule_reconfirm(trade_date: str):
    logger.info(f"⏳ 每一分钟执行一次实时数据下载和实时复审任务，开始监控...（交易日: {trade_date}）")

    def job():
        if is_market_open():
            record_realtime_ticks(trade_date)
            reconfirm_from_file(trade_date)
        else:
            logger.info("当前非交易时间段，不执行实时复审和下载任务。")

    schedule.every(1).minute.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)


def get_trade_date(trade_date: str = "") -> str:
    """获取上一个工作日（非周六/周日）"""
    if trade_date:
        today = datetime.strptime(trade_date, "%Y%m%d")
    else:
        today = datetime.now()
    delta = timedelta(days=1)

    # 回溯到最近的工作日
    while True:
        today -= delta
        if today.weekday() < 5:  # 0~4 表示周一~周五
            break

    return today.strftime("%Y%m%d")


def run_by_time():
    now = datetime.now()
    current_hour = now.hour
    current_minute = now.minute
    current_time = current_hour * 100 + current_minute  # e.g. 930, 1530

    trade_date = datetime.now().strftime("%Y%m%d")
    # trade_date = "20250607"
    logger.info(f"\n🕒 当前时间: {now.strftime('%H:%M')}，判断逻辑触发中…")

    if 800 <= current_time < 1330:
        logger.info("🌅 [盘前] 执行非实时选股")
        run_all_strategies_with_confirmation(trade_date, need_realtime_confirm=False)

    elif 930 <= current_time < 1500:
        logger.info("📈 [盘中] 每分钟记录股票行情，启动定时实时复审")
        run_schedule_reconfirm(trade_date)

    elif 1500 <= current_time < 2100:
        logger.info("🌇 [盘后] 执行非实时选股")
        trade_date = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
        run_all_strategies_with_confirmation(trade_date, need_realtime_confirm=False)

    else:
        logger.info("🛑 当前时间不在策略运行时段内，无操作。")


if __name__ == "__main__":
    run_by_time()
    trade_date = datetime.now().strftime("%Y%m%d")
    # run_all_strategies_with_confirmation(trade_date, need_realtime_confirm=False)
    # reconfirm_from_file(trade_date)
