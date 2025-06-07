import os
import sys

sys.path.insert(0, os.path.abspath("../"))

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
from strategies import ALL_STRATEGIES, STRATEGY_CONFIG

from utils.logger import logger

engine = create_engine(MYSQL_URL)


def calculate_strategy_score(row: pd.Series) -> float:
    """
    计算策略得分
    技术面得分 (40%):
    - 涨幅得分 (20%)
    - 量能得分 (10%)
    - 均线得分 (10%)
    基本面得分 (60%):
    - ROE得分 (15%)
    - 毛利率得分 (15%)
    - 营收增长得分 (15%)
    - 利润增长得分 (15%)
    """
    # 技术面得分 (40%)
    tech_score = 0
    
    # 涨幅得分 (20%)
    if row['pct_chg'] > 0:
        tech_score += min(row['pct_chg'] / 5, 1) * 20
    
    # 量能得分 (10%)
    if 'vol_ratio' in row and row['vol_ratio'] > 1:
        tech_score += min(row['vol_ratio'] / 2, 1) * 10
    
    # 均线得分 (10%)
    ma_score = 0
    if row['close'] > row['ma5']:
        ma_score += 2
    if row['close'] > row['ma10']:
        ma_score += 2
    if row['close'] > row['ma20']:
        ma_score += 2
    if row['close'] > row['ma60']:
        ma_score += 2
    if row['close'] > row['ma120']:
        ma_score += 2
    tech_score += ma_score
    
    # 基本面得分 (60%)
    fund_score = 0
    
    # ROE得分 (15%)
    if 'roe' in row:
        if row['roe'] > 15:
            fund_score += 15
        elif row['roe'] > 10:
            fund_score += 10
        elif row['roe'] > 8:
            fund_score += 5
    
    # 毛利率得分 (15%)
    if 'gross_margin' in row:
        if row['gross_margin'] > 25:
            fund_score += 15
        elif row['gross_margin'] > 20:
            fund_score += 10
        elif row['gross_margin'] > 15:
            fund_score += 5
    
    # 营收增长得分 (15%)
    if 'revenue_yoy' in row:
        if row['revenue_yoy'] > 20:
            fund_score += 15
        elif row['revenue_yoy'] > 15:
            fund_score += 10
        elif row['revenue_yoy'] > 10:
            fund_score += 5
    
    # 利润增长得分 (15%)
    if 'profit_yoy' in row:
        if row['profit_yoy'] > 30:
            fund_score += 15
        elif row['profit_yoy'] > 20:
            fund_score += 10
        elif row['profit_yoy'] > 10:
            fund_score += 5
    
    # 现金流得分 (额外加分项，最多10分)
    if 'operating_cash_flow' in row and row['operating_cash_flow'] > 0:
        fund_score += 10
    
    # 资产负债率得分 (额外加分项，最多10分)
    if 'total_liabilities' in row and 'total_assets' in row:
        debt_ratio = row['total_liabilities'] / row['total_assets'] * 100
        if debt_ratio < 50:
            fund_score += 10
        elif debt_ratio < 60:
            fund_score += 5
    
    # 总分 = 技术面得分 + 基本面得分
    total_score = tech_score + fund_score
    
    return total_score


def run_all_strategies_with_confirmation(trade_date: str):
    """
    执行所有策略选股，生成初步选股列表
    实盘确认由reconfirm_from_file函数处理
    """
    logger.info(f"\n===== 执行选股流程: {trade_date} =====")

    all_hits = []
    for strategy_func in ALL_STRATEGIES:
        try:
            # 给策略传前一天的数据
            pre_work_day = get_trade_date(trade_date)
            df = strategy_func(pre_work_day)
            if not df.empty:
                df["strategy"] = strategy_func.__name__  # 标记来源策略
                all_hits.append(df)
            logger.info(f"【{strategy_func.__name__}】命中数量: {len(df)}")
        except Exception as e:
            logger.error(f"策略 {strategy_func.__name__} 运行失败: {e}")

    if not all_hits:
        logger.info("无策略命中，结束")
        return

    df_all = pd.concat(all_hits, ignore_index=True)
    logger.info(df_all)

    # 过滤掉科创板（688开头）和创业板（300开头）
    df_confirmed = df_all[~df_all["ts_code"].str.startswith(("300", "688"))]

    # 获取昨收价格
    yesterday_closes = []
    for ts_code in df_confirmed["ts_code"]:
        yesterday_close = get_yesterday_close(ts_code, trade_date)
        yesterday_closes.append(yesterday_close)

    df_confirmed["昨收"] = yesterday_closes
    df_confirmed["策略名称"] = df_confirmed["strategy"].apply(lambda x: ", ".join(x))

    # 仅保留指定列
    df_confirmed = df_confirmed[["ts_code", "昨收", "策略名称"]]
    df_confirmed.rename(columns={"ts_code": "股票代码"}, inplace=True)

    filename = f"confirmed_stocks/confirmed_stocks_{trade_date}.csv"
    df_confirmed.to_csv(filename, index=False, encoding="utf-8-sig")
    logger.info(f"\n📁 初步选股列表已保存为文件: {filename}")
    logger.info(f"最终选股数量: {len(df_confirmed)}")


def reconfirm_from_file(trade_date: str):
    """
    从 confirmed_stocks_{trade_date}.csv 文件读取已确认股票，
    重新执行 confirm_buy_with_realtime 逻辑确认，并保存成功的股票列表。
    """
    filename = f"confirmed_stocks/confirmed_stocks_{trade_date}.csv"
    try:
        df = pd.read_csv(filename, encoding="utf-8-sig")
    except FileNotFoundError:
        logger.error(f"❌ 文件未找到: {filename}")
        return

    reconfirmed_list = []

    for _, row in df.iterrows():
        ts_code = row["股票代码"]
        strategies = row["策略名称"].split(", ")

        try:
            if confirm_buy_with_realtime(ts_code, trade_date):
                # 计算策略得分和持有期
                score, holding_days = calculate_strategy_score(row)
                row["策略得分"] = score
                row["建议持有期"] = holding_days
                reconfirmed_list.append(row)
        except Exception as e:
            logger.error(f"⚠️ {ts_code} 实时确认失败: {e}")
            continue

    if not reconfirmed_list:
        # 清空 CSV 内容：保存空表头或无内容的 DataFrame
        empty_df = pd.DataFrame(columns=["股票代码", "昨收", "策略数量", "策略得分", "建议持有期", "策略名称"])
        new_filename = f"reconfirmed_stocks/reconfirmed_stocks_{trade_date}.csv"
        empty_df.to_csv(new_filename, index=False, encoding="utf-8-sig")
        logger.info(f"🗑️ 已清空旧文件内容: {new_filename}")
        return

    df_reconfirmed = pd.DataFrame(reconfirmed_list)
    # 按策略得分、昨收价格和策略数量排序
    df_reconfirmed.sort_values(by=["策略得分", "昨收", "策略数量"], ascending=[False, True, False], inplace=True)
    
    new_filename = f"reconfirmed_stocks/reconfirmed_stocks_{trade_date}.csv"
    df_reconfirmed.to_csv(new_filename, index=False, encoding="utf-8-sig")
    logger.info(f"✅ 实时复审完成，已保存为: {new_filename}")


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

    # trade_date = datetime.now().strftime("%Y%m%d")
    # TODO 注意这里
    trade_date = "20250607"
    logger.info(f"\n🕒 当前时间: {now.strftime('%H:%M')}，判断逻辑触发中…")

    if 700 <= current_time < 1130:
        logger.info("🌅 [盘前] 执行选股")
        run_all_strategies_with_confirmation(trade_date)

    elif 930 <= current_time < 1500:
        logger.info("📈 [盘中] 每分钟记录股票行情，启动定时实时复审")
        run_schedule_reconfirm(trade_date)

    elif 1500 <= current_time < 2100:
        logger.info("🌇 [盘后] 执行选股")
        run_all_strategies_with_confirmation(trade_date)

    else:
        logger.info("🛑 当前时间不在策略运行时段内，无操作。")


if __name__ == "__main__":
    run_by_time()
    # trade_date = datetime.now().strftime("%Y%m%d")
    # run_all_strategies_with_confirmation(trade_date, need_realtime_confirm=False)
