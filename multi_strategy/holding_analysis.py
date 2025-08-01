import os
import sys
from datetime import datetime, timedelta
from typing import List

import pandas as pd
from config import MYSQL_URL
from sqlalchemy import create_engine

# 修复导入路径问题
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from utils.logger import logger
except ImportError:
    # 如果找不到utils模块，创建一个简单的logger
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

engine = create_engine(MYSQL_URL)


def get_previous_trading_date(trade_date: str):
    """
    获取指定日期之前的最近一个交易日
    """
    trade_date_obj = datetime.strptime(trade_date, "%Y%m%d")

    # 从T-1日开始往前找，最多找10天
    for i in range(1, 11):
        check_date = trade_date_obj - timedelta(days=i)
        check_date_str = check_date.strftime("%Y-%m-%d")

        # 查询这个日期是否有股票数据
        sql = """
        SELECT COUNT(*) as count
        FROM stock_daily
        WHERE trade_date = %(check_date)s
        LIMIT 1
        """

        try:
            result = pd.read_sql(sql, engine, params={"check_date": check_date_str})
            if result.iloc[0]["count"] > 0:
                logger.info(f"找到最近交易日: {check_date_str}")
                return check_date_str
        except Exception as e:
            logger.error(f"查询日期 {check_date_str} 失败: {e}")
            continue

    logger.error(f"未找到 {trade_date} 之前的交易日")
    return None


def add_exchange_suffix(stock_codes: List[str]) -> List[str]:
    """
    为股票代码添加交易所后缀
    """
    result = []
    for code in stock_codes:
        # 移除可能已有的后缀
        clean_code = code.replace(".SZ", "").replace(".SH", "")

        # 根据代码规则判断交易所
        if clean_code.startswith(("000", "002", "003", "300")):
            result.append(f"{clean_code}.SZ")
        elif clean_code.startswith(("600", "601", "603", "605", "688")):
            result.append(f"{clean_code}.SH")
        else:
            # 默认添加.SZ后缀
            result.append(f"{clean_code}.SZ")

    return result


def analyze_holding_stocks(trade_date: str, holding_stocks: List[str]):
    """
    持仓股票T日涨势分析策略
    在T-1日收盘后，根据T-1日及之前的数据，分析持仓股票T日的上涨可能性

    Args:
        trade_date: 分析日期，格式：YYYYMMDD
        holding_stocks: 持仓股票代码列表，如 ["000001", "000002", "600000"]
    """
    # 直接使用传入的股票代码，不添加后缀
    print(f"原始股票代码: {holding_stocks}")

    # 获取T-1日（最近一个交易日）的日期
    yesterday = get_previous_trading_date(trade_date)

    if not yesterday:
        logger.error(f"无法获取 {trade_date} 之前的交易日")
        return pd.DataFrame()

    start_date_obj = datetime.strptime(yesterday, "%Y-%m-%d") - timedelta(days=60)
    start_date = start_date_obj.strftime("%Y-%m-%d")

    print(f"\n{'='*80}")
    print(f"持仓股票分析报告")
    print(f"分析日期: {trade_date} -> T-1日: {yesterday}")
    print(f"持仓股票数量: {len(holding_stocks)}")
    print(f"数据范围: {start_date} 至 {yesterday}")
    print(f"{'='*80}")

    if not holding_stocks:
        print("没有持仓股票需要分析")
        return pd.DataFrame()

    # 先检查数据库中是否有这些股票的数据
    print(f"\n🔍 检查数据库中的股票数据...")

    # 检查单个股票的数据
    for ts_code in holding_stocks:
        check_sql = """
        SELECT COUNT(*) as count, MIN(trade_date) as min_date, MAX(trade_date) as max_date
        FROM stock_daily
        WHERE ts_code = %(ts_code)s
        """
        try:
            result = pd.read_sql(check_sql, engine, params={"ts_code": ts_code})
            count = result.iloc[0]["count"]
            min_date = result.iloc[0]["min_date"]
            max_date = result.iloc[0]["max_date"]
            print(f"  {ts_code}: {count}条记录, 日期范围: {min_date} 至 {max_date}")
        except Exception as e:
            print(f"  {ts_code}: 查询失败 - {e}")

    # 检查指定日期范围的数据
    check_date_sql = """
    SELECT COUNT(*) as count
    FROM stock_daily
    WHERE trade_date >= %(start_date)s AND trade_date <= %(yesterday)s
    """
    try:
        date_result = pd.read_sql(check_date_sql, engine, params={"start_date": start_date, "yesterday": yesterday})
        total_records = date_result.iloc[0]["count"]
        print(f"\n✅ 日期范围 {start_date} 至 {yesterday} 的总记录数: {total_records}")
    except Exception as e:
        print(f"\n❌ 日期范围查询失败: {e}")

    # 获取持仓股票的详细数据
    placeholders = ",".join([f"%(ts_code_{i})s" for i in range(len(holding_stocks))])
    sql = f"""
    SELECT ts_code, trade_date, open, close, pre_close, vol, high, low, amount
    FROM stock_daily
    WHERE ts_code IN ({placeholders}) AND trade_date >= %(start_date)s AND trade_date <= %(yesterday)s
    ORDER BY ts_code, trade_date
    """

    params = {"start_date": start_date, "yesterday": yesterday}
    for i, code in enumerate(holding_stocks):
        params[f"ts_code_{i}"] = code

    print(f"\n🔍 执行查询SQL...")
    print(f"SQL: {sql}")
    print(f"参数: {params}")

    try:
        df_all = pd.read_sql(sql, engine, params=params)
        print(f"查询结果: {len(df_all)}条记录")
    except Exception as e:
        logger.error(f"数据库查询失败: {e}")
        return pd.DataFrame()

    if df_all.empty:
        print("没有找到持仓股票数据")
        return pd.DataFrame()

    results = []
    analysis_count = 0

    for ts_code, df in df_all.groupby("ts_code"):
        df = df.sort_values("trade_date").reset_index(drop=True)

        if len(df) < 20:  # 需要至少20天数据
            print(f"⚠️  {ts_code} 数据不足20天，跳过分析")
            continue

        analysis_count += 1
        print(f"\n{'─'*60}")
        print(f"📊 分析股票: {ts_code}")

        # 计算技术指标
        df["pct_chg"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100
        df["ma5"] = df["close"].rolling(5).mean()
        df["ma10"] = df["close"].rolling(10).mean()
        df["ma20"] = df["close"].rolling(20).mean()
        df["ma30"] = df["close"].rolling(30).mean()
        df["avg_vol_5"] = df["vol"].rolling(5).mean()
        df["avg_vol_10"] = df["vol"].rolling(10).mean()
        df["vol_ratio"] = df["vol"] / df["avg_vol_5"]

        # 计算MACD指标
        df["ema12"] = df["close"].ewm(span=12).mean()
        df["ema26"] = df["close"].ewm(span=26).mean()
        df["diff"] = df["ema12"] - df["ema26"]
        df["dea"] = df["diff"].ewm(span=9).mean()
        df["macd"] = 2 * (df["diff"] - df["dea"])

        # 计算KDJ指标
        df["low_9"] = df["low"].rolling(9).min()
        df["high_9"] = df["high"].rolling(9).max()
        df["rsv"] = (df["close"] - df["low_9"]) / (df["high_9"] - df["low_9"]) * 100
        df["k"] = df["rsv"].ewm(com=2).mean()
        df["d"] = df["k"].ewm(com=2).mean()
        df["j"] = 3 * df["k"] - 2 * df["d"]

        # 计算RSI指标
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df["rsi"] = 100 - (100 / (1 + rs))

        # T-1日（昨天）是最后一天数据
        current_idx = len(df) - 1
        yesterday_close = df["close"].iloc[current_idx]
        yesterday_open = df["open"].iloc[current_idx]
        yesterday_high = df["high"].iloc[current_idx]
        yesterday_low = df["low"].iloc[current_idx]

        print(f"📈 当前价格: {yesterday_close:.2f} (涨跌幅: {df['pct_chg'].iloc[current_idx]:.2f}%)")

        # 1. 技术面分析（40分）
        tech_score = 0
        tech_details = {}

        # 均线系统分析
        ma5_yesterday = df["ma5"].iloc[current_idx]
        ma10_yesterday = df["ma10"].iloc[current_idx]
        ma20_yesterday = df["ma20"].iloc[current_idx]
        ma30_yesterday = df["ma30"].iloc[current_idx]

        print(f"📊 均线系统: MA5={ma5_yesterday:.2f}, MA10={ma10_yesterday:.2f}, MA20={ma20_yesterday:.2f}")

        # 价格位置分析
        if yesterday_close > ma5_yesterday > ma10_yesterday > ma20_yesterday > ma30_yesterday:
            tech_score += 20
            tech_details["完美多头排列"] = 20
            print("✅ 完美多头排列")
        elif yesterday_close > ma5_yesterday > ma10_yesterday > ma20_yesterday:
            tech_score += 15
            tech_details["多头排列"] = 15
            print("✅ 多头排列")
        elif yesterday_close > ma5_yesterday > ma10_yesterday:
            tech_score += 10
            tech_details["短期多头"] = 10
            print("✅ 短期多头")
        elif yesterday_close > ma5_yesterday:
            tech_score += 5
            tech_details["价格在MA5之上"] = 5
            print("✅ 价格在MA5之上")
        else:
            print("❌ 价格在均线下方")

        # 均线趋势分析
        if current_idx >= 5:
            ma5_slope = (ma5_yesterday - df["ma5"].iloc[current_idx - 5]) / df["ma5"].iloc[current_idx - 5] * 100
            if ma5_slope > 2:
                tech_score += 10
                tech_details["MA5强势向上"] = 10
                print(f"✅ MA5强势向上 ({ma5_slope:.2f}%)")
            elif ma5_slope > 1:
                tech_score += 5
                tech_details["MA5向上"] = 5
                print(f"✅ MA5向上 ({ma5_slope:.2f}%)")
            else:
                print(f"❌ MA5趋势不明 ({ma5_slope:.2f}%)")

        # 技术指标分析
        current_macd_diff = df["diff"].iloc[current_idx]
        current_macd_dea = df["dea"].iloc[current_idx]

        if current_macd_diff > current_macd_dea > 0:
            tech_score += 10
            tech_details["MACD强势多头"] = 10
            print("✅ MACD强势多头")
        elif current_macd_diff > current_macd_dea:
            tech_score += 5
            tech_details["MACD金叉"] = 5
            print("✅ MACD金叉")
        else:
            print("❌ MACD弱势")

        # 2. 资金流向分析（30分）
        flow_score = 0
        flow_details = {}

        # 成交量分析
        yesterday_vol_ratio = df["vol_ratio"].iloc[current_idx]
        print(f"📊 成交量比: {yesterday_vol_ratio:.2f}")

        if yesterday_vol_ratio > 2.0:
            flow_score += 15
            flow_details["强势放量"] = 15
            print("✅ 强势放量")
        elif yesterday_vol_ratio > 1.5:
            flow_score += 10
            flow_details["放量"] = 10
            print("✅ 放量")
        elif yesterday_vol_ratio > 1.0:
            flow_score += 5
            flow_details["正常量能"] = 5
            print("✅ 正常量能")
        else:
            print("❌ 成交量萎缩")

        # 资金流入分析
        recent_3_days_positive = sum(1 for i in range(current_idx - 2, current_idx + 1) if df["pct_chg"].iloc[i] > 0)
        if recent_3_days_positive >= 2:
            flow_score += 10
            flow_details["连续资金流入"] = 10
            print(f"✅ 连续资金流入 ({recent_3_days_positive}/3天)")
        elif recent_3_days_positive >= 1:
            flow_score += 5
            flow_details["部分资金流入"] = 5
            print(f"✅ 部分资金流入 ({recent_3_days_positive}/3天)")
        else:
            print(f"❌ 资金流出 ({recent_3_days_positive}/3天)")

        # 价格强度分析
        recent_5_days_gain = sum(df["pct_chg"].iloc[current_idx - 4 : current_idx + 1])
        if recent_5_days_gain > 5:
            flow_score += 5
            flow_details["近期强势"] = 5
            print(f"✅ 近期强势 ({recent_5_days_gain:.2f}%)")
        else:
            print(f"📊 近期表现 ({recent_5_days_gain:.2f}%)")

        # 3. 市场情绪分析（20分）
        emotion_score = 0
        emotion_details = {}

        # RSI分析
        current_rsi = df["rsi"].iloc[current_idx]
        print(f"📊 RSI: {current_rsi:.2f}")

        if 30 < current_rsi < 70:
            emotion_score += 10
            emotion_details["RSI健康"] = 10
            print("✅ RSI健康")
        elif current_rsi > 70:
            emotion_score += 5
            emotion_details["RSI偏高"] = 5
            print("⚠️ RSI偏高")
        else:
            print("⚠️ RSI偏低")

        # KDJ分析
        current_k = df["k"].iloc[current_idx]
        current_d = df["d"].iloc[current_idx]
        print(f"📊 KDJ: K={current_k:.2f}, D={current_d:.2f}")

        if current_k > current_d and current_k > 50:
            emotion_score += 10
            emotion_details["KDJ强势"] = 10
            print("✅ KDJ强势")
        elif current_k > current_d:
            emotion_score += 5
            emotion_details["KDJ金叉"] = 5
            print("✅ KDJ金叉")
        else:
            print("❌ KDJ弱势")

        # 4. 风险控制分析（10分）
        risk_score = 0
        risk_details = {}

        # 位置分析
        recent_30_days_high = df["high"].iloc[current_idx - 29 : current_idx + 1].max()
        position_ratio = yesterday_close / recent_30_days_high

        print(f"📊 位置比: {position_ratio:.3f} (30日高点: {recent_30_days_high:.2f})")

        if position_ratio < 0.8:
            risk_score += 10
            risk_details["低位安全"] = 10
            print("✅ 低位安全")
        elif position_ratio < 0.9:
            risk_score += 5
            risk_details["中位安全"] = 5
            print("✅ 中位安全")
        else:
            print("⚠️ 高位风险")

        # 5. 计算总评分
        total_score = tech_score + flow_score + emotion_score + risk_score

        print(f"\n📊 评分详情:")
        print(f"   技术面: {tech_score}/40分")
        print(f"   资金流: {flow_score}/30分")
        print(f"   市场情绪: {emotion_score}/20分")
        print(f"   风险控制: {risk_score}/10分")
        print(f"   总分: {total_score}/100分")

        # 6. 计算关键价位（提前计算，供价格建议使用）
        support_level = min(ma10_yesterday, ma20_yesterday) * 0.98
        resistance_level = recent_30_days_high * 1.02

        # 7. 操作建议和价格建议
        if total_score >= 75:
            action = "持有/加仓"
            action_reason = "技术面强势，建议持有或适当加仓"
            action_icon = "🟢"

            # 强势股票：基于均线和昨收价格给出更实用的价格
            if yesterday_close > ma5_yesterday:
                # 如果价格在5日均线之上，建议在5日均线附近加仓
                suggested_buy_price = max(ma5_yesterday * 0.998, yesterday_close * 0.98)  # 不低于昨收98%
                price_reason = (
                    f"强势股票，建议在5日均线({ma5_yesterday:.2f})附近或昨收98%({yesterday_close*0.98:.2f})加仓"
                )
            else:
                # 如果价格跌破5日均线，建议在10日均线附近加仓
                suggested_buy_price = max(ma10_yesterday * 0.998, yesterday_close * 0.97)  # 不低于昨收97%
                price_reason = (
                    f"强势股票，建议在10日均线({ma10_yesterday:.2f})附近或昨收97%({yesterday_close*0.97:.2f})加仓"
                )

        elif total_score >= 60:
            action = "持有"
            action_reason = "技术面良好，建议持有观望"
            action_icon = "🟡"

            # 良好股票：基于昨收价格给出价格建议，更贴近开盘价
            suggested_buy_price = yesterday_close * 0.98  # 昨收98%
            price_reason = f"良好股票，建议在昨收98%({suggested_buy_price:.2f})附近补仓"

        elif total_score >= 40:
            action = "谨慎持有"
            action_reason = "技术面一般，建议谨慎持有"
            action_icon = "⚠️"

            # 一般股票：建议在昨收价格附近止损
            suggested_buy_price = yesterday_close * 0.97  # 昨收97%
            price_reason = f"谨慎持有，建议在昨收97%({suggested_buy_price:.2f})附近止损"

        else:
            action = "减仓/止损"
            action_reason = "技术面弱势，建议减仓"
            action_icon = "🔴"

            # 弱势股票：建议在昨收价格附近止损
            suggested_buy_price = yesterday_close * 0.96  # 昨收96%
            price_reason = f"弱势股票，建议在昨收96%({suggested_buy_price:.2f})附近止损"

        print(f"\n{action_icon} 操作建议: {action}")
        print(f" 理由: {action_reason}")
        print(f" 价格建议: {price_reason}")

        # 8. 显示关键价位
        print(f"\n💰 关键价位:")
        print(f"   支撑位: {support_level:.2f}")
        print(f"   压力位: {resistance_level:.2f}")
        print(f"   建议价格: {suggested_buy_price:.2f}")

        # 9. 添加实际交易建议
        print(f"\n📋 实际交易建议:")
        if total_score >= 60:
            print(f"   • 建议价格: {suggested_buy_price:.2f}")
            print(f"   • 开盘后观察价格走势，在建议价格附近分批介入")
            print(f"   • 如果开盘价接近建议价格，可直接买入")
            print(f"   • 如果开盘价高于建议价格较多，等待回调时买入")
            print(f"   • 分批介入，不要一次性满仓")
        else:
            print(f"   • 建议价格: {suggested_buy_price:.2f}")
            print(f"   • 如果跌破建议价格，及时减仓")
            print(f"   • 注意风险控制，不要恋战")

        # 10. 风险提示
        risk_warnings = []
        if current_rsi > 80:
            risk_warnings.append("RSI超买，注意回调风险")
        if current_k > 80:
            risk_warnings.append("KDJ超买，注意回调风险")
        if yesterday_vol_ratio < 0.8:
            risk_warnings.append("成交量萎缩，注意资金流出")
        if position_ratio > 0.95:
            risk_warnings.append("接近历史高点，注意压力")

        if risk_warnings:
            print(f"\n⚠️ 风险提示:")
            for warning in risk_warnings:
                print(f"   • {warning}")

        results.append(
            {
                "ts_code": ts_code,
                "trade_date": trade_date,
                "close": round(yesterday_close, 2),
                "pct_chg": round(df["pct_chg"].iloc[current_idx], 2),
                "vol_ratio": round(yesterday_vol_ratio, 2),
                "ma5": round(ma5_yesterday, 2),
                "ma10": round(ma10_yesterday, 2),
                "ma20": round(ma20_yesterday, 2),
                "rsi": round(current_rsi, 2),
                "kdj_k": round(current_k, 2),
                "macd_diff": round(current_macd_diff, 4),
                "position_ratio": round(position_ratio, 3),
                "total_score": total_score,
                "tech_score": tech_score,
                "flow_score": flow_score,
                "emotion_score": emotion_score,
                "risk_score": risk_score,
                "action": action,
                "action_reason": action_reason,
                "suggested_price": round(suggested_buy_price, 2),
                "price_reason": price_reason,
                "support_level": round(support_level, 2),
                "resistance_level": round(resistance_level, 2),
                "risk_warnings": str(risk_warnings),
                "strategy": "holding_stock_analysis",
            }
        )

    # 打印总结报告
    print(f"\n{'='*80}")
    print(f" 分析总结")
    print(f"{'='*80}")
    print(f"📊 分析股票数量: {analysis_count}")
    print(f"📊 成功分析数量: {len(results)}")

    if results:
        df_result = pd.DataFrame(results)
        df_result = df_result.sort_values("total_score", ascending=False)

        print(f"\n🏆 股票排名 (分析日期: {trade_date}):")
        for i, (_, row) in enumerate(df_result.iterrows(), 1):
            print(
                f"   {i}. {row['ts_code']} - 评分: {row['total_score']} - 建议: {row['action']} - 建议价格: {row['suggested_price']}"
            )

        print(f"\n📈 评分分布:")
        high_score = len(df_result[df_result["total_score"] >= 80])
        good_score = len(df_result[(df_result["total_score"] >= 60) & (df_result["total_score"] < 80)])
        medium_score = len(df_result[(df_result["total_score"] >= 40) & (df_result["total_score"] < 60)])
        low_score = len(df_result[df_result["total_score"] < 40])

        print(f"   强势(80+): {high_score}只")
        print(f"   良好(60-79): {good_score}只")
        print(f"   一般(40-59): {medium_score}只")
        print(f"   弱势(<40): {low_score}只")

        # 打印加仓建议汇总
        buy_recommendations = df_result[df_result["total_score"] >= 60].copy()
        if not buy_recommendations.empty:
            print(f"\n🟢 加仓建议汇总:")
            for _, row in buy_recommendations.iterrows():
                print(
                    f"   {row['ts_code']}: 当前价格 {row['close']}, 建议价格 {row['suggested_price']}, 理由: {row['price_reason']}"
                )

        return df_result
    else:
        print("没有持仓股票分析结果")
        return pd.DataFrame()


if __name__ == "__main__":
    # 示例使用 - 只传股票代码，不传交易所后缀
    holding_stocks = ["000758", "001213", "601669", "600110"]  # 只传股票代码
    # holding_stocks = [
    #     "000558",
    #     "002370",
    #     "002424",
    #     "002533",
    #     "002775",
    #     "002799",
    #     "002811",
    #     "600222",
    #     "600481",
    #     "600774",
    #     "600828",
    # ]
    today = datetime.now()
    if today.weekday() == 4:  # 4代表周五
        next_trade_date = today + timedelta(days=3)
    else:
        next_trade_date = today + timedelta(days=1)
    trade_date = next_trade_date.strftime("%Y%m%d")
    # trade_date = "20250724"

    result = analyze_holding_stocks(trade_date, holding_stocks)
