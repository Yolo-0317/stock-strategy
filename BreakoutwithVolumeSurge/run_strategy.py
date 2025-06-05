import pandas as pd
from breakout_strategy import check_breakout, confirm_buy_with_realtime
from download_data import get_stock_list


def main():
    """
    主程序：对所有股票运行策略并实时确认，输出满足买入条件的股票
    """
    # 获取股票列表（DataFrame 格式，含 ts_code 和 name）
    stocks = get_stock_list()

    result = []

    for _, row in stocks.iterrows():
        ts_code = row["ts_code"]
        name = row["name"]

        # 第一步：技术面信号检测
        last = check_breakout(ts_code)
        if last is not None:
            trade_date = last["trade_date"]
            print(f"🎯 {ts_code} {name} 于 {trade_date} 触发买入信号，开始实时确认...")

            # 第二步：通过实时行情确认是否买入
            if confirm_buy_with_realtime(ts_code, trade_date):
                print(f"✅ {ts_code} {name} 实时确认通过，加入结果列表")
                result.append(
                    {
                        "股票代码": ts_code,
                        "股票名称": name,
                        # "涨幅(%)": round(last["pct_chg"], 2),
                        "成交量": last["vol"],
                        "收盘价": last["close"],
                        "日期": last["trade_date"],
                    }
                )
            else:
                print(f"⚠️ {ts_code} {name} 实时确认未通过，跳过")
        # else:
        #     print(f"⏭️ {ts_code} {name} 未触发买入信号，跳过")

    # 输出结果
    df = pd.DataFrame(result)
    df.to_csv("selected_stocks.csv", index=False, encoding="utf-8-sig")
    print(f"\n📈 策略执行完成，共确认买入 {len(df)} 支股票，结果已保存到 selected_stocks.csv")


if __name__ == "__main__":
    main()
