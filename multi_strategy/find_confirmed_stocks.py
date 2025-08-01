import os
import pandas as pd
from datetime import datetime


def find_confirmed_stocks(stock_codes, date=None, confirmed_dir="confirmed_stocks"):
    """
    在 confirmed_stocks 目录下，根据日期查找对应的 csv 文件，返回出现在该文件中的股票代码
    :param stock_codes: 股票代码列表
    :param date: 日期，格式为 'YYYYMMDD' 或 datetime 对象，如果为 None 则查找最新文件
    :param confirmed_dir: confirmed_stocks 目录路径
    :return: 存在于 csv 文件中的股票代码列表
    """
    # 获取 confirmed_stocks 目录下所有 csv 文件
    files = [f for f in os.listdir(confirmed_dir) if f.endswith(".csv")]
    if not files:
        print("未找到csv文件")
        return []

    # 根据日期参数确定要查找的文件
    if date is None:
        # 如果没有指定日期，查找最新文件
        files.sort(reverse=True)
        target_file = files[0]
    else:
        # 如果指定了日期，查找对应日期的文件
        if isinstance(date, datetime):
            date_str = date.strftime("%Y%m%d")
        else:
            date_str = str(date)

        target_file = f"confirmed_stocks_{date_str}.csv"

        if target_file not in files:
            print(f"未找到日期为 {date_str} 的csv文件")
            return []

    target_path = os.path.join(confirmed_dir, target_file)

    # 读取 csv 文件
    df = pd.read_csv(target_path, dtype={"股票代码": str})

    # 查找存在的股票代码
    found_codes = df["股票代码"].astype(str).isin([str(code) for code in stock_codes])
    result = df.loc[found_codes, "股票代码"].tolist()

    # 中文注释：返回存在于csv中的股票代码
    return result


# 示例用法
if __name__ == "__main__":
    # 例子1：查找这些股票代码是否在指定日期的confirmed_stocks中
    codes = ["000758", "002218", "002356", "002481", "002871", "600545", "600807"]
    date = datetime.now().strftime("%Y%m%d")
    date = "20250717"
    found = find_confirmed_stocks(codes, date=date)
    print(f"存在于的confirmed_stocks_{date}.csv的股票代码：", found)
