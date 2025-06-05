import pandas as pd
import requests


def get_stock_list_from_eastmoney():
    stock_list = []
    page = 1
    while True:
        url = f"https://19.push2.eastmoney.com/api/qt/clist/get?pn={page}&pz=500&fid=f3&fs=m:0+t:6,m:0+t:13,m:1+t:2,m:1+t:23&fields=f12,f14"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=10)
        data = resp.json()
        if not data["data"]:
            break
        items = data["data"]["diff"]
        print(f"已获取第 {page} 页数据")
        if not items:
            break
        for item in items.values():
            code = str(item["f12"])
            stock_list.append({"ts_code": code + (".SH" if code.startswith("6") else ".SZ"), "name": item["f14"]})
        page += 1

    df = pd.DataFrame(stock_list)
    df.to_csv("data/stock_list.csv", index=False, encoding="utf-8-sig")
    return df


# 测试用例
if __name__ == "__main__":
    df = get_stock_list_from_eastmoney()
    print(df.head())
