import requests

# 示例：贵州茅台 secid = 1.600519（上证 600519）
url = "https://push2.eastmoney.com/api/qt/stock/get"
params = {
    "secid": "1.600519",  # 1 表示沪市，0 表示深市
    "fields": "f43,f44,f45,f46,f47,f48,f49,f58"
}
headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://quote.eastmoney.com/"
}

response = requests.get(url, params=params, headers=headers)
data = response.json()

stock_data = data.get("data", {})

print({
    "股票名称": stock_data.get("f58"),
    "最新价格": stock_data.get("f43"),
    "主力流入": stock_data.get("f46"),
    "主力流出": stock_data.get("f47"),
    "主力净额": stock_data.get("f48"),
    "主力净比": stock_data.get("f49")
})