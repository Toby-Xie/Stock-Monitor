import akshare as ak
import pandas as pd
from datetime import datetime

def normalize_margin(df, exchange):
    df = df.copy()
    
    if exchange == "SSE":
        df.rename(columns={
            "信用交易日期": "trade_date",
            "标的证券代码": "code",
            "标的证券简称": "name",
            "融资余额": "fin_balance",
            "融资买入额": "fin_buy",
            "融资偿还额": "fin_repay",
            "融券余量": "sec_volume",
            "融券卖出量": "sec_sell",
            "融券偿还量": "sec_repay",
        }, inplace=True)
        
    elif exchange == "SZSE":
        df.rename(columns={
            "证券代码": "code",
            "证券简称": "name",
            "融资余额": "fin_balance",
            "融资买入额": "fin_buy",
            "融券余量": "sec_volume",
            "融券卖出量": "sec_sell",
            "融券偿还量": "sec_repay",
            "融券余额": "sec_balance",
            "融资融券余额": "margin_balance",
        }, inplace=True)
    
    df["exchange"] = exchange
    
    return df

# 如果今天不是交易日，你可以手动改成最近交易日，比如：
today = "20260420"

print(f"获取日期: {today}")

# ===== 2. 获取上交所融资融券数据 =====
try:
    df_sse = ak.stock_margin_detail_sse(date=today)
    df_sse = normalize_margin(df_sse, "SSE")
    print("上交所数据获取成功")
except Exception as e:
    print("上交所数据获取失败:", e)
    df_sse = pd.DataFrame()

# ===== 3. 获取深交所融资融券数据 =====
try:
    df_szse = ak.stock_margin_detail_szse(date=today)
    df_szse = normalize_margin(df_szse, "SZSE")
    df_szse["trade_date"] = str(today)
    print("深交所数据获取成功")
except Exception as e:
    print("深交所数据获取失败:", e)
    df_szse = pd.DataFrame()

# ===== 4. 合并数据 =====
df_all = pd.concat([df_sse, df_szse], ignore_index=True)

# ===== 5. 导出 CSV =====
file_name = f"C:\\Users\\TobyXie\\Documents\\Stock-Monitor\\margin_{today}.csv"
df_all.to_csv(file_name, index=False, encoding="utf-8-sig")

print(f"导出完成: {file_name}")
print(f"总记录数: {len(df_all)}")