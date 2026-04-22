import pandas as pd
import akshare as ak
from datetime import date

df_today = ak.stock_hsgt_fund_flow_summary_em()
today = date.today().strftime("%Y%m%d")
df_today.to_csv(f"C:\\Users\\TobyXie\\Documents\\Stock-Monitor\\hsgt_fund_flow_summary_em_{today}.csv", index=False,encoding="utf-8-sig")