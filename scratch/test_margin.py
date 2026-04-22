import akshare as ak
import time
df = ak.stock_margin_sse(start_date="20210106", end_date="20260416")   # 上交所
print(df.head())
time.sleep(1)  # 避免请求过快被封
df2 = ak.stock_margin_szse(date="20260415")  # 深交所
print(df2.head())