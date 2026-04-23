import akshare as ak
import pandas as pd
from datetime import datetime

symbols = ["北向资金", "沪股通", "深股通", "南向资金", "港股通沪", "港股通深"]

# 文件名
today = datetime.now().strftime("%Y%m%d")
file_path = f".\hsgt_hist_{today}.xlsx"

with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
    for symbol in symbols:
        try:
            df = ak.stock_hsgt_hist_em(symbol=symbol)
            
            if df.empty:
                print(f"{symbol} 没有数据")
                continue

            # 只取最近100行（你之前的需求）

            # Excel sheet 名不能太长
            sheet_name = symbol[:30]

            df.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"{symbol} 写入成功")

        except Exception as e:
            print(f"{symbol} 失败: {e}")

print(f"导出完成: {file_path}")