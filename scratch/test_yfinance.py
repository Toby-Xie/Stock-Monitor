import yfinance as yf
import pandas as pd

ticker = yf.Ticker("AAPL")
price = ticker.history(start="2020-01-01")[["Close"]]
price.index = price.index.tz_localize(None)

income = ticker.quarterly_income_stmt.T
income.index = pd.to_datetime(income.index).tz_localize(None)
income = income.sort_index()

net_income_col = "Net Income Common Stockholders"
income["NetIncome"] = income[net_income_col]

shares = ticker.info.get("sharesOutstanding")

income["EPS"] = income["NetIncome"] / shares
income["TTM_EPS"] = income["EPS"].rolling(4).sum()

price = price.join(income[["TTM_EPS"]], how="left")
price["TTM_EPS"] = price["TTM_EPS"].ffill()
price["PE_TTM"] = price["Close"] / price["TTM_EPS"]

print(price.tail())
price.to_csv("/Users/wentaoxie/Stock-Monitor/scratch/appl_pe_ttm.csv")