import yfinance as yf
import pandas as pd
ticker = yf.Ticker("XEQT.TO")
data = ticker.history(period="7d", interval="1m")
pd.DataFrame(data).to_excel("./xeqt.xlsx", index=False)