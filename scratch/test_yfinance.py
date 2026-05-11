# scratch/test_xqq_full_history.py

import pandas as pd
import yfinance as yf


SYMBOL = "XQQ.TO"


def main():
    print(f"Downloading full history for {SYMBOL} ...")

    ticker = yf.Ticker(SYMBOL)

    # 获取全部历史数据
    hist = ticker.history(
        period="max",
        auto_adjust=False
    )

    if hist.empty:
        print("No history data found.")
        return

    # 时间处理
    hist.index = pd.to_datetime(hist.index).tz_localize(None)
    hist = hist.sort_index()

    # 添加技术指标
    hist["MA20"] = hist["Close"].rolling(20).mean()
    hist["MA60"] = hist["Close"].rolling(60).mean()

    # 52周高低
    hist["52W_High"] = hist["Close"].rolling(252).max()
    hist["52W_Low"] = hist["Close"].rolling(252).min()

    # 52周位置
    hist["Position_52W"] = (
        (hist["Close"] - hist["52W_Low"])
        / (hist["52W_High"] - hist["52W_Low"])
    )

    # 增加 symbol 列
    hist["Symbol"] = SYMBOL

    # index 转列
    hist = hist.reset_index()

    # 列顺序
    columns = [
        "Date",
        "Symbol",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume",
        "Dividends",
        "Stock Splits",
        "MA20",
        "MA60",
        "52W_High",
        "52W_Low",
        "Position_52W",
    ]

    hist = hist[columns]

    # 保存 CSV
    output_file = "/Users/wentaoxie/Stock-Monitor/scratch/xqq_full_history.csv"

    hist.to_csv(
        output_file,
        index=False,
        encoding="utf-8-sig"
    )

    print(f"\nSaved full history to: {output_file}")
    print("\nLast 5 rows:")
    print(hist.tail())


if __name__ == "__main__":
    main()