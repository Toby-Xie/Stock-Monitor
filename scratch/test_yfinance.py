# scratch/test_xqq_full_history.py

import pandas as pd
import yfinance as yf


SYMBOL = "XQQ.TO"
VIX_SYMBOL = "^VIX"
SPY_SYMBOL = "SPY"
TNX_SYMBOL = "^TNX"
IRX_SYMBOL = "^IRX"

def main():
    print(f"Downloading full history for {SYMBOL}, VIX and SPY...")

    # =========================
    # 1. 下载 XQQ
    # =========================
    xqq = yf.Ticker(SYMBOL)

    hist = xqq.history(
        period="max",
        auto_adjust=False
    )

    if hist.empty:
        print("No XQQ data found.")
        return

    hist.index = pd.to_datetime(hist.index).tz_localize(None)
    hist = hist.sort_index()

    # =========================
    # 2. 下载 VIX
    # =========================
    vix = yf.Ticker(VIX_SYMBOL)

    vix_hist = vix.history(period="max", auto_adjust=False)

    vix_hist.index = pd.to_datetime(vix_hist.index).tz_localize(None)
    vix_hist = vix_hist.sort_index()

    vix_hist = vix_hist[["Close"]].rename(columns={"Close": "VIX"})

    # =========================
    # 3. 下载 SPY（关键新增）
    # =========================
    spy = yf.Ticker(SPY_SYMBOL)

    spy_hist = spy.history(period="max", auto_adjust=False)

    spy_hist.index = pd.to_datetime(spy_hist.index).tz_localize(None)
    spy_hist = spy_hist.sort_index()

    spy_hist = spy_hist[["Close"]].rename(columns={"Close": "SPY"})

    #==========================
    #利率proxy
    #==========================
    tnx = yf.Ticker(TNX_SYMBOL)
    tnx_hist = tnx.history(period="max", auto_adjust=False)
    tnx_hist.index = pd.to_datetime(tnx_hist.index).tz_localize(None)
    tnx_hist = tnx_hist.sort_index()
    tnx_hist = tnx_hist[["Close"]].rename(columns={"Close": "TNX"})
    
    irx = yf.Ticker(IRX_SYMBOL)
    irx_hist = irx.history(period="max", auto_adjust=False)
    irx_hist.index = pd.to_datetime(irx_hist.index).tz_localize(None)
    irx_hist = irx_hist.sort_index()
    irx_hist = irx_hist[["Close"]].rename(columns={"Close": "IRX"})

    # =========================
    # 4. 合并数据
    # =========================
    hist = hist.join(vix_hist, how="left")
    hist = hist.join(spy_hist, how="left")
    hist = hist.join(tnx_hist, how="left")
    hist = hist.join(irx_hist, how="left")
    # forward fill
    hist["VIX"] = hist["VIX"].ffill()
    hist["SPY"] = hist["SPY"].ffill()
    hist["TNX"] = hist["TNX"].ffill()
    hist["IRX"] = hist["IRX"].ffill()
    # =========================
    # 5. 技术指标
    # =========================
    hist["MA20"] = hist["Close"].rolling(20).mean()
    hist["MA60"] = hist["Close"].rolling(60).mean()

    hist["52W_High"] = hist["Close"].rolling(252).max()
    hist["52W_Low"] = hist["Close"].rolling(252).min()

    hist["Position_52W"] = (
        (hist["Close"] - hist["52W_Low"])
        / (hist["52W_High"] - hist["52W_Low"])
    )

    # =========================
    # 6. VIX 特征（强化）
    # =========================
    hist["VIX_MA20"] = hist["VIX"].rolling(20).mean()
    hist["VIX_Ratio"] = hist["VIX"] / hist["VIX_MA20"]

    # ⭐ 推荐再加两个
    hist["VIX_Change_10D"] = hist["VIX"].pct_change(10)

    hist["VIX_Zscore"] = (
        hist["VIX"] - hist["VIX"].rolling(60).mean()
    ) / hist["VIX"].rolling(60).std()

    # =========================
    # 7. SPY 特征（重点）
    # =========================
    hist["SPY_MA200"] = hist["SPY"].rolling(200).mean()

    # 市场趋势（核心）
    hist["SPY_Trend"] = hist["SPY"] / hist["SPY_MA200"]

    # SPY 动量
    hist["SPY_Return_20D"] = hist["SPY"].pct_change(20)

    hist["TNX_MA50"] = hist["TNX"].rolling(50).mean()
    hist["TNX_Change"] = hist["TNX"].pct_change(20)

    # =========================
    # 8. 输出整理
    # =========================
    hist["Symbol"] = SYMBOL

    hist = hist.reset_index()

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

        # VIX
        "VIX",
        "VIX_MA20",
        "VIX_Ratio",
        "VIX_Change_10D",
        "VIX_Zscore",

        # SPY
        "SPY",
        "SPY_MA200",
        "SPY_Trend",
        "SPY_Return_20D",

        # TNX
        "TNX",
        "TNX_MA50",
        "TNX_Change",
        # IRX
        "IRX",

        # 原指标
        "MA20",
        "MA60",
        "52W_High",
        "52W_Low",
        "Position_52W",
    ]

    hist = hist[columns]

    # =========================
    # 9. 保存
    # =========================
    output_file = "./xqq_full_history_with_vix_spy_tnx_irx.csv"

    hist.to_csv(
        output_file,
        index=False,
        encoding="utf-8-sig"
    )

    print(f"\nSaved to: {output_file}")
    print("\nLast 5 rows:")
    print(hist.tail())


if __name__ == "__main__":
    main()