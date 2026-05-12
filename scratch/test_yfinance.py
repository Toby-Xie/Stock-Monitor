# scratch/test_xqq_full_history.py

import pandas as pd
import yfinance as yf


SYMBOL = "XQQ.TO"
VIX_SYMBOL = "^VIX"
SPY_SYMBOL = "SPY"
TNX_SYMBOL = "^TNX"
IRX_SYMBOL = "^IRX"

HYG_SYMBOL = "HYG"
IEF_SYMBOL = "IEF"
LQD_SYMBOL = "LQD"
DXY_SYMBOL = "DX-Y.NYB"


def download_close(symbol, name):
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="max", auto_adjust=False)

    if hist.empty:
        print(f"No data found for {symbol}")
        return None

    hist.index = pd.to_datetime(hist.index).tz_localize(None)
    hist = hist.sort_index()

    return hist[["Close"]].rename(columns={"Close": name})


def main():
    print(f"Downloading full history for {SYMBOL}...")

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
    # 2. 下载宏观 / 市场数据
    # =========================
    vix_hist = download_close(VIX_SYMBOL, "VIX")
    spy_hist = download_close(SPY_SYMBOL, "SPY")
    tnx_hist = download_close(TNX_SYMBOL, "TNX")
    irx_hist = download_close(IRX_SYMBOL, "IRX")

    hyg_hist = download_close(HYG_SYMBOL, "HYG")
    ief_hist = download_close(IEF_SYMBOL, "IEF")
    lqd_hist = download_close(LQD_SYMBOL, "LQD")
    dxy_hist = download_close(DXY_SYMBOL, "DXY")

    extra_data = [
        vix_hist,
        spy_hist,
        tnx_hist,
        irx_hist,
        hyg_hist,
        ief_hist,
        lqd_hist,
        dxy_hist,
    ]

    for data in extra_data:
        if data is not None:
            hist = hist.join(data, how="left")

    # forward fill
    fill_cols = [
        "VIX",
        "SPY",
        "TNX",
        "IRX",
        "HYG",
        "IEF",
        "LQD",
        "DXY",
    ]

    for col in fill_cols:
        if col in hist.columns:
            hist[col] = hist[col].ffill()

    # =========================
    # 3. XQQ 技术指标
    # =========================
    hist["MA20"] = hist["Close"].rolling(20).mean()
    hist["MA60"] = hist["Close"].rolling(60).mean()
    hist["MA120"] = hist["Close"].rolling(120).mean()

    hist["52W_High"] = hist["Close"].rolling(252).max()
    hist["52W_Low"] = hist["Close"].rolling(252).min()

    hist["Position_52W"] = (
        (hist["Close"] - hist["52W_Low"])
        / (hist["52W_High"] - hist["52W_Low"])
    )

    # =========================
    # 4. VIX 特征
    # =========================
    hist["VIX_MA20"] = hist["VIX"].rolling(20).mean()
    hist["VIX_Ratio"] = hist["VIX"] / hist["VIX_MA20"]
    hist["VIX_Change_10D"] = hist["VIX"].pct_change(10)

    hist["VIX_Zscore"] = (
        hist["VIX"] - hist["VIX"].rolling(60).mean()
    ) / hist["VIX"].rolling(60).std()

    # =========================
    # 5. SPY 特征
    # =========================
    hist["SPY_MA200"] = hist["SPY"].rolling(200).mean()
    hist["SPY_Trend"] = hist["SPY"] / hist["SPY_MA200"]
    hist["SPY_Return_20D"] = hist["SPY"].pct_change(20)

    # =========================
    # 6. 利率特征
    # =========================
    hist["TNX_MA50"] = hist["TNX"].rolling(50).mean()
    hist["TNX_Change"] = hist["TNX"].pct_change(20)

    # 10Y - 3M 利差
    hist["Yield_Spread"] = hist["TNX"] - hist["IRX"]

    hist["Yield_Spread_MA50"] = hist["Yield_Spread"].rolling(50).mean()
    hist["Yield_Spread_Change_20D"] = hist["Yield_Spread"].diff(20)

    # =========================
    # 7. 信用风险特征
    # =========================

    # High Yield vs Treasury
    hist["HYG_IEF"] = hist["HYG"] / hist["IEF"]
    hist["HYG_IEF_MA50"] = hist["HYG_IEF"].rolling(50).mean()
    hist["HYG_IEF_Ratio"] = hist["HYG_IEF"] / hist["HYG_IEF_MA50"]
    hist["HYG_IEF_Change_20D"] = hist["HYG_IEF"].pct_change(20)

    # Investment Grade vs Treasury
    hist["LQD_IEF"] = hist["LQD"] / hist["IEF"]
    hist["LQD_IEF_MA50"] = hist["LQD_IEF"].rolling(50).mean()
    hist["LQD_IEF_Ratio"] = hist["LQD_IEF"] / hist["LQD_IEF_MA50"]
    hist["LQD_IEF_Change_20D"] = hist["LQD_IEF"].pct_change(20)

    # =========================
    # 8. 美元指数特征
    # =========================
    hist["DXY_MA50"] = hist["DXY"].rolling(50).mean()
    hist["DXY_Trend"] = hist["DXY"] / hist["DXY_MA50"]
    hist["DXY_Return_20D"] = hist["DXY"].pct_change(20)

    # =========================
    # 9. 输出整理
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

        # Rates
        "TNX",
        "TNX_MA50",
        "TNX_Change",
        "IRX",
        "Yield_Spread",
        "Yield_Spread_MA50",
        "Yield_Spread_Change_20D",

        # Credit
        "HYG",
        "IEF",
        "LQD",
        "HYG_IEF",
        "HYG_IEF_MA50",
        "HYG_IEF_Ratio",
        "HYG_IEF_Change_20D",
        "LQD_IEF",
        "LQD_IEF_MA50",
        "LQD_IEF_Ratio",
        "LQD_IEF_Change_20D",

        # Dollar
        "DXY",
        "DXY_MA50",
        "DXY_Trend",
        "DXY_Return_20D",

        # XQQ indicators
        "MA20",
        "MA60",
        "MA120",
        "52W_High",
        "52W_Low",
        "Position_52W",
    ]

    hist = hist[columns]

    # =========================
    # 10. 保存
    # =========================
    output_file = "./xqq_full_history_macro.csv"

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