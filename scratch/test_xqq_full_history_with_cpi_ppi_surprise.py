# scratch/test_xqq_full_history.py

from pathlib import Path

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

INFLATION_EVENTS_FILE = "./USD_CPI_PPI_filtered.csv"


def parse_ff_number(value):
    """Convert Forex Factory values like '0.3%', '1.2', '<0.1%' to float."""
    if pd.isna(value):
        return pd.NA

    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "n/a", "na", "-"}:
        return pd.NA

    # Forex Factory sometimes uses symbols such as <, >, %, commas.
    text = (
        text.replace("%", "")
            .replace(",", "")
            .replace("<", "")
            .replace(">", "")
            .strip()
    )

    try:
        return float(text)
    except ValueError:
        return pd.NA


def safe_event_name(event):
    """Make stable column names from event names, e.g. 'Core CPI m/m' -> 'Core_CPI_mm'."""
    text = str(event).strip()
    text = text.replace("/", "")
    text = text.replace(" ", "_")
    text = text.replace("-", "_")
    return text


def load_inflation_surprises(events_file, trading_dates):
    """
    Load USD CPI/PPI events and calculate surprise = Actual - Forecast.

    The Forex Factory file is release-level data. CPI/PPI releases are normally at
    8:30am ET, before the North American cash equity open, so the event is aligned
    to the same XQQ trading date. If XQQ is closed that day, the event is moved to
    the next available XQQ trading date.
    """
    events_path = Path(events_file)
    if not events_path.exists():
        print(f"Inflation events file not found: {events_file}")
        return None

    events = pd.read_csv(events_path)

    required_cols = {"Day", "Currency", "Event", "Actual", "Forecast"}
    missing_cols = required_cols - set(events.columns)
    if missing_cols:
        raise ValueError(f"Inflation events file is missing columns: {missing_cols}")

    events = events.copy()
    events["Date"] = pd.to_datetime(
        events["Day"].astype(str),
        format="%Y%m%d",
        errors="coerce"
    )

    # ✅ Fix: force Date dtype to datetime64[ns]
    events["Date"] = pd.to_datetime(events["Date"], errors="coerce").dt.tz_localize(None)
    events["Date"] = events["Date"].astype("datetime64[ns]")

    events = events[
        (events["Currency"].astype(str).str.upper() == "USD")
        & (events["Event"].astype(str).str.contains("CPI|PPI", case=False, na=False))
        & events["Date"].notna()
    ].copy()

    events["Actual_Value"] = events["Actual"].apply(parse_ff_number)
    events["Forecast_Value"] = events["Forecast"].apply(parse_ff_number)

    if "Previous" in events.columns:
        events["Previous_Value"] = events["Previous"].apply(parse_ff_number)

    events["Actual_Value"] = pd.to_numeric(events["Actual_Value"], errors="coerce")
    events["Forecast_Value"] = pd.to_numeric(events["Forecast_Value"], errors="coerce")
    events["Surprise"] = events["Actual_Value"] - events["Forecast_Value"]

    events = events.dropna(subset=["Actual_Value", "Forecast_Value", "Surprise"])

    if events.empty:
        print("No usable CPI/PPI surprise rows found.")
        return None

    # Align release dates to XQQ trading days.
    trading_calendar = pd.DataFrame({
        "TradingDate": pd.to_datetime(pd.Index(trading_dates), errors="coerce")
    })

    # ✅ Fix: force TradingDate dtype to datetime64[ns]
    trading_calendar["TradingDate"] = pd.to_datetime(
        trading_calendar["TradingDate"],
        errors="coerce"
    ).dt.tz_localize(None)

    trading_calendar["TradingDate"] = trading_calendar["TradingDate"].astype("datetime64[ns]")

    trading_calendar = (
        trading_calendar
        .dropna(subset=["TradingDate"])
        .sort_values("TradingDate")
        .drop_duplicates()
        .reset_index(drop=True)
    )

    events = (
        events
        .sort_values("Date")
        .reset_index(drop=True)
    )

    events = pd.merge_asof(
        events,
        trading_calendar,
        left_on="Date",
        right_on="TradingDate",
        direction="forward"
    )

    events = events.dropna(subset=["TradingDate"])

    # Per-event columns: Core_CPI_mm_Surprise, CPI_yy_Actual, etc.
    events["Event_Key"] = events["Event"].apply(safe_event_name)

    surprise_pivot = events.pivot_table(
        index="TradingDate",
        columns="Event_Key",
        values="Surprise",
        aggfunc="mean"
    ).add_suffix("_Surprise")

    actual_pivot = events.pivot_table(
        index="TradingDate",
        columns="Event_Key",
        values="Actual_Value",
        aggfunc="mean"
    ).add_suffix("_Actual")

    forecast_pivot = events.pivot_table(
        index="TradingDate",
        columns="Event_Key",
        values="Forecast_Value",
        aggfunc="mean"
    ).add_suffix("_Forecast")

    features = pd.concat([surprise_pivot, actual_pivot, forecast_pivot], axis=1)

    # Compact summary columns for modelling.
    summary = events.groupby("TradingDate").agg(
        Inflation_Event_Count=("Event", "count"),
        Inflation_Surprise_Mean=("Surprise", "mean"),
        Inflation_Surprise_Sum=("Surprise", "sum"),
        Inflation_Surprise_MaxAbs=("Surprise", lambda x: x.abs().max()),
    )

    summary["Inflation_Release_Flag"] = 1

    # Separate CPI and PPI broad surprise summaries.
    events["Is_CPI"] = events["Event"].str.contains("CPI", case=False, na=False)
    events["Is_PPI"] = events["Event"].str.contains("PPI", case=False, na=False)

    cpi_summary = (
        events[events["Is_CPI"]]
        .groupby("TradingDate")["Surprise"]
        .mean()
        .rename("CPI_Surprise_Mean")
    )

    ppi_summary = (
        events[events["Is_PPI"]]
        .groupby("TradingDate")["Surprise"]
        .mean()
        .rename("PPI_Surprise_Mean")
    )

    features = pd.concat([features, summary, cpi_summary, ppi_summary], axis=1)
    features.index.name = "Date"

    return features.sort_index()




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
    # 3. CPI / PPI surprise 特征
    # =========================
    inflation_features = load_inflation_surprises(
        INFLATION_EVENTS_FILE,
        hist.index
    )

    if inflation_features is not None:
        hist = hist.join(inflation_features, how="left")

        # Release-day features: no release means 0 flag, but surprise columns stay NaN.
        if "Inflation_Release_Flag" in hist.columns:
            hist["Inflation_Release_Flag"] = hist["Inflation_Release_Flag"].fillna(0)

        if "Inflation_Event_Count" in hist.columns:
            hist["Inflation_Event_Count"] = hist["Inflation_Event_Count"].fillna(0)

        # Optional event-window features useful for modelling.
        for col in ["CPI_Surprise_Mean", "PPI_Surprise_Mean", "Inflation_Surprise_Mean"]:
            if col in hist.columns:
                hist[f"{col}_Last"] = hist[col].ffill()
                hist[f"{col}_20D_Sum"] = hist[col].fillna(0).rolling(20).sum()

    # =========================
    # 4. XQQ 技术指标
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
    inflation_columns = [
        col for col in hist.columns
        if (
            "CPI" in col
            or "PPI" in col
            or col.startswith("Inflation_")
        )
    ]

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

        # CPI / PPI surprise
        *inflation_columns,

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