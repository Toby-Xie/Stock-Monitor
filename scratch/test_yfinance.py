# scratch/test_ca_yfinance.py

import pandas as pd
import yfinance as yf


SYMBOLS = [
    "RY.TO",    # Royal Bank of Canada
    "TD.TO",    # Toronto-Dominion Bank
    "SHOP.TO",  # Shopify Canada listing
    "ENB.TO",   # Enbridge
    "CNQ.TO",   # Canadian Natural Resources
    "AAPL",     # control sample
]


def safe_float(x):
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def inspect_symbol(symbol: str):
    print("=" * 100)
    print(f"SYMBOL: {symbol}")

    ticker = yf.Ticker(symbol)

    # 1) Price history
    hist = ticker.history(period="2y", auto_adjust=False)

    print("\n[history]")
    print("empty:", hist.empty)
    print("shape:", hist.shape)
    print("columns:", list(hist.columns))

    if not hist.empty:
        hist.index = pd.to_datetime(hist.index).tz_localize(None)
        hist = hist.sort_index()

        print("date range:", hist.index.min(), "->", hist.index.max())
        print(hist.tail(3)[['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'Dividends', 'Stock Splits']])

        close = safe_float(hist["Close"].iloc[-1])
        ma20 = safe_float(hist["Close"].rolling(20).mean().iloc[-1])
        ma60 = safe_float(hist["Close"].rolling(60).mean().iloc[-1])
        high_52w = safe_float(hist["Close"].tail(252).max())
        low_52w = safe_float(hist["Close"].tail(252).min())

        position_52w = None
        if close is not None and high_52w is not None and low_52w is not None and high_52w != low_52w:
            position_52w = (close - low_52w) / (high_52w - low_52w)

        print("\n[derived price metrics]")
        print("close:", close)
        print("ma20:", ma20)
        print("ma60:", ma60)
        print("high_52w:", high_52w)
        print("low_52w:", low_52w)
        print("position_52w:", position_52w)

    # 2) Quarterly income statement
    print("\n[quarterly_income_stmt]")
    income = ticker.quarterly_income_stmt

    print("empty:", income.empty)
    print("shape:", income.shape)

    if not income.empty:
        print("index sample:", list(income.index[:20]))
        print("columns:", list(income.columns[:8]))

        income_t = income.T
        income_t.index = pd.to_datetime(income_t.index).tz_localize(None)
        income_t = income_t.sort_index()

        net_income_candidates = [
            "Net Income Common Stockholders",
            "Net Income",
            "Net Income From Continuing Operation Net Minority Interest",
        ]

        net_income_col = next((c for c in net_income_candidates if c in income_t.columns), None)
        print("net_income_col:", net_income_col)

        if net_income_col:
            print(income_t[[net_income_col]].tail(6))

    # 3) Info fields
    print("\n[info]")
    info = ticker.info or {}

    interesting_keys = [
        "symbol",
        "shortName",
        "currency",
        "exchange",
        "market",
        "regularMarketPrice",
        "currentPrice",
        "previousClose",
        "sharesOutstanding",
        "trailingEps",
        "trailingPE",
        "forwardPE",
        "dividendYield",
    ]

    for key in interesting_keys:
        print(f"{key}: {info.get(key)}")

    # 4) Our PE_TTM calculation
    print("\n[calculated PE_TTM]")
    try:
        if hist.empty or income.empty:
            print("PE_TTM: None, missing hist or income")
            return

        close = safe_float(hist["Close"].iloc[-1])
        income_t = income.T
        income_t.index = pd.to_datetime(income_t.index).tz_localize(None)
        income_t = income_t.sort_index()

        net_income_col = None
        for c in [
            "Net Income Common Stockholders",
            "Net Income",
            "Net Income From Continuing Operation Net Minority Interest",
        ]:
            if c in income_t.columns:
                net_income_col = c
                break

        shares = info.get("sharesOutstanding")

        print("close used:", close)
        print("sharesOutstanding used:", shares)
        print("net_income_col used:", net_income_col)

        if not close or not shares or not net_income_col:
            print("PE_TTM: None, missing close/shares/net income col")
            return

        income_t["NetIncome"] = pd.to_numeric(income_t[net_income_col], errors="coerce")
        income_t["EPS"] = income_t["NetIncome"] / shares
        income_t["TTM_EPS"] = income_t["EPS"].rolling(4).sum()

        latest_ttm_eps = income_t["TTM_EPS"].dropna().iloc[-1] if not income_t["TTM_EPS"].dropna().empty else None
        pe_ttm = close / latest_ttm_eps if latest_ttm_eps else None

        print(income_t[[net_income_col, "EPS", "TTM_EPS"]].tail(6))
        print("latest_ttm_eps:", latest_ttm_eps)
        print("calculated PE_TTM:", pe_ttm)
        print("yfinance trailingPE:", info.get("trailingPE"))
        print("yfinance trailingEps:", info.get("trailingEps"))

    except Exception as e:
        print("PE_TTM calculation failed:", repr(e))


if __name__ == "__main__":
    for symbol in SYMBOLS:
        try:
            inspect_symbol(symbol)
        except Exception as e:
            print("=" * 100)
            print(f"SYMBOL: {symbol}")
            print("FAILED:", repr(e))