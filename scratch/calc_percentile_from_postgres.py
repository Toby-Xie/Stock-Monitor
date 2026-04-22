import pandas as pd
from sqlalchemy import create_engine, text

# =========================
# PostgreSQL 连接配置
# =========================
PG_URL = "postgresql+psycopg2://postgres:123456@localhost:5432/stockdb"

engine = create_engine(PG_URL, pool_pre_ping=True)


def calc_percentile(series: pd.Series, current_value: float) -> float | None:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty or pd.isna(current_value):
        return None
    return float((s <= current_value).mean() * 100)


def load_raw_data(start_date: str, end_date: str) -> pd.DataFrame:
    sql = text("""
        SELECT
            trade_date,
            code,
            close,
            tradestatus,
            is_st,
            pe_ttm,
            pb_mrq,
            turn
        FROM stock_valuation_daily
        WHERE trade_date BETWEEN :start_date AND :end_date
        ORDER BY code, trade_date
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={
            "start_date": start_date,
            "end_date": end_date,
        })

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    for col in ["close", "pe_ttm", "pb_mrq"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["tradestatus", "is_st"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def calc_one_stock_latest_percentile(stock_df: pd.DataFrame, data_range: int = 500) -> dict | None:
    """
    对单只股票，从数据库中的历史数据计算最新分位
    """
    if stock_df.empty:
        return None

    stock_df = stock_df.copy()
    stock_df = stock_df[stock_df["tradestatus"] == 1].sort_values("trade_date")
    if stock_df.empty:
        return None

    # 估值异常过滤
    stock_df["pe_ttm"] = stock_df["pe_ttm"].where((stock_df["pe_ttm"] > 0) & (stock_df["pe_ttm"] < 200))
    stock_df["pb_mrq"] = stock_df["pb_mrq"].where((stock_df["pb_mrq"] > 0) & (stock_df["pb_mrq"] < 50))

    pe_valid = stock_df["pe_ttm"].dropna()
    pb_valid = stock_df["pb_mrq"].dropna()
    if len(pe_valid) < 60 and len(pb_valid) < 60:
        return None

    pe_hist = stock_df["pe_ttm"].dropna().tail(data_range)
    pb_hist = stock_df["pb_mrq"].dropna().tail(data_range)

    latest = stock_df.iloc[-1]
    pe_current = latest["pe_ttm"]
    pb_current = latest["pb_mrq"]
    turn = latest["turn"] if "turn" in latest else None

    pe_pct = calc_percentile(pe_hist, pe_current) if pd.notna(pe_current) else None
    pb_pct = calc_percentile(pb_hist, pb_current) if pd.notna(pb_current) else None

    return {
        "date": latest["trade_date"].strftime("%Y-%m-%d"),
        "code": latest["code"],
        "close": latest["close"],
        "isST": int(latest["is_st"]) if pd.notna(latest["is_st"]) else None,
        "peTTM": pe_current,
        "pe_percentile": pe_pct,
        "pbMRQ": pb_current,
        "pb_percentile": pb_pct,
        "sample_pe_days": len(pe_hist),
        "sample_pb_days": len(pb_hist),
        "turn": turn,
    }


def scan_market_valuation_from_db(
    start_date: str,
    end_date: str,
    data_range: int = 500,
) -> pd.DataFrame:
    """
    从 PostgreSQL 读取全市场原始数据，计算最新估值分位
    """
    df = load_raw_data(start_date, end_date)
    if df.empty:
        return pd.DataFrame()

    results = []
    grouped = df.groupby("code", sort=True)

    total = len(grouped)
    for idx, (code, stock_df) in enumerate(grouped, 1):
        try:
            row = calc_one_stock_latest_percentile(stock_df, data_range=data_range)
            if row is not None:
                results.append(row)
                print(f"success: {idx}/{total} {code}")
            else:
                print(f"skip:    {idx}/{total} {code}")
        except Exception as e:
            print(f"{code} 失败: {e}")

    result_df = pd.DataFrame(results)
    if result_df.empty:
        return result_df

    result_df = result_df.sort_values(
        by=["pe_percentile", "pb_percentile"],
        ascending=[True, True],
        na_position="last"
    ).reset_index(drop=True)

    return result_df


if __name__ == "__main__":
    df = scan_market_valuation_from_db(
        start_date="2024-01-01",
        end_date="2026-04-20",
        data_range=500,
    )

    print(df.head(20))
    df.to_csv("C:\\Users\\TobyXie\\Documents\\Stock-Monitor\\all_stock_valuation_scan_from_db.csv", index=False, encoding="utf-8-sig")