import time
import baostock as bs
import pandas as pd


def calc_percentile(series: pd.Series, current_value: float) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty or pd.isna(current_value):
        return None
    return float((s <= current_value).mean() * 100)


def get_all_stocks(trade_date: str) -> list[str]:
    """
    获取指定交易日的全市场股票列表
    """
    rs = bs.query_all_stock(trade_date)
    data = []
    while rs.error_code == "0" and rs.next():
        data.append(rs.get_row_data())

    df = pd.DataFrame(data, columns=rs.fields)
    if df.empty:
        return []

    # 只保留A股普通股票，排除指数等
    codes = df["code"].dropna().tolist()
    codes = [c for c in codes if c.startswith(("sh.", "sz."))]
    codes = codes[1000:]
    return codes


def fetch_one_stock_valuation(code: str, start_date: str, end_date: str, data_range: int = 500) -> dict | None:
    """
    拉一只股票的估值数据，并计算最新分位
    """
    rs = bs.query_history_k_data_plus(
        code,
        "date,code,close,tradestatus,isST,peTTM,pbMRQ",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="3",
    )

    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=rs.fields)

    # 类型转换
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ["close", "peTTM", "pbMRQ"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["tradestatus"] = pd.to_numeric(df["tradestatus"], errors="coerce")
    df["isST"] = pd.to_numeric(df["isST"], errors="coerce")

    # 只保留正常交易行
    df = df[df["tradestatus"] == 1].copy()
    if df.empty:
        return None

    # 估值异常过滤
    df["peTTM"] = df["peTTM"].where((df["peTTM"] > 0) & (df["peTTM"] < 200))
    df["pbMRQ"] = df["pbMRQ"].where((df["pbMRQ"] > 0) & (df["pbMRQ"] < 50))

    # 如果有效历史太少，跳过
    pe_valid = df["peTTM"].dropna()
    pb_valid = df["pbMRQ"].dropna()
    if len(pe_valid) < 60 and len(pb_valid) < 60:
        return None

    # 只取最近 data_range 个有效窗口来算最新分位
    pe_hist = df["peTTM"].dropna().tail(data_range)
    pb_hist = df["pbMRQ"].dropna().tail(data_range)

    latest = df.iloc[-1]
    pe_current = latest["peTTM"]
    pb_current = latest["pbMRQ"]

    pe_pct = calc_percentile(pe_hist, pe_current) if pd.notna(pe_current) else None
    pb_pct = calc_percentile(pb_hist, pb_current) if pd.notna(pb_current) else None

    return {
        "date": latest["date"].strftime("%Y-%m-%d"),
        "code": latest["code"],
        "close": latest["close"],
        "isST": int(latest["isST"]) if pd.notna(latest["isST"]) else None,
        "peTTM": pe_current,
        "pe_percentile": pe_pct,
        "pbMRQ": pb_current,
        "pb_percentile": pb_pct,
        "sample_pe_days": len(pe_hist),
        "sample_pb_days": len(pb_hist),
    }


def scan_market_valuation(
    trade_date: str,
    start_date: str,
    end_date: str,
    data_range: int = 500,
    sleep_sec: float = 0.03,
) -> pd.DataFrame:
    """
    全市场扫描估值分位
    """
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")

    try:
        codes = get_all_stocks(trade_date)
        results = []

        total = len(codes)
        for idx, code in enumerate(codes, 1):
            try:
                row = fetch_one_stock_valuation(
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    data_range=data_range,
                )
                if row is not None:
                    results.append(row)
                    print("success: ", idx, code)
                else:
                    print("fail: ",idx, code)

                if idx % 200 == 0:
                    print(f"已处理 {idx}/{total}")
                time.sleep(sleep_sec)

            except Exception as e:
                print(f"{code} 失败: {e}")

        result_df = pd.DataFrame(results)
        if result_df.empty:
            return result_df

        # 排序：低估优先
        result_df = result_df.sort_values(
            by=["pe_percentile", "pb_percentile"],
            ascending=[True, True],
            na_position="last"
        ).reset_index(drop=True)

        return result_df

    finally:
        bs.logout()


if __name__ == "__main__":
    df = scan_market_valuation(
        trade_date="2026-04-16",
        start_date="2024-01-01",
        end_date="2026-04-16",
        data_range=500,
        sleep_sec=0.02,
    )

    print(df.head(20))

    # 示例筛选：低估值候选
    # filtered = df[
    #     (df["isST"] == 0) &
    #     (df["pe_percentile"].notna()) &
    #     (df["pb_percentile"].notna()) &
    #     (df["pe_percentile"] <= 20) &
    #     (df["pb_percentile"] <= 20)
    # ].copy()

    # print("\n低估候选：")
    # print(filtered.head(20))

    df.to_csv("/Users/wentaoxie/Downloads/Stock-Monitor/all_stock_valuation_scan.csv", index=False, encoding="utf-8-sig")
    # filtered.to_csv("./low_valuation_candidates.csv", index=False, encoding="utf-8-sig")