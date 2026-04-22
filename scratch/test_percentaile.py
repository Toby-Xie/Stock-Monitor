import baostock as bs
import pandas as pd


def calc_percentile(series: pd.Series, current_value: float) -> float:
    series = series.dropna()
    return (series <= current_value).mean() * 100

def get_daily_valuation(start_date: str, end_date: str, frequency: str = "d", data_range: int = 500) -> pd.DataFrame:
    # 登录
    bs.login()

    # 拉数据
    rs = bs.query_history_k_data_plus(
        "sh.600519",
        "date,open,high,low,close,volume,peTTM,pbMRQ",
        start_date='2024-01-01',
        end_date='2026-04-16',
        frequency="d",
        adjustflag="3"
    )

    data = []
    while rs.next():
        data.append(rs.get_row_data())

    df = pd.DataFrame(data, columns=rs.fields)

    # 退出
    bs.logout()

    # =========================
    # 数据清洗（关键）
    # =========================
    df["date"] = pd.to_datetime(df["date"])
    df["peTTM"] = pd.to_numeric(df["peTTM"], errors="coerce")
    df["pbMRQ"] = pd.to_numeric(df["pbMRQ"], errors="coerce")

    # 去掉异常值（非常重要）
    df = df[(df["peTTM"] > 0) & (df["peTTM"] < 200)]

    # =========================
    # 计算历史分位（滚动）
    # =========================
    pe_percentiles = []
    pb_percentiles = []

    for i in range(len(df)):
        start_idx = max(0, i - data_range)  # 最多看最近data_range个交易日
        pe_hist = df["peTTM"].iloc[start_idx:i+1]
        pb_hist = df["pbMRQ"].iloc[start_idx:i+1]

        pe_current = df["peTTM"].iloc[i]
        pb_current = df["pbMRQ"].iloc[i]

        pe_percentiles.append(calc_percentile(pe_hist, pe_current))
        pb_percentiles.append(calc_percentile(pb_hist, pb_current))

    df["pe_percentile"] = pe_percentiles
    df["pb_percentile"] = pb_percentiles

    
    return df[-1:]  # 只返回最新一行
df = get_daily_valuation(start_date="20240101", end_date="20260416")
# =========================
# 输出结果
# =========================
print(df[["date", "close", "peTTM", "pe_percentile", "pbMRQ", "pb_percentile"]].tail())