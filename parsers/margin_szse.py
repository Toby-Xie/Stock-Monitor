import time
from typing import Optional

import requests
import pandas as pd


def parse_cn_number_to_float(value):
    if value is None:
        return None

    value = str(value).replace(",", "").strip()

    if value in {"", "-", "--", "None", "nan"}:
        return None

    return float(value)


def normalize_date_to_dash(date_value: Optional[str]) -> Optional[str]:
    """
    支持：
    - None
    - YYYYMMDD
    - YYYY-MM-DD

    返回：
    - YYYY-MM-DD
    - None
    """
    if not date_value:
        return None

    date_value = str(date_value).strip()

    if len(date_value) == 8 and date_value.isdigit():
        return f"{date_value[:4]}-{date_value[4:6]}-{date_value[6:8]}"

    return date_value


def _fetch_szse_margin_one_day(date: Optional[str] = None) -> pd.DataFrame:
    """
    获取 SZSE 单日融资融券交易总量。

    date:
    - None: 返回接口默认最新日期
    - YYYYMMDD / YYYY-MM-DD: 返回指定日期

    返回金额单位：元
    返回数量单位：股/份
    """

    url = "https://www.szse.cn/api/report/ShowReport/data"

    params = {
        "SHOWTYPE": "JSON",
        "CATALOGID": "1837_xxpl",
        "loading": "first",
        "random": str(time.time()),
    }

    trade_date = normalize_date_to_dash(date)
    if trade_date:
        params["txtDate"] = trade_date

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json",
        "Referer": "https://www.szse.cn/disclosure/margin/margin/index.html",
        "User-Agent": "Mozilla/5.0",
        "X-Request-Type": "ajax",
        "X-Requested-With": "XMLHttpRequest",
    }

    resp = requests.get(url, params=params, headers=headers, timeout=20)
    resp.raise_for_status()

    data = resp.json()

    rows = []
    report_date = trade_date

    if isinstance(data, list):
        for block in data:
            if not isinstance(block, dict):
                continue

            metadata = block.get("metadata", {})
            if (
                metadata.get("catalogid") == "1837_xxpl"
                and metadata.get("tabkey") == "tab1"
            ):
                rows = block.get("data", [])
                report_date = metadata.get("subname") or trade_date
                break

    elif isinstance(data, dict):
        rows = data.get("data", [])

    if not rows:
        return pd.DataFrame()

    raw = rows[0]

    amount_multiplier = 100_000_000
    volume_multiplier = 100_000_000

    def amount_yi(key: str):
        value = parse_cn_number_to_float(raw.get(key))
        return None if value is None else value * amount_multiplier

    def volume_yi(key: str):
        value = parse_cn_number_to_float(raw.get(key))
        return None if value is None else value * volume_multiplier

    result = {
        "trade_date": pd.to_datetime(report_date).date(),
        "exchange": "SZSE",
        "fin_balance": amount_yi("jrrzye"),
        "fin_buy": amount_yi("jrrzmr"),
        "sec_volume": volume_yi("jrrjyl"),
        "sec_sell": volume_yi("jrrjmc"),
        "sec_balance": amount_yi("jrrjye"),
        "margin_balance": amount_yi("jrrzrjye"),
    }

    return pd.DataFrame([result])


def parse_margin_szse(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    sleep_sec: float = 0.1,
) -> pd.DataFrame:
    """
    SZSE 融资融券交易总量。

    start_date/end_date:
    - None
    - YYYYMMDD
    - YYYY-MM-DD

    行为：
    - start_date 和 end_date 都为空：返回接口默认最新日期
    - 只传 start_date：查询 start_date 单日
    - 只传 end_date：查询 end_date 单日
    - 两个都传：逐日查询区间，自动合并有数据的交易日

    返回金额单位：元
    返回数量单位：股/份
    """

    if not start_date and not end_date:
        return _fetch_szse_margin_one_day(None)

    start = normalize_date_to_dash(start_date or end_date)
    end = normalize_date_to_dash(end_date or start_date)

    dates = pd.date_range(start=start, end=end, freq="D")

    frames = []

    for dt in dates:
        date_str = dt.strftime("%Y-%m-%d")

        try:
            df = _fetch_szse_margin_one_day(date_str)
        except Exception as e:
            print(f"SZSE margin 获取失败 {date_str}: {e}")
            continue

        if not df.empty:
            frames.append(df)

        if sleep_sec > 0:
            time.sleep(sleep_sec)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)

    result = result.dropna(subset=["trade_date"])
    result = result.drop_duplicates(subset=["trade_date", "exchange"], keep="last")
    result = result.sort_values("trade_date")

    return result[
        [
            "trade_date",
            "exchange",
            "fin_balance",
            "fin_buy",
            "sec_volume",
            "sec_sell",
            "sec_balance",
            "margin_balance",
        ]
    ]


if __name__ == "__main__":
    df = parse_margin_szse(
        start_date="20260301",
        end_date="20260427",
    )
    print(df)