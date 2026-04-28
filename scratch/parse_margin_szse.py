import time
import requests
import pandas as pd


def parse_cn_number_to_float(value):
    if value is None:
        return None

    value = str(value).replace(",", "").strip()

    if value in {"", "-", "--", "None", "nan"}:
        return None

    return float(value)


def fetch_szse_margin_summary(date: str | None = None) -> pd.DataFrame:
    """
    SZSE 融资融券交易总量。

    date:
    - None: 返回接口默认最新日期
    - YYYYMMDD / YYYY-MM-DD: 返回指定日期

    返回金额单位统一为“元”：
    - fin_balance
    - fin_buy
    - sec_balance
    - margin_balance

    数量单位统一为“股/份”：
    - sec_volume
    - sec_sell
    """

    def fmt_date(d: str | None) -> str | None:
        if not d:
            return None
        d = str(d)
        if len(d) == 8 and d.isdigit():
            return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        return d

    url = "https://www.szse.cn/api/report/ShowReport/data"

    params = {
        "SHOWTYPE": "JSON",
        "CATALOGID": "1837_xxpl",
        "loading": "first",
        "random": str(time.time()),
    }

    # 如果指定日期，追加 txtDate
    trade_date = fmt_date(date)
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

    # print("status:", resp.status_code)
    # print("url:", resp.url)
    # print("content-type:", resp.headers.get("Content-Type"))
    # print("text head:", resp.text[:300])

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

    # SZSE 页面单位：
    # 金额：亿元
    # 数量：亿股/亿份
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
if __name__ == "__main__":
    df2 = fetch_szse_margin_summary("20260427")
    print(df2)