import json
import re
import requests
import pandas as pd


def parse_json_or_jsonp(text: str) -> dict:
    text = text.strip()

    if not text:
        raise ValueError("SSE response is empty")

    if text.startswith("{"):
        return json.loads(text)

    match = re.search(r"^[^(]+\((.*)\)\s*;?$", text, re.S)
    if match:
        return json.loads(match.group(1))

    raise ValueError(f"Response is not JSON/JSONP: {text[:500]}")


def fetch_sse_margin_summary(
    start_date: str | None = None,
    end_date: str | None = None,
    page_no: int = 1,
    page_size: int = 25,
) -> pd.DataFrame:
    """
    SSE 融资融券汇总数据。

    start_date/end_date: YYYYMMDD，可不传；不传时 SSE 会返回默认最近数据。
    """

    url = "https://query.sse.com.cn/commonSoaQuery.do"

    params = {
        "jsonCallBack": "jsonpCallback",
        "isPagination": "true",
        "pageHelp.pageSize": str(page_size),
        "pageHelp.pageNo": str(page_no),
        "pageHelp.beginPage": str(page_no),
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": str(page_no),
        "stockCode": "",
        "beginDate": start_date or "",
        "endDate": end_date or "",
        "sqlId": "RZRQ_HZ_INFO",
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.sse.com.cn/market/othersdata/margin/",
        "Accept": "*/*",
    }

    resp = requests.get(url, params=params, headers=headers, timeout=20)

    print("status:", resp.status_code)
    print("url:", resp.url)
    print("content-type:", resp.headers.get("Content-Type"))
    print("text head:", resp.text[:200])

    resp.raise_for_status()

    data = parse_json_or_jsonp(resp.text)
    rows = data.get("result", [])

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df = df.rename(
        columns={
            "opDate": "trade_date",
            "rzye": "fin_balance",
            "rzmre": "fin_buy",
            "rqyl": "sec_volume",
            "rqylje": "sec_balance",
            "rqmcl": "sec_sell",
            "rzrqjyzl": "margin_balance",
        }
    )

    numeric_cols = [
        "fin_balance",
        "fin_buy",
        "sec_volume",
        "sec_balance",
        "sec_sell",
        "margin_balance",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce").dt.date
    df["exchange"] = "SSE"

    return df[
        [
            "trade_date",
            "exchange",
            "fin_balance",
            "fin_buy",
            "sec_volume",
            "sec_balance",
            "sec_sell",
            "margin_balance",
        ]
    ]


if __name__ == "__main__":
    df = fetch_sse_margin_summary(
        start_date="20260427",
        end_date="20260427",
        page_size=25,
    )
    print(df)