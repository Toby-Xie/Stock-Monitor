import json
import re
from typing import Optional

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


def normalize_date(date_value: Optional[str]) -> str:
    """
    支持：
    - None
    - YYYYMMDD
    - YYYY-MM-DD

    返回：
    - YYYYMMDD
    - ""
    """
    if not date_value:
        return ""

    date_value = str(date_value).strip()

    if len(date_value) == 10:
        return date_value.replace("-", "")

    return date_value


def _fetch_sse_margin_page(
    start_date: str,
    end_date: str,
    page_no: int,
    page_size: int,
) -> dict:
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
        "beginDate": start_date,
        "endDate": end_date,
        "sqlId": "RZRQ_HZ_INFO",
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.sse.com.cn/market/othersdata/margin/",
        "Accept": "*/*",
    }

    resp = requests.get(url, params=params, headers=headers, timeout=20)
    resp.raise_for_status()

    return parse_json_or_jsonp(resp.text)


def parse_margin_sse(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page_size: int = 100,
) -> pd.DataFrame:
    """
    SSE 融资融券汇总数据。

    start_date/end_date:
    - None
    - YYYYMMDD
    - YYYY-MM-DD

    返回区间内所有页合并后的结果。
    金额单位：元
    """

    start_date = normalize_date(start_date)
    end_date = normalize_date(end_date)

    all_rows = []

    first_data = _fetch_sse_margin_page(
        start_date=start_date,
        end_date=end_date,
        page_no=1,
        page_size=page_size,
    )

    all_rows.extend(first_data.get("result", []))

    page_help = first_data.get("pageHelp") or {}
    page_count = int(page_help.get("pageCount") or 1)

    for page_no in range(2, page_count + 1):
        data = _fetch_sse_margin_page(
            start_date=start_date,
            end_date=end_date,
            page_no=page_no,
            page_size=page_size,
        )
        all_rows.extend(data.get("result", []))

    df = pd.DataFrame(all_rows)

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

    df["trade_date"] = pd.to_datetime(
        df["trade_date"],
        format="%Y%m%d",
        errors="coerce",
    ).dt.date

    df["exchange"] = "SSE"

    df = df.dropna(subset=["trade_date"])
    df = df.drop_duplicates(subset=["trade_date", "exchange"], keep="last")
    df = df.sort_values("trade_date")

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
    df = parse_margin_sse(
        start_date="20260301",
        end_date="20260427",
    )
    print(df)