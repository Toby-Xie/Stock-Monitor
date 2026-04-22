from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
import akshare as ak
import pandas as pd
from io import BytesIO
from datetime import datetime

router = APIRouter(prefix="/margin", tags=["margin"])

def normalize_margin(df: pd.DataFrame, exchange: str, trade_date: str) -> pd.DataFrame:
    df = df.copy()

    if df is None or df.empty:
        return pd.DataFrame()

    if exchange == "SSE":
        df.rename(columns={
            "信用交易日期": "trade_date",
            "标的证券代码": "code",
            "标的证券简称": "name",
            "融资余额": "fin_balance",
            "融资买入额": "fin_buy",
            "融资偿还额": "fin_repay",
            "融券余量": "sec_volume",
            "融券卖出量": "sec_sell",
            "融券偿还量": "sec_repay",
        }, inplace=True)

        if "trade_date" not in df.columns:
            df["trade_date"] = trade_date

    elif exchange == "SZSE":
        df.rename(columns={
            "证券代码": "code",
            "证券简称": "name",
            "融资余额": "fin_balance",
            "融资买入额": "fin_buy",
            "融券余量": "sec_volume",
            "融券卖出量": "sec_sell",
            "融券偿还量": "sec_repay",
            "融券余额": "sec_balance",
            "融资融券余额": "margin_balance",
        }, inplace=True)

        df["trade_date"] = trade_date

    df["exchange"] = exchange
    return df


def safe_get_margin(date: str) -> tuple[pd.DataFrame, list[str]]:
    errors = []

    try:
        df_sse = ak.stock_margin_detail_sse(date=date)
        df_sse = normalize_margin(df_sse, "SSE", date)
    except Exception as e:
        errors.append(f"SSE 获取失败: {e}")
        df_sse = pd.DataFrame()

    try:
        df_szse = ak.stock_margin_detail_szse(date=date)
        df_szse = normalize_margin(df_szse, "SZSE", date)
    except Exception as e:
        errors.append(f"SZSE 获取失败: {e}")
        df_szse = pd.DataFrame()

    df_all = pd.concat([df_sse, df_szse], ignore_index=True)

    common_cols = [
        "trade_date", "exchange", "code", "name",
        "fin_balance", "fin_buy", "fin_repay",
        "sec_volume", "sec_sell", "sec_repay",
        "sec_balance", "margin_balance",
    ]
    existing_cols = [c for c in common_cols if c in df_all.columns]
    if not df_all.empty:
        df_all = df_all[existing_cols]

    return df_all, errors



@router.get("")
def get_margin(
    date: str = Query(..., description="交易日，格式 YYYYMMDD"),
    exchange: str = Query("ALL", description="SSE / SZSE / ALL"),
):
    if len(date) != 8 or not date.isdigit():
        raise HTTPException(status_code=400, detail="date 格式必须是 YYYYMMDD")

    exchange = exchange.upper()
    if exchange not in {"SSE", "SZSE", "ALL"}:
        raise HTTPException(status_code=400, detail="exchange 只能是 SSE, SZSE, ALL")

    df_all, errors = safe_get_margin(date)

    if df_all.empty:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "没有获取到数据，可能是非交易日、接口异常或上游无数据",
                "errors": errors,
            },
        )

    if exchange != "ALL":
        df_all = df_all[df_all["exchange"] == exchange]

    return JSONResponse(
        content={
            "date": date,
            "exchange": exchange,
            "count": len(df_all),
            "errors": errors,
            "data": df_all.fillna("").to_dict(orient="records"),
        }
    )


@router.get("/excel")
def get_margin_excel(
    date: str = Query(..., description="交易日，格式 YYYYMMDD"),
    exchange: str = Query("ALL", description="SSE / SZSE / ALL"),
):
    if len(date) != 8 or not date.isdigit():
        raise HTTPException(status_code=400, detail="date 格式必须是 YYYYMMDD")

    exchange = exchange.upper()
    if exchange not in {"SSE", "SZSE", "ALL"}:
        raise HTTPException(status_code=400, detail="exchange 只能是 SSE, SZSE, ALL")

    df_all, errors = safe_get_margin(date)

    if df_all.empty:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "没有获取到数据，可能是非交易日、接口异常或上游无数据",
                "errors": errors,
            },
        )

    if exchange != "ALL":
        df_all = df_all[df_all["exchange"] == exchange]

    output = BytesIO()
    df_all.to_excel(output, index=False, engine="openpyxl")
    output.seek(0)

    filename = f"margin_{exchange}_{date}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )