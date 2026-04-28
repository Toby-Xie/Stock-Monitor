from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
import akshare as ak
import pandas as pd
from io import BytesIO
import os
from sqlalchemy import create_engine, text
from datetime import date
from typing import Optional

PG_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:123456@db:5432/stockdb",
)

engine = create_engine(PG_URL, pool_pre_ping=True)

router = APIRouter(prefix="/margin", tags=["margin"])


def normalize_margin_detail(df: pd.DataFrame, exchange: str, trade_date: str) -> pd.DataFrame:
    df = df.copy()

    if df is None or df.empty:
        return pd.DataFrame()

    if exchange == "SSE":
        df.rename(
            columns={
                "信用交易日期": "trade_date",
                "标的证券代码": "code",
                "标的证券简称": "name",
                "融资余额": "fin_balance",
                "融资买入额": "fin_buy",
                "融资偿还额": "fin_repay",
                "融券余量": "sec_volume",
                "融券卖出量": "sec_sell",
                "融券偿还量": "sec_repay",
            },
            inplace=True,
        )

        if "trade_date" not in df.columns:
            df["trade_date"] = trade_date

    elif exchange == "SZSE":
        df.rename(
            columns={
                "证券代码": "code",
                "证券简称": "name",
                "融资余额": "fin_balance",
                "融资买入额": "fin_buy",
                "融券余量": "sec_volume",
                "融券卖出量": "sec_sell",
                "融券偿还量": "sec_repay",
                "融券余额": "sec_balance",
                "融资融券余额": "margin_balance",
            },
            inplace=True,
        )

        df["trade_date"] = trade_date

    df["exchange"] = exchange

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(6)
        if exchange == "SSE":
            df["bs_code"] = "sh." + df["code"]
        elif exchange == "SZSE":
            df["bs_code"] = "sz." + df["code"]

    return df


def safe_get_margin_detail(date: str) -> tuple[pd.DataFrame, list[str]]:
    errors = []

    try:
        df_sse = ak.stock_margin_detail_sse(date=date)
        df_sse = normalize_margin_detail(df_sse, "SSE", date)
    except Exception as e:
        errors.append(f"SSE 明细获取失败: {e}")
        df_sse = pd.DataFrame()

    try:
        df_szse = ak.stock_margin_detail_szse(date=date)
        df_szse = normalize_margin_detail(df_szse, "SZSE", date)
    except Exception as e:
        errors.append(f"SZSE 明细获取失败: {e}")
        df_szse = pd.DataFrame()

    df_all = pd.concat([df_sse, df_szse], ignore_index=True)

    common_cols = [
        "trade_date",
        "exchange",
        "code",
        "bs_code",
        "name",
        "fin_balance",
        "fin_buy",
        "fin_repay",
        "sec_volume",
        "sec_sell",
        "sec_repay",
        "sec_balance",
        "margin_balance",
    ]

    existing_cols = [c for c in common_cols if c in df_all.columns]

    if not df_all.empty:
        df_all = df_all[existing_cols]

    return df_all, errors


def normalize_margin_summary(df: pd.DataFrame, exchange: str) -> pd.DataFrame:
    df = df.copy()

    if df is None or df.empty:
        return pd.DataFrame()

    if exchange == "SSE":
        df.rename(
            columns={
                "信用交易日期": "trade_date",
                "融资余额": "fin_balance",
                "融资买入额": "fin_buy",
                "融资偿还额": "fin_repay",
                "融券余量": "sec_volume",
                "融券卖出量": "sec_sell",
                "融券偿还量": "sec_repay",
                "融资融券余额": "margin_balance",
            },
            inplace=True,
        )

    elif exchange == "SZSE":
        df.rename(
            columns={
                "融资融券交易日期": "trade_date",
                "融资余额": "fin_balance",
                "融资买入额": "fin_buy",
                "融资偿还额": "fin_repay",
                "融券余量": "sec_volume",
                "融券卖出量": "sec_sell",
                "融券偿还量": "sec_repay",
                "融券余额": "sec_balance",
                "融资融券余额": "margin_balance",
            },
            inplace=True,
        )

    df["exchange"] = exchange

    common_cols = [
        "trade_date",
        "exchange",
        "fin_balance",
        "fin_buy",
        "fin_repay",
        "sec_volume",
        "sec_sell",
        "sec_repay",
        "sec_balance",
        "margin_balance",
    ]

    existing_cols = [c for c in common_cols if c in df.columns]
    df = df[existing_cols]

    return df

def filter_summary_by_date_and_exchange(
    df_all: pd.DataFrame,
    date: str | None,
    exchange: str,
) -> pd.DataFrame:
    df = df_all.copy()

    if exchange != "ALL":
        df = df[df["exchange"] == exchange]

    if date:
        target_date = pd.to_datetime(date, format="%Y%m%d", errors="coerce")
        if pd.isna(target_date):
            raise HTTPException(status_code=400, detail="date 格式必须是 YYYYMMDD")

        df = df[df["trade_date"] == target_date.date()]

    return df


def aggregate_summary(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "fin_balance": 0,
            "sec_balance": 0,
            "margin_balance": 0,
            "fin_buy": 0,
            "fin_repay": 0,
            "sec_volume": 0,
            "sec_sell": 0,
            "sec_repay": 0,
        }

    numeric_cols = [
        "fin_balance",
        "sec_balance",
        "margin_balance",
        "fin_buy",
        "fin_repay",
        "sec_volume",
        "sec_sell",
        "sec_repay",
    ]

    result = {}

    for col in numeric_cols:
        if col in df.columns:
            result[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).sum()
        else:
            result[col] = 0

    if result["margin_balance"] == 0:
        result["margin_balance"] = result["fin_balance"] + result["sec_balance"]

    return result
def query_margin_summary_from_db(
    start_date: date | None,
    end_date: date | None,
    exchange: str,
) -> pd.DataFrame:
    conditions = []
    params = {}

    if exchange != "ALL":
        conditions.append("exchange = :exchange")
        params["exchange"] = exchange

    if start_date is None and end_date is None:
        latest_conditions = []
        latest_params = {}

        if exchange != "ALL":
            latest_conditions.append("exchange = :exchange")
            latest_params["exchange"] = exchange

        latest_where = ""
        if latest_conditions:
            latest_where = "WHERE " + " AND ".join(latest_conditions)

        latest_sql = f"""
        SELECT MAX(trade_date) AS latest_trade_date
        FROM market_margin_daily
        {latest_where}
        """

        with engine.connect() as conn:
            latest_date = conn.execute(text(latest_sql), latest_params).scalar()

        if latest_date is None:
            return pd.DataFrame()

        conditions.append("trade_date = :latest_date")
        params["latest_date"] = latest_date

    else:
        if start_date is None:
            start_date = end_date

        if end_date is None:
            end_date = start_date

        if start_date > end_date:
            raise HTTPException(
                status_code=400,
                detail="start_date 不能晚于 end_date",
            )

        conditions.append("trade_date >= :start_date")
        conditions.append("trade_date <= :end_date")
        params["start_date"] = start_date
        params["end_date"] = end_date

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    sql = f"""
    SELECT
        trade_date,
        exchange,
        fin_balance,
        fin_buy,
        sec_volume,
        sec_sell,
        sec_balance,
        margin_balance,
        created_at,
        updated_at
    FROM market_margin_daily
    {where_clause}
    ORDER BY trade_date DESC, exchange
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    return df

@router.get("")
def get_margin(
    start_date: date | None = Query(None, description="开始日期，格式 YYYY-MM-DD"),
    end_date: date | None = Query(None, description="结束日期，格式 YYYY-MM-DD"),
    exchange: str = Query("ALL", description="SSE / SZSE / ALL"),
):
    exchange = exchange.upper()
    if exchange not in {"SSE", "SZSE", "ALL"}:
        raise HTTPException(status_code=400, detail="exchange 只能是 SSE, SZSE, ALL")

    df = query_margin_summary_from_db(
        start_date=start_date,
        end_date=end_date,
        exchange=exchange,
    )

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "没有找到对应日期/交易所的两融汇总数据，请先运行 update_market_margin_daily 任务入库",
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "exchange": exchange,
            },
        )

    total = aggregate_summary(df)

    response_start_date = pd.to_datetime(df["trade_date"].min()).strftime("%Y-%m-%d")
    response_end_date = pd.to_datetime(df["trade_date"].max()).strftime("%Y-%m-%d")

    return JSONResponse(
        content={
            "start_date": response_start_date,
            "end_date": response_end_date,
            "exchange": exchange,
            "count": len(df),
            "errors": [],
            "summary": {
                k: float(v) for k, v in total.items()
            },
            "data": df.fillna("").astype(str).to_dict(orient="records"),
        }
    )


@router.get("/margin-detail")
def get_margin_detail(
    date: str = Query(..., description="交易日，格式 YYYYMMDD"),
    exchange: str = Query("ALL", description="SSE / SZSE / ALL"),
):
    if len(date) != 8 or not date.isdigit():
        raise HTTPException(status_code=400, detail="date 格式必须是 YYYYMMDD")

    exchange = exchange.upper()
    if exchange not in {"SSE", "SZSE", "ALL"}:
        raise HTTPException(status_code=400, detail="exchange 只能是 SSE, SZSE, ALL")

    df_all, errors = safe_get_margin_detail(date)

    if df_all.empty:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "没有获取到明细数据，可能是非交易日、接口异常或上游无数据",
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
def export_margin_excel(
    start_date: Optional[date] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="结束日期 YYYY-MM-DD"),
    exchange: str = Query("ALL", description="SSE / SZSE / ALL"),
):
    try:
        exchange = exchange.upper()
        if exchange not in {"SSE", "SZSE", "ALL"}:
            raise HTTPException(status_code=400, detail="exchange 只能是 SSE, SZSE, ALL")

        df = query_margin_summary_from_db(
            start_date=start_date,
            end_date=end_date,
            exchange=exchange,
        )

        if df.empty:
            raise HTTPException(
                status_code=404,
                detail={
                    "message": "没有找到对应日期/交易所的两融汇总数据，请先运行 update_market_margin_daily 任务入库",
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None,
                    "exchange": exchange,
                },
            )

        output = BytesIO()

        export_df = df.copy()
        export_df.rename(
            columns={
                "trade_date": "交易日期",
                "exchange": "交易所",
                "fin_balance": "融资余额",
                "fin_buy": "融资买入额",
                "sec_volume": "融券余量",
                "sec_sell": "融券卖出量",
                "sec_balance": "融券余额",
                "margin_balance": "融资融券余额",
                "created_at": "创建时间",
                "updated_at": "更新时间",
            },
            inplace=True,
        )

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            export_df.to_excel(writer, index=False, sheet_name="market_margin_daily")

            worksheet = writer.sheets["market_margin_daily"]

            for column_cells in worksheet.columns:
                max_length = 0
                column_letter = column_cells[0].column_letter

                for cell in column_cells:
                    value = cell.value
                    if value is not None:
                        max_length = max(max_length, len(str(value)))

                worksheet.column_dimensions[column_letter].width = min(max_length + 2, 30)

            worksheet.freeze_panes = "A2"

        output.seek(0)

        filename = "market_margin_daily.xlsx"
        if start_date or end_date:
            start_part = start_date.isoformat() if start_date else "start"
            end_part = end_date.isoformat() if end_date else "end"
            filename = f"market_margin_daily_{exchange}_{start_part}_{end_part}.xlsx"
        else:
            latest_date = pd.to_datetime(df["trade_date"].max()).strftime("%Y-%m-%d")
            filename = f"market_margin_daily_{exchange}_{latest_date}.xlsx"

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"'
        }

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出 market margin Excel 失败: {e}")

@router.get("/margin-detail/excel")
def get_margin_detail_excel(
    date: str = Query(..., description="交易日，格式 YYYYMMDD"),
    exchange: str = Query("ALL", description="SSE / SZSE / ALL"),
):
    if len(date) != 8 or not date.isdigit():
        raise HTTPException(status_code=400, detail="date 格式必须是 YYYYMMDD")

    exchange = exchange.upper()
    if exchange not in {"SSE", "SZSE", "ALL"}:
        raise HTTPException(status_code=400, detail="exchange 只能是 SSE, SZSE, ALL")

    df_all, errors = safe_get_margin_detail(date)

    if df_all.empty:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "没有获取到明细数据，可能是非交易日、接口异常或上游无数据",
                "errors": errors,
            },
        )

    if exchange != "ALL":
        df_all = df_all[df_all["exchange"] == exchange]

    output = BytesIO()
    df_all.to_excel(output, index=False, engine="openpyxl")
    output.seek(0)

    filename = f"margin_detail_{exchange}_{date}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )