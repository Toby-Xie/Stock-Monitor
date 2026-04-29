from io import BytesIO
from typing import Optional

import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import create_engine, text


PG_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:123456@db:5432/stockdb"
)

engine = create_engine(PG_URL, pool_pre_ping=True)

router = APIRouter(prefix="/industry-turnover", tags=["industry-turnover"])


def load_industry_turnover_data(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 5000,
) -> pd.DataFrame:
    sql = text("""
        SELECT
            trade_date,
            sw_l1_code,
            sw_l1_name,
            industry_amount,
            total_amount,
            industry_amount_ratio,
            stock_count,
            created_at,
            updated_at
        FROM industry_turnover_daily
        WHERE (:start_date IS NULL OR trade_date >= :start_date)
          AND (:end_date IS NULL OR trade_date <= :end_date)
        ORDER BY trade_date DESC, industry_amount_ratio DESC, sw_l1_code ASC
        LIMIT :limit
    """)

    with engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
            },
        )

    if df.empty:
        return df

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")

    for col in [
        "industry_amount",
        "total_amount",
        "industry_amount_ratio",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "stock_count" in df.columns:
        df["stock_count"] = pd.to_numeric(df["stock_count"], errors="coerce")

    return df


@router.get("/scan")
def get_industry_turnover_scan(
    start_date: Optional[str] = Query(None, description="开始日期，格式 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期，格式 YYYY-MM-DD"),
    limit: int = Query(5000, ge=1, le=50000, description="返回记录数"),
):
    if start_date:
        try:
            pd.to_datetime(start_date, format="%Y-%m-%d")
        except Exception:
            raise HTTPException(status_code=400, detail="start_date 日期格式必须是 YYYY-MM-DD")

    if end_date:
        try:
            pd.to_datetime(end_date, format="%Y-%m-%d")
        except Exception:
            raise HTTPException(status_code=400, detail="end_date 日期格式必须是 YYYY-MM-DD")

    try:
        df = load_industry_turnover_data(
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

        if df.empty:
            raise HTTPException(status_code=404, detail="没有查到行业成交额数据")

        df = df.copy()
        df["trade_date"] = df["trade_date"].dt.strftime("%Y-%m-%d")

        if "created_at" in df.columns:
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce").dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        if "updated_at" in df.columns:
            df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce").dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        return JSONResponse(
            content={
                "start_date": start_date,
                "end_date": end_date,
                "count": len(df),
                "data": df.where(pd.notna(df), None).to_dict(orient="records"),
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询 industry turnover 失败: {e}")


@router.get("/scan/excel")
def download_industry_turnover_excel(
    start_date: Optional[str] = Query(None, description="开始日期，格式 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期，格式 YYYY-MM-DD"),
    limit: int = Query(50000, ge=1, le=200000, description="最大导出行数"),
):
    if start_date:
        try:
            pd.to_datetime(start_date, format="%Y-%m-%d")
        except Exception:
            raise HTTPException(status_code=400, detail="start_date 日期格式必须是 YYYY-MM-DD")

    if end_date:
        try:
            pd.to_datetime(end_date, format="%Y-%m-%d")
        except Exception:
            raise HTTPException(status_code=400, detail="end_date 日期格式必须是 YYYY-MM-DD")

    try:
        df = load_industry_turnover_data(
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

        if df.empty:
            raise HTTPException(status_code=404, detail="没有查到行业成交额数据")

        export_df = df.copy()

        if "trade_date" in export_df.columns:
            export_df["trade_date"] = pd.to_datetime(
                export_df["trade_date"],
                errors="coerce",
            ).dt.strftime("%Y-%m-%d")

        export_df.rename(
            columns={
                "trade_date": "交易日期",
                "sw_l1_code": "申万一级行业代码",
                "sw_l1_name": "申万一级行业名称",
                "industry_amount": "行业成交额",
                "total_amount": "全市场成交额",
                "industry_amount_ratio": "行业成交额占比",
                "stock_count": "股票数量",
                "created_at": "创建时间",
                "updated_at": "更新时间",
            },
            inplace=True,
        )

        output = BytesIO()

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            export_df.to_excel(
                writer,
                index=False,
                sheet_name="industry_turnover",
            )

            worksheet = writer.sheets["industry_turnover"]

            for column_cells in worksheet.columns:
                max_length = 0
                column_letter = column_cells[0].column_letter

                for cell in column_cells:
                    value = cell.value
                    if value is not None:
                        max_length = max(max_length, len(str(value)))

                worksheet.column_dimensions[column_letter].width = min(
                    max_length + 2,
                    30,
                )

            worksheet.freeze_panes = "A2"

        output.seek(0)

        filename = "industry_turnover_daily.xlsx"
        if start_date or end_date:
            start_part = start_date if start_date else "start"
            end_part = end_date if end_date else "end"
            filename = f"industry_turnover_daily_{start_part}_{end_part}.xlsx"

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出 industry turnover Excel 失败: {e}")