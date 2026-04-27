from datetime import date
from io import BytesIO
import os
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import create_engine, text

PG_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:123456@db:5432/stockdb"
)
engine = create_engine(PG_URL, pool_pre_ping=True)

router = APIRouter(prefix="/market-pe", tags=["market-pe"])


def query_market_pe(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 5000,
) -> pd.DataFrame:
    sql = """
    SELECT
        trade_date,
        market_pe,
        matched_stock_count,
        total_market_value,
        total_implied_profit,
        created_at,
        updated_at
    FROM market_pe_daily
    WHERE (:start_date IS NULL OR trade_date >= :start_date)
      AND (:end_date IS NULL OR trade_date <= :end_date)
    ORDER BY trade_date DESC
    LIMIT :limit
    """

    with engine.connect() as conn:
        df = pd.read_sql(
            text(sql),
            conn,
            params={
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
            },
        )

    return df


@router.get("/scan")
def get_market_pe(
    start_date: Optional[date] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="结束日期 YYYY-MM-DD"),
    limit: int = Query(5000, ge=1, le=20000, description="最大返回行数"),
):
    try:
        df = query_market_pe(start_date=start_date, end_date=end_date, limit=limit)

        return {
            "status": "success",
            "count": len(df),
            "data": df.where(pd.notnull(df), None).to_dict(orient="records"),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询 market PE 失败: {e}")


@router.get("/scan/excel")
def export_market_pe_excel(
    start_date: Optional[date] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="结束日期 YYYY-MM-DD"),
    limit: int = Query(5000, ge=1, le=50000, description="最大导出行数"),
):
    try:
        df = query_market_pe(start_date=start_date, end_date=end_date, limit=limit)

        output = BytesIO()

        export_df = df.copy()
        export_df.rename(
            columns={
                "trade_date": "交易日期",
                "market_pe": "市场PE",
                "matched_stock_count": "匹配股票数",
                "total_market_value": "总市值",
                "total_implied_profit": "隐含净利润",
                "created_at": "创建时间",
                "updated_at": "更新时间",
            },
            inplace=True,
        )

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            export_df.to_excel(writer, index=False, sheet_name="market_pe_daily")

            worksheet = writer.sheets["market_pe_daily"]

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

        filename = "market_pe_daily.xlsx"
        if start_date or end_date:
            start_part = start_date.isoformat() if start_date else "start"
            end_part = end_date.isoformat() if end_date else "end"
            filename = f"market_pe_daily_{start_part}_{end_part}.xlsx"

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"'
        }

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出 market PE Excel 失败: {e}")