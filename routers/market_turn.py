from datetime import date
from io import BytesIO
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from jobs.update_market_turn_daily import engine


router = APIRouter(prefix="/market-turn", tags=["market-turn"])


def query_market_turn(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 5000,
) -> pd.DataFrame:
    sql = """
    SELECT
        trade_date,
        market_turn,
        matched_stock_count,
        total_circulating_market_value,
        weighted_turn_value,
        created_at,
        updated_at
    FROM market_turn_daily
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


@router.get("")
def get_market_turn(
    start_date: Optional[date] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="结束日期 YYYY-MM-DD"),
    limit: int = Query(5000, ge=1, le=20000, description="最大返回行数"),
):
    try:
        df = query_market_turn(
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

        return {
            "status": "success",
            "count": len(df),
            "data": df.where(pd.notnull(df), None).to_dict(orient="records"),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询 market turn 失败: {e}")


@router.get("/export")
def export_market_turn_excel(
    start_date: Optional[date] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="结束日期 YYYY-MM-DD"),
    limit: int = Query(5000, ge=1, le=50000, description="最大导出行数"),
):
    try:
        df = query_market_turn(
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

        output = BytesIO()

        export_df = df.copy()
        export_df.rename(
            columns={
                "trade_date": "交易日期",
                "market_turn": "市场换手率",
                "matched_stock_count": "匹配股票数",
                "total_circulating_market_value": "总流通市值",
                "weighted_turn_value": "换手率加权值",
                "created_at": "创建时间",
                "updated_at": "更新时间",
            },
            inplace=True,
        )

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            export_df.to_excel(writer, index=False, sheet_name="market_turn_daily")

            worksheet = writer.sheets["market_turn_daily"]

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

        filename = "market_turn_daily.xlsx"
        if start_date or end_date:
            start_part = start_date.isoformat() if start_date else "start"
            end_part = end_date.isoformat() if end_date else "end"
            filename = f"market_turn_daily_{start_part}_{end_part}.xlsx"

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"'
        }

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出 market turn Excel 失败: {e}")