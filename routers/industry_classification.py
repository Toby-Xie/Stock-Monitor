from datetime import date
from io import BytesIO
from typing import Optional

import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import create_engine, text


PG_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:123456@db:5432/stockdb"
)

engine = create_engine(PG_URL, pool_pre_ping=True)

router = APIRouter(prefix="/industry-classification", tags=["industry-classification"])


def query_industry_classification() -> pd.DataFrame:
    sql = """
    SELECT
        code,
        trade_date,
        sw_l1_code,
        sw_l1_name,
        source,
        created_at,
        updated_at
    FROM stock_sw_industry_classification
    ORDER BY trade_date DESC, sw_l1_code ASC, code ASC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)

    return df


@router.get("")
def get_industry_classification():
    try:
        df = query_industry_classification()

        return {
            "status": "success",
            "count": len(df),
            "data": df.where(pd.notnull(df), None).to_dict(orient="records"),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"查询 industry classification 失败: {e}"
        )



@router.get("/excel")
def export_industry_classification_excel():
    try:
        df = query_industry_classification()

        output = BytesIO()

        export_df = df.copy()
        export_df.rename(
            columns={
                "code": "股票代码",
                "trade_date": "交易日期",
                "sw_l1_code": "申万一级行业代码",
                "sw_l1_name": "申万一级行业名称",
                "source": "数据来源",
                "created_at": "创建时间",
                "updated_at": "更新时间",
            },
            inplace=True,
        )

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            export_df.to_excel(
                writer,
                index=False,
                sheet_name="sw_industry_classification"
            )

            worksheet = writer.sheets["sw_industry_classification"]

            for column_cells in worksheet.columns:
                max_length = 0
                column_letter = column_cells[0].column_letter

                for cell in column_cells:
                    value = cell.value
                    if value is not None:
                        max_length = max(max_length, len(str(value)))

                worksheet.column_dimensions[column_letter].width = min(
                    max_length + 2,
                    30
                )

            worksheet.freeze_panes = "A2"

        output.seek(0)

        headers = {
            "Content-Disposition": 'attachment; filename="sw_industry_classification.xlsx"'
        }

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"导出 industry classification Excel 失败: {e}"
        )