from io import StringIO, BytesIO
from datetime import date

import akshare as ak
import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

router = APIRouter(prefix="/hsgt", tags=["hsgt"])


def get_hsgt_fund_flow_summary_df() -> pd.DataFrame:
    df = ak.stock_hsgt_fund_flow_summary_em()
    if df is None or df.empty:
        return pd.DataFrame()
    return df


@router.get("/fund-flow-summary")
def get_hsgt_fund_flow_summary():
    df = get_hsgt_fund_flow_summary_df()

    if df.empty:
        raise HTTPException(status_code=404, detail="没有获取到沪深港通资金流向汇总数据")

    today = date.today().strftime("%Y-%m-%d")

    return JSONResponse(
        content={
            "date": today,
            "count": len(df),
            "data": df.where(pd.notna(df), None).to_dict(orient="records"),
        }
    )

@router.get("/fund-flow-summary/excel")
def download_hsgt_fund_flow_summary_excel():
    df = get_hsgt_fund_flow_summary_df()

    if df.empty:
        raise HTTPException(status_code=404, detail="没有获取到沪深港通资金流向汇总数据")

    today = date.today().strftime("%Y%m%d")
    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="hsgt_summary")

    output.seek(0)
    filename = f"hsgt_fund_flow_summary_em_{today}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )