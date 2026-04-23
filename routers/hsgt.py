from io import StringIO, BytesIO
from datetime import date,datetime

import akshare as ak
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse

router = APIRouter(prefix="/hsgt", tags=["hsgt"])


def get_hsgt_fund_flow_summary_df() -> pd.DataFrame:
    df = ak.stock_hsgt_fund_flow_summary_em()
    if df is None or df.empty:
        return pd.DataFrame()
    return df


@router.get("/daily-summary")
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

@router.get("/daily/excel")
def download_hsgt_daily_summary_excel():
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

@router.get("/hist/excel")
def download_hsgt_hist_excel(
    rows: int = Query(100, ge=1, le=5000, description="每个数据集导出的最近行数"),
):
    symbols = ["北向资金", "沪股通", "深股通", "南向资金", "港股通沪", "港股通深"]

    today = datetime.now().strftime("%Y%m%d")
    output = BytesIO()

    success_count = 0

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for symbol in symbols:
            try:
                df = ak.stock_hsgt_hist_em(symbol=symbol)

                if df is None or df.empty:
                    continue

                df_tail = df.tail(rows)

                # Excel sheet 名最长 31 个字符
                sheet_name = symbol[:31]

                df_tail.to_excel(writer, sheet_name=sheet_name, index=False)
                success_count += 1

            except Exception:
                # 单个失败不影响整个文件导出
                continue

    if success_count == 0:
        raise HTTPException(status_code=404, detail="没有获取到任何沪深港通历史数据")

    output.seek(0)
    filename = f"hsgt_hist_{today}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )