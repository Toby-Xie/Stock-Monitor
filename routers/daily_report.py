from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from jobs.generate_daily_report import generate_daily_report_bytes

router = APIRouter(prefix="/daily-report", tags=["daily-report"])

@router.get("/excel")
def download_daily_report_excel(
    report_date: Optional[str] = Query(None, description="报告日期 YYYY-MM-DD，不传则使用今天"),
):
    try:
        if report_date is None:
            report_date = datetime.now().strftime("%Y-%m-%d")

        output = generate_daily_report_bytes(report_date=report_date)

        filename = f"daily_report_{report_date}.xlsx"

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"生成 daily report Excel 失败: {e}",
        )