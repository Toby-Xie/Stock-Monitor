from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
from typing import Optional

from jobs.update_valuation_daily import engine, init_table, run_daily_job
from jobs.send_email_daily import send_email_job
router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("/health")
def jobs_health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"数据库连接异常: {e}")


@router.post("/valuation/run")
def run_valuation_job(
    force: bool = Query(False, description="是否强制运行，true 时周末和已存在数据也继续执行"),
    sleep_sec: float = Query(0.02, ge=0, le=5, description="每只股票之间的间隔秒数"),
):
    try:
        result = run_daily_job(force=force, sleep_sec=sleep_sec)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"任务执行失败: {e}")


@router.get("/valuation/status")
def get_valuation_job_status():
    try:
        init_table()
        sql = text("""
            SELECT
                MAX(trade_date) AS latest_trade_date,
                COUNT(*) AS total_rows
            FROM stock_valuation_daily
        """)
        with engine.connect() as conn:
            row = conn.execute(sql).fetchone()

        latest_trade_date = None
        total_rows = 0

        if row is not None:
            latest_trade_date = row[0].isoformat() if row[0] else None
            total_rows = int(row[1] or 0)

        return {
            "latest_trade_date": latest_trade_date,
            "total_rows": total_rows,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询任务状态失败: {e}")

@router.post("/email/send")
def send_email_now():
    try:
        result = send_email_job()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"邮件发送失败: {e}")