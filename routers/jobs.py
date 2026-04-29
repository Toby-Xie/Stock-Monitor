from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
from typing import Optional

from jobs.update_valuation_daily import engine, init_table, run_daily_job
from jobs.update_share_structure import init_table as init_share_table, update_share_structure
from jobs.send_email_daily import send_email_job
from jobs.update_market_pe_daily import init_table as init_market_pe_table, update_market_pe_daily
from jobs.update_market_turn_daily import init_table as init_market_turn_table, update_market_turn_daily
from jobs.update_market_margin_daily import init_table as init_margin_table, update_market_margin_daily
from jobs.update_sw_industry_classification import init_table as init_sw_table, update_sw_industry_classification
from jobs.update_industry_turnover import update_industry_turnover

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
    start_date: Optional[str] = Query(
        None,
        description="开始日期 YYYY-MM-DD；不传则默认使用最近交易日"
    ),
    end_date: Optional[str] = Query(
        None,
        description="结束日期 YYYY-MM-DD；不传则默认使用最近交易日"
    ),
    force: bool = Query(
        False,
        description="是否强制运行，true 时周末和已存在数据也继续执行"
    ),
    sleep_sec: float = Query(
        0.02,
        ge=0,
        le=5,
        description="每只股票之间的间隔秒数"
    ),
):
    try:
        result = run_daily_job(
            start_date=start_date,
            end_date=end_date,
            force=force,
            sleep_sec=sleep_sec,
        )
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

@router.post("/share-structure/run")
def run_share_structure_job(
    sleep_sec: float = Query(0.2, ge=0, le=5, description="每只股票之间的间隔秒数"),
    batch_size: int = Query(100, ge=1, le=1000, description="每批写入数量"),
):
    try:
        result = update_share_structure(sleep_sec=sleep_sec, batch_size=batch_size)
        return result or {"status": "ok", "message": "股本结构更新完成"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"股本结构任务执行失败: {e}")

@router.get("/share-structure/status")
def get_share_structure_status():
    try:
        init_share_table()
        sql = text("""
            SELECT
                MAX(change_date) AS latest_change_date,
                COUNT(*) AS total_rows,
                COUNT(DISTINCT code) AS total_stocks
            FROM stock_share_structure
        """)
        with engine.connect() as conn:
            row = conn.execute(sql).fetchone()

        return {
            "latest_change_date": row[0].isoformat() if row and row[0] else None,
            "total_rows": int(row[1] or 0) if row else 0,
            "total_stocks": int(row[2] or 0) if row else 0,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询股本结构状态失败: {e}")

@router.post("/market-pe/run")
def run_market_pe_job(
    start_date: Optional[str] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
):
    try:
        return update_market_pe_daily(start_date=start_date, end_date=end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"market PE 任务执行失败: {e}")
    
@router.post("/market-turn/run")
def run_market_turn_job(
    start_date: Optional[str] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
):
    try:
        return update_market_turn_daily(start_date=start_date, end_date=end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"market Turn 任务执行失败: {e}")
    
@router.post("/margin/run")
def run_margin_job(
    start_date: Optional[str] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
):
    try:
        return update_market_margin_daily(start_date=start_date, end_date=end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"margin 任务执行失败: {e}")
    
@router.post("/industruy-classification/sw/run")
def run_sw_industry_classification_job():
    try:
        result = update_sw_industry_classification()
        return result or {"status": "ok", "message": "SW 行业分类更新完成"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SW 行业分类任务执行失败: {e}")

@router.post("/industry-turnover/run")
def run_industry_turnover_job(
    start_date: Optional[str] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
):
    try:
        result = update_industry_turnover(start_date=start_date, end_date=end_date)
        return result or {"status": "ok", "message": "行业成交额占比更新完成"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"行业成交额占比任务执行失败: {e}")
    
@router.post("/email/send")
def send_email_now():
    try:
        result = send_email_job()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"邮件发送失败: {e}")