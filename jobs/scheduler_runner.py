import time
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from update_valuation_daily import run_daily_job
from send_email_daily import send_email_job

def job_wrapper():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[SCHEDULER] trigger at {now}", flush=True)
    try:
        result = run_daily_job(force=False, sleep_sec=0.02)
        print(f"[SCHEDULER] result: {result}", flush=True)
    except Exception as e:
        print(f"[SCHEDULER] error: {e}", flush=True)


if __name__ == "__main__":
    print("[SCHEDULER] starting...", flush=True)

    scheduler = BlockingScheduler(timezone="America/Toronto")

    # 每周一到周五 10:00 运行
    scheduler.add_job(
        job_wrapper,
        trigger=CronTrigger(day_of_week="mon-fri", hour=10, minute=0),
        id="valuation_daily_job",
        replace_existing=True,
    )

    # 邮件发送任务：每天 11:00
    scheduler.add_job(
        send_email_job,
        trigger=CronTrigger(hour=11, minute=0),
        id="send_email_daily_job",
        replace_existing=True,
    )
    
    # scheduler.add_job(
    #     job_wrapper,
    #     trigger=CronTrigger(second=0),
    #     id="valuation_daily_job",
    #     replace_existing=True,
    # )

    print("[SCHEDULER] job registered: mon-fri 10:00 America/Toronto", flush=True)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("[SCHEDULER] stopped", flush=True)