import os
import time
from datetime import datetime, timedelta
from typing import Optional

import baostock as bs
import pandas as pd
from sqlalchemy import create_engine, text

PG_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:123456@db:5432/stockdb"
)

engine = create_engine(PG_URL, pool_pre_ping=True)


FULL_FIELDS = [
    "close",
    "amount",
    "tradestatus",
    "is_st",
    "pe_ttm",
    "pb_mrq",
    "turn",
]


def init_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS stock_valuation_daily (
        trade_date DATE NOT NULL,
        code VARCHAR(16) NOT NULL,
        close NUMERIC(20, 6),
        amount NUMERIC(24, 4),
        tradestatus SMALLINT,
        is_st SMALLINT,
        pe_ttm NUMERIC(20, 6),
        pb_mrq NUMERIC(20, 6),
        turn NUMERIC(10, 4),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, code)
    );

    ALTER TABLE stock_valuation_daily
    ADD COLUMN IF NOT EXISTS amount NUMERIC(24, 4);

    CREATE INDEX IF NOT EXISTS idx_stock_valuation_daily_code_date
    ON stock_valuation_daily (code, trade_date);
    """
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def get_all_stocks(trade_date: str) -> list[str]:
    rs = bs.query_all_stock(trade_date)
    data = []

    while rs.error_code == "0" and rs.next():
        data.append(rs.get_row_data())

    df = pd.DataFrame(data, columns=rs.fields)
    if df.empty:
        return []

    codes = df["code"].dropna().tolist()
    codes = [c for c in codes if c.startswith(("sh.", "sz."))]
    return codes


def fetch_one_stock_history(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    rs = bs.query_history_k_data_plus(
        code,
        "date,code,close,amount,tradestatus,isST,peTTM,pbMRQ,turn",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="3",
    )

    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=rs.fields)

    df.rename(
        columns={
            "date": "trade_date",
            "isST": "is_st",
            "peTTM": "pe_ttm",
            "pbMRQ": "pb_mrq",
        },
        inplace=True,
    )

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date

    for col in ["close", "amount", "pe_ttm", "pb_mrq", "turn"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["tradestatus", "is_st"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df = df[df["trade_date"].notna()].copy()

    return df[
        [
            "trade_date",
            "code",
            "close",
            "amount",
            "tradestatus",
            "is_st",
            "pe_ttm",
            "pb_mrq",
            "turn",
        ]
    ]


def upsert_dataframe(df: pd.DataFrame):
    if df.empty:
        return

    temp_table = "tmp_stock_valuation_daily"

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        conn.execute(text(f"""
            CREATE TEMP TABLE {temp_table} (
                trade_date DATE,
                code VARCHAR(16),
                close NUMERIC(20, 6),
                amount NUMERIC(24, 4),
                tradestatus SMALLINT,
                is_st SMALLINT,
                pe_ttm NUMERIC(20, 6),
                pb_mrq NUMERIC(20, 6),
                turn NUMERIC(10, 4)
            ) ON COMMIT DROP
        """))

        df.to_sql(temp_table, conn, if_exists="append", index=False)

        conn.execute(text(f"""
            INSERT INTO stock_valuation_daily (
                trade_date, code, close, amount, tradestatus, is_st, pe_ttm, pb_mrq, turn
            )
            SELECT
                trade_date, code, close, amount, tradestatus, is_st, pe_ttm, pb_mrq, turn
            FROM {temp_table}
            ON CONFLICT (trade_date, code)
            DO UPDATE SET
                close = EXCLUDED.close,
                amount = EXCLUDED.amount,
                tradestatus = EXCLUDED.tradestatus,
                is_st = EXCLUDED.is_st,
                pe_ttm = EXCLUDED.pe_ttm,
                pb_mrq = EXCLUDED.pb_mrq,
                turn = EXCLUDED.turn
        """))


def load_full_existing_codes(trade_date: str) -> set[str]:
    """
    获取某个交易日已经完整入库的股票代码。
    只有所有关键字段都不是 NULL，才认为完整。
    """
    sql = text("""
        SELECT code
        FROM stock_valuation_daily
        WHERE trade_date = :trade_date
          AND close IS NOT NULL
          AND amount IS NOT NULL
          AND tradestatus IS NOT NULL
          AND is_st IS NOT NULL
          AND pe_ttm IS NOT NULL
          AND pb_mrq IS NOT NULL
          AND turn IS NOT NULL
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, {"trade_date": trade_date}).fetchall()

    return {row[0] for row in rows}


def is_weekend(dt: datetime) -> bool:
    return dt.weekday() >= 5


def already_done(trade_date: str) -> bool:
    sql = text("""
        SELECT 1
        FROM stock_valuation_daily
        WHERE trade_date = :trade_date
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"trade_date": trade_date}).fetchone()
        return row is not None


def fetch_and_save_market(
    trade_date: str,
    start_date: str,
    end_date: str,
    sleep_sec: float = 0.02,
):
    init_table()

    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")

    try:
        codes = get_all_stocks(trade_date)
        total = len(codes)

        if total == 0:
            raise RuntimeError(f"{trade_date} 没有获取到股票列表，可能不是交易日或数据源异常")

        existing_full_codes = load_full_existing_codes(trade_date)

        print(
            f"[RUN] trade_date={trade_date}, total_codes={total}, "
            f"existing_full={len(existing_full_codes)}"
        )

        for idx, code in enumerate(codes, 1):
            if code in existing_full_codes:
                print(f"skip(full): {idx}/{total} {code}")
                continue

            try:
                df = fetch_one_stock_history(code, start_date, end_date)

                if not df.empty:
                    upsert_dataframe(df)
                    print(f"success: {idx}/{total} {code}, rows={len(df)}")
                else:
                    print(f"empty:   {idx}/{total} {code}")

                if idx % 200 == 0:
                    print(f"已处理 {idx}/{total}")

                time.sleep(sleep_sec)

            except Exception as e:
                print(f"{code} 失败: {e}")

    finally:
        bs.logout()


def get_last_trading_day(dt: datetime) -> datetime:
    """
    如果是周末：
    - 周六 -> 周五
    - 周日 -> 周五
    """
    if dt.weekday() == 5:
        return dt - timedelta(days=1)
    elif dt.weekday() == 6:
        return dt - timedelta(days=2)
    return dt


def run_daily_job(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force: bool = False,
    sleep_sec: float = 0.02,
) -> dict:
    now = datetime.now()

    default_trade_date = get_last_trading_day(now).strftime("%Y-%m-%d")

    if end_date is None:
        end_date = default_trade_date

    if start_date is None:
        start_date = end_date

    trade_date = end_date

    if not force and is_weekend(now):
        return {
            "status": "skipped",
            "reason": "weekend",
            "trade_date": trade_date,
            "start_date": start_date,
            "end_date": end_date,
            "message": f"{trade_date} 是周末，不运行任务",
        }

    init_table()

    # 原逻辑：只要当天有任意一条数据，就整天跳过。
    # 现在改成逐股票判断完整性，所以这里不要直接 return。
    #
    # if not force and start_date == end_date and already_done(trade_date):
    #     return {
    #         "status": "skipped",
    #         "reason": "already_done",
    #         "trade_date": trade_date,
    #         "start_date": start_date,
    #         "end_date": end_date,
    #         "message": f"{trade_date} 已有数据，不重复运行",
    #     }

    fetch_and_save_market(
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        sleep_sec=sleep_sec,
    )

    return {
        "status": "success",
        "trade_date": trade_date,
        "start_date": start_date,
        "end_date": end_date,
        "message": f"{start_date} 至 {end_date} 数据更新完成",
    }


if __name__ == "__main__":
    result = run_daily_job(force=False, sleep_sec=0.02)
    print(result)