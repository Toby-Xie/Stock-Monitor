import os
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

from parsers.margin_sse import parse_margin_sse
from parsers.margin_szse import parse_margin_szse


PG_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:123456@db:5432/stockdb",
)

engine = create_engine(PG_URL, pool_pre_ping=True)


def fetch_market_margin_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    df_sse = parse_margin_sse(start_date=start_date, end_date=end_date)
    df_szse = parse_margin_szse(start_date=start_date, end_date=end_date)

    return pd.concat([df_sse, df_szse], ignore_index=True)


def init_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS market_margin_daily (
        trade_date DATE NOT NULL,
        exchange VARCHAR(16) NOT NULL,
        fin_balance NUMERIC(30, 4),
        fin_buy NUMERIC(30, 4),
        sec_volume NUMERIC(30, 4),
        sec_sell NUMERIC(30, 4),
        sec_balance NUMERIC(30, 4),
        margin_balance NUMERIC(30, 4),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, exchange)
    );

    CREATE INDEX IF NOT EXISTS idx_market_margin_daily_trade_date
    ON market_margin_daily (trade_date);
    """

    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def upsert_dataframe(df: pd.DataFrame):
    if df.empty:
        return

    temp_table = "tmp_market_margin_daily"

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

        conn.execute(text(f"""
            CREATE TEMP TABLE {temp_table} (
                trade_date DATE,
                exchange VARCHAR(16),
                fin_balance NUMERIC(30, 4),
                fin_buy NUMERIC(30, 4),
                sec_volume NUMERIC(30, 4),
                sec_sell NUMERIC(30, 4),
                sec_balance NUMERIC(30, 4),
                margin_balance NUMERIC(30, 4)
            ) ON COMMIT DROP
        """))

        df.to_sql(temp_table, conn, if_exists="append", index=False)

        conn.execute(text(f"""
            INSERT INTO market_margin_daily (
                trade_date,
                exchange,
                fin_balance,
                fin_buy,
                sec_volume,
                sec_sell,
                sec_balance,
                margin_balance,
                updated_at
            )
            SELECT
                trade_date,
                exchange,
                fin_balance,
                fin_buy,
                sec_volume,
                sec_sell,
                sec_balance,
                margin_balance,
                CURRENT_TIMESTAMP
            FROM {temp_table}
            ON CONFLICT (trade_date, exchange)
            DO UPDATE SET
                fin_balance = EXCLUDED.fin_balance,
                fin_buy = EXCLUDED.fin_buy,
                sec_volume = EXCLUDED.sec_volume,
                sec_sell = EXCLUDED.sec_sell,
                sec_balance = EXCLUDED.sec_balance,
                margin_balance = EXCLUDED.margin_balance,
                updated_at = CURRENT_TIMESTAMP
        """))


def update_market_margin_daily(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """
    market margin 更新任务：
    1. 创建 market_margin_daily 表
    2. 从 SSE + SZSE 获取融资融券汇总数据
    3. upsert 到 market_margin_daily

    start_date / end_date 支持：
    - None
    - YYYYMMDD
    - YYYY-MM-DD
    """

    init_table()

    df = fetch_market_margin_summary(
        start_date=start_date,
        end_date=end_date,
    )

    if df.empty:
        return {
            "status": "skipped",
            "reason": "empty_result",
            "start_date": start_date,
            "end_date": end_date,
            "message": "没有获取到 market margin 数据",
        }

    upsert_dataframe(df)

    total = {
        "fin_balance": float(df["fin_balance"].sum()),
        "sec_balance": float(df["sec_balance"].sum()),
        "margin_balance": float(df["margin_balance"].sum()),
    }

    return {
        "status": "success",
        "rows": len(df),
        "start_date": str(df["trade_date"].min()),
        "end_date": str(df["trade_date"].max()),
        "summary": total,
        "message": f"market margin 更新完成，共 {len(df)} 条",
    }


if __name__ == "__main__":
    result = update_market_margin_daily()
    print(result)