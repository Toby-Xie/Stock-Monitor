import os
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text


PG_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:123456@db:5432/stockdb"
)

engine = create_engine(PG_URL, pool_pre_ping=True)


def init_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS industry_turnover_daily (
        trade_date DATE NOT NULL,
        sw_l1_code VARCHAR(16) NOT NULL,
        sw_l1_name VARCHAR(64) NOT NULL,
        industry_amount NUMERIC(30, 4),
        total_amount NUMERIC(30, 4),
        industry_amount_ratio NUMERIC(20, 10),
        stock_count INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, sw_l1_code)
    );
    """

    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))

def query_industry_turnover(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:

    sql = """
        WITH latest_industry AS (
        SELECT DISTINCT ON (code)
            code,
            sw_l1_code,
            sw_l1_name
        FROM stock_sw_industry_classification
        WHERE sw_l1_code IS NOT NULL
        AND sw_l1_name IS NOT NULL
        ORDER BY code, trade_date DESC
    ),

    base AS (
        SELECT
            v.trade_date,
            v.code,
            v.amount,
            i.sw_l1_code,
            i.sw_l1_name
        FROM stock_valuation_daily v
        JOIN latest_industry i
            ON v.code = i.code
        WHERE v.amount IS NOT NULL
        AND (:start_date IS NULL OR v.trade_date >= :start_date)
        AND (:end_date IS NULL OR v.trade_date <= :end_date)
    ),

    market_amount AS (
        SELECT
            trade_date,
            SUM(amount) AS total_amount
        FROM base
        GROUP BY trade_date
    ),

    industry_amount AS (
        SELECT
            trade_date,
            sw_l1_code,
            sw_l1_name,
            SUM(amount) AS industry_amount,
            COUNT(*) AS stock_count
        FROM base
        GROUP BY
            trade_date,
            sw_l1_code,
            sw_l1_name
    )

    SELECT
        ia.trade_date,
        ia.sw_l1_code,
        ia.sw_l1_name,
        ia.industry_amount,
        ma.total_amount,
        ia.industry_amount / NULLIF(ma.total_amount, 0) AS industry_amount_ratio,
        ia.stock_count
    FROM industry_amount ia
    JOIN market_amount ma
        ON ia.trade_date = ma.trade_date
    ORDER BY
        ia.trade_date DESC,
        industry_amount_ratio DESC;
    """

    with engine.connect() as conn:
        df = pd.read_sql(
            text(sql),
            conn,
            params={
                "start_date": start_date,
                "end_date": end_date,
            },
        )

    return df


def upsert(df: pd.DataFrame):
    if df.empty:
        print("无数据")
        return

    temp = "tmp_industry_turnover"

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp}"))

        conn.execute(text(f"""
            CREATE TEMP TABLE {temp} (
                trade_date DATE,
                sw_l1_code VARCHAR(16),
                sw_l1_name VARCHAR(64),
                industry_amount NUMERIC,
                total_amount NUMERIC,
                industry_amount_ratio NUMERIC,
                stock_count INTEGER
            ) ON COMMIT DROP
        """))

        df.to_sql(temp, conn, if_exists="append", index=False)

        conn.execute(text(f"""
            INSERT INTO industry_turnover_daily (
                trade_date,
                sw_l1_code,
                sw_l1_name,
                industry_amount,
                total_amount,
                industry_amount_ratio,
                stock_count,
                updated_at
            )
            SELECT
                trade_date,
                sw_l1_code,
                sw_l1_name,
                industry_amount,
                total_amount,
                industry_amount_ratio,
                stock_count,
                CURRENT_TIMESTAMP
            FROM {temp}
            ON CONFLICT (trade_date, sw_l1_code)
            DO UPDATE SET
                industry_amount = EXCLUDED.industry_amount,
                total_amount = EXCLUDED.total_amount,
                industry_amount_ratio = EXCLUDED.industry_amount_ratio,
                stock_count = EXCLUDED.stock_count,
                updated_at = CURRENT_TIMESTAMP
        """))


def update_industry_turnover(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    init_table()

    df = query_industry_turnover(start_date, end_date)

    upsert(df)

    print(f"完成，共 {len(df)} 行")


if __name__ == "__main__":
    update_industry_turnover()