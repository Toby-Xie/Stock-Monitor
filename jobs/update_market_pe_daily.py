import os
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text


PG_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:123456@db:5432/stockdb",
)

engine = create_engine(PG_URL, pool_pre_ping=True)


def init_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS market_pe_daily (
        trade_date DATE NOT NULL,
        market_pe NUMERIC(20, 6),
        matched_stock_count INTEGER,
        total_market_value NUMERIC(30, 4),
        total_implied_profit NUMERIC(30, 4),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date)
    );

    CREATE INDEX IF NOT EXISTS idx_market_pe_daily_trade_date
    ON market_pe_daily (trade_date);
    """

    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def calc_market_pe_daily(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    从 stock_valuation_daily 和 stock_share_structure 计算每日市场 PE。

    计算逻辑：
    market_pe = sum(close * total_share) / sum(close * total_share / pe_ttm)

    注意：
    - 以 stock_valuation_daily 为主表
    - 只匹配 stock_share_structure 中同 code 且 change_date <= trade_date 的最近股本
    - 过滤 pe_ttm <= 0、close <= 0、total_share <= 0
    """

    sql = """
    WITH matched AS (
        SELECT
            v.trade_date,
            v.code,
            v.close,
            v.pe_ttm,
            s.total_share,
            v.close * s.total_share AS total_market_value,
            (v.close * s.total_share) / v.pe_ttm AS implied_profit
        FROM stock_valuation_daily v
        JOIN LATERAL (
            SELECT ss.total_share
            FROM stock_share_structure ss
            WHERE ss.code = v.code
              AND ss.change_date <= v.trade_date
            ORDER BY ss.change_date DESC
            LIMIT 1
        ) s ON true
        WHERE v.close IS NOT NULL
          AND v.close > 0
          AND v.pe_ttm IS NOT NULL
          AND s.total_share IS NOT NULL
          AND s.total_share > 0
          AND (:start_date IS NULL OR v.trade_date >= CAST(:start_date AS DATE))
          AND (:end_date IS NULL OR v.trade_date <= CAST(:end_date AS DATE))
    )
    SELECT
        trade_date,
        SUM(total_market_value) / NULLIF(SUM(implied_profit), 0) AS market_pe,
        COUNT(*) AS matched_stock_count,
        SUM(total_market_value) AS total_market_value,
        SUM(implied_profit) AS total_implied_profit
    FROM matched
    GROUP BY trade_date
    ORDER BY trade_date;
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


def upsert_dataframe(df: pd.DataFrame):
    if df.empty:
        return

    temp_table = "tmp_market_pe_daily"

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

        conn.execute(text(f"""
            CREATE TEMP TABLE {temp_table} (
                trade_date DATE,
                market_pe NUMERIC(20, 6),
                matched_stock_count INTEGER,
                total_market_value NUMERIC(30, 4),
                total_implied_profit NUMERIC(30, 4)
            ) ON COMMIT DROP
        """))

        df.to_sql(temp_table, conn, if_exists="append", index=False)

        conn.execute(text(f"""
            INSERT INTO market_pe_daily (
                trade_date,
                market_pe,
                matched_stock_count,
                total_market_value,
                total_implied_profit,
                updated_at
            )
            SELECT
                trade_date,
                market_pe,
                matched_stock_count,
                total_market_value,
                total_implied_profit,
                CURRENT_TIMESTAMP
            FROM {temp_table}
            ON CONFLICT (trade_date)
            DO UPDATE SET
                market_pe = EXCLUDED.market_pe,
                matched_stock_count = EXCLUDED.matched_stock_count,
                total_market_value = EXCLUDED.total_market_value,
                total_implied_profit = EXCLUDED.total_implied_profit,
                updated_at = CURRENT_TIMESTAMP
        """))


def update_market_pe_daily(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """
    market PE 更新任务：
    1. 创建 market_pe_daily 表
    2. 从 stock_valuation_daily + stock_share_structure 计算每日 market PE
    3. upsert 到 market_pe_daily
    """

    init_table()

    df = calc_market_pe_daily(start_date=start_date, end_date=end_date)

    if df.empty:
        return {
            "status": "skipped",
            "reason": "empty_result",
            "start_date": start_date,
            "end_date": end_date,
            "message": "没有可计算的 market PE 数据",
        }

    upsert_dataframe(df)

    return {
        "status": "success",
        "rows": len(df),
        "start_date": str(df["trade_date"].min()),
        "end_date": str(df["trade_date"].max()),
        "message": f"market PE 更新完成，共 {len(df)} 天",
    }


if __name__ == "__main__":
    result = update_market_pe_daily()
    print(result)