import time
import baostock as bs
import pandas as pd
from sqlalchemy import create_engine, text

# =========================
# PostgreSQL 连接配置
# =========================
PG_URL = "postgresql+psycopg2://postgres:123456@localhost:5432/stockdb"

engine = create_engine(PG_URL, pool_pre_ping=True)


def init_table():
    """
    创建原始估值表
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS stock_valuation_daily (
        trade_date DATE NOT NULL,
        code VARCHAR(16) NOT NULL,
        close NUMERIC(20, 6),
        tradestatus SMALLINT,
        is_st SMALLINT,
        pe_ttm NUMERIC(20, 6),
        pb_mrq NUMERIC(20, 6),
        turn NUMERIC(10, 4),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, code)
    );
    CREATE INDEX IF NOT EXISTS idx_stock_valuation_daily_code_date
    ON stock_valuation_daily (code, trade_date);
    """
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def get_all_stocks(trade_date: str) -> list[str]:
    """
    获取指定交易日的全市场股票列表
    """
    rs = bs.query_all_stock(trade_date)
    data = []
    while rs.error_code == "0" and rs.next():
        data.append(rs.get_row_data())

    df = pd.DataFrame(data, columns=rs.fields)
    if df.empty:
        return []

    codes = df["code"].dropna().tolist()
    codes = [c for c in codes if c.startswith(("sh.", "sz."))]
    # codes = codes[4600:]
    return codes


def fetch_one_stock_history(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    拉取单只股票历史估值原始数据
    """
    rs = bs.query_history_k_data_plus(
        code,
        "date,code,close,tradestatus,isST,peTTM,pbMRQ,turn",
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

    # 字段标准化
    df.rename(
        columns={
            "date": "trade_date",
            "isST": "is_st",
            "peTTM": "pe_ttm",
            "pbMRQ": "pb_mrq",
        },
        inplace=True,
    )

    # 类型转换
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    for col in ["close", "pe_ttm", "pb_mrq", "turn"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["tradestatus", "is_st"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # 去掉空日期
    df = df[df["trade_date"].notna()].copy()

    return df[
        ["trade_date", "code", "close", "tradestatus", "is_st", "pe_ttm", "pb_mrq", "turn",]
    ]


def upsert_dataframe(df: pd.DataFrame):
    """
    批量 UPSERT 到 PostgreSQL
    """
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
                trade_date, code, close, tradestatus, is_st, pe_ttm, pb_mrq, turn
            )
            SELECT
                trade_date, code, close, tradestatus, is_st, pe_ttm, pb_mrq, turn
            FROM {temp_table}
            ON CONFLICT (trade_date, code)
            DO UPDATE SET
                close = EXCLUDED.close,
                tradestatus = EXCLUDED.tradestatus,
                is_st = EXCLUDED.is_st,
                pe_ttm = EXCLUDED.pe_ttm,
                pb_mrq = EXCLUDED.pb_mrq,
                turn = EXCLUDED.turn
        """))


def fetch_and_save_market(
    trade_date: str,
    start_date: str,
    end_date: str,
    sleep_sec: float = 0.02,
):
    """
    全市场拉取并落库
    """
    init_table()

    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")

    try:
        codes = get_all_stocks(trade_date)
        total = len(codes)

        for idx, code in enumerate(codes, 1):
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


if __name__ == "__main__":
    fetch_and_save_market(
        trade_date="2026-04-21",
        start_date="2026-04-21",
        end_date="2026-04-21",
        sleep_sec=0.02,
    )