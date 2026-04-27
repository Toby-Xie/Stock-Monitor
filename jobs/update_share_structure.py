import time
import pandas as pd
import akshare as ak
from sqlalchemy import text

from db import engine


def init_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS stock_share_structure (
        code VARCHAR(16) NOT NULL,
        change_date DATE NOT NULL,
        total_share NUMERIC(24, 4),
        circulating_share NUMERIC(24, 4),
        restricted_share NUMERIC(24, 4),
        source VARCHAR(32) DEFAULT 'akshare.stock_zh_a_gbjg_em',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (code, change_date)
    );

    CREATE INDEX IF NOT EXISTS idx_stock_share_structure_code_date
    ON stock_share_structure (code, change_date);
    """
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def baostock_code_to_ak_symbol(code: str) -> str:
    """
    sh.600000 -> 600000.SH
    sz.000001 -> 000001.SZ
    """
    if code.startswith("sh."):
        return code[3:] + ".SH"
    if code.startswith("sz."):
        return code[3:] + ".SZ"
    return code


def normalize_share_value(series: pd.Series, column_name: str) -> pd.Series:
    """
    把 AkShare 返回的股本字段统一转成“股”。

    如果字段名里有“万股”，则乘以 10000。
    如果已经是“股”，则保持原单位。
    """
    value = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("万股", "", regex=False)
        .str.replace("股", "", regex=False)
        .str.strip()
    )

    value = pd.to_numeric(value, errors="coerce")

    if "万股" in column_name:
        value = value * 10000

    return value


def pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    return next((c for c in candidates if c in df.columns), None)


def fetch_one_share_structure(code: str) -> pd.DataFrame:
    symbol = baostock_code_to_ak_symbol(code)

    try:
        raw = ak.stock_zh_a_gbjg_em(symbol=symbol)
    except Exception as e:
        print(f"{code} 股本结构获取失败: {e}")
        return pd.DataFrame()

    if raw.empty:
        return pd.DataFrame()

    date_col = pick_column(raw, ["变动日期", "公告日期", "日期"])

    total_col = pick_column(raw, ["总股本", "总股本(股)", "总股本股", "总股本(万股)"])
    circ_col = pick_column(raw, ["流通股本", "流通股本(股)", "流通股本股", "流通股本(万股)"])
    rest_col = pick_column(raw, ["限售股本", "限售股本(股)", "限售股本股", "限售股本(万股)"])

    if date_col is None or total_col is None:
        print(f"{code} 字段无法识别, columns={list(raw.columns)}")
        return pd.DataFrame()

    df = pd.DataFrame()
    df["code"] = code
    df["change_date"] = pd.to_datetime(raw[date_col], errors="coerce").dt.date
    df["total_share"] = normalize_share_value(raw[total_col], total_col)

    if circ_col:
        df["circulating_share"] = normalize_share_value(raw[circ_col], circ_col)
    else:
        df["circulating_share"] = pd.NA

    if rest_col:
        df["restricted_share"] = normalize_share_value(raw[rest_col], rest_col)
    else:
        df["restricted_share"] = pd.NA

    df = df.dropna(subset=["change_date", "total_share"])
    df = df.drop_duplicates(subset=["code", "change_date"], keep="last")
    df = df.sort_values(["code", "change_date"])

    return df[
        [
            "code",
            "change_date",
            "total_share",
            "circulating_share",
            "restricted_share",
        ]
    ]


def upsert_share_structure(df: pd.DataFrame):
    if df.empty:
        return

    temp_table = "tmp_stock_share_structure"

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        conn.execute(text(f"""
            CREATE TEMP TABLE {temp_table} (
                code VARCHAR(16),
                change_date DATE,
                total_share NUMERIC(24, 4),
                circulating_share NUMERIC(24, 4),
                restricted_share NUMERIC(24, 4)
            ) ON COMMIT DROP
        """))

        df.to_sql(temp_table, conn, if_exists="append", index=False)

        conn.execute(text(f"""
            INSERT INTO stock_share_structure (
                code,
                change_date,
                total_share,
                circulating_share,
                restricted_share,
                updated_at
            )
            SELECT
                code,
                change_date,
                total_share,
                circulating_share,
                restricted_share,
                CURRENT_TIMESTAMP
            FROM {temp_table}
            ON CONFLICT (code, change_date)
            DO UPDATE SET
                total_share = EXCLUDED.total_share,
                circulating_share = EXCLUDED.circulating_share,
                restricted_share = EXCLUDED.restricted_share,
                updated_at = CURRENT_TIMESTAMP
        """))


def get_stock_codes_from_valuation() -> list[str]:
    """
    直接从已有 valuation 表里取股票代码。
    这样不用再依赖 BaoStock 股票列表。
    """
    sql = """
    SELECT DISTINCT code
    FROM stock_valuation_daily
    WHERE code IS NOT NULL
    ORDER BY code
    """

    with engine.begin() as conn:
        rows = conn.execute(text(sql)).fetchall()

    return [r[0] for r in rows]


def update_share_structure(sleep_sec: float = 0.2, batch_size: int = 100):
    init_table()

    codes = get_stock_codes_from_valuation()
    total = len(codes)

    print(f"开始更新股本结构，共 {total} 只股票")

    buffer = []

    for i, code in enumerate(codes, start=1):
        print(f"[{i}/{total}] {code}")

        df = fetch_one_share_structure(code)

        if not df.empty:
            buffer.append(df)

        if len(buffer) >= batch_size:
            batch_df = pd.concat(buffer, ignore_index=True)
            upsert_share_structure(batch_df)
            print(f"已写入 {len(batch_df)} 条股本记录")
            buffer = []

        time.sleep(sleep_sec)

    if buffer:
        batch_df = pd.concat(buffer, ignore_index=True)
        upsert_share_structure(batch_df)
        print(f"已写入 {len(batch_df)} 条股本记录")

    print("股本结构更新完成")


if __name__ == "__main__":
    update_share_structure()