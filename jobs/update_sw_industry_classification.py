import time
from datetime import date

import pandas as pd
import requests
from sqlalchemy import create_engine, text
import akshare as ak
import os

PG_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:123456@db:5432/stockdb"
)

engine = create_engine(PG_URL, pool_pre_ping=True)

# 申万 2021 一级行业代码。AkShare 申万列表接口不稳定，所以这里固定一级行业列表，
# 再用 index_component_sw 拉每个行业的成分股。
SW_L1_INDUSTRIES = {
    "801010": "农林牧渔",
    "801030": "基础化工",
    "801040": "钢铁",
    "801050": "有色金属",
    "801080": "电子",
    "801110": "家用电器",
    "801120": "食品饮料",
    "801130": "纺织服饰",
    "801140": "轻工制造",
    "801150": "医药生物",
    "801160": "公用事业",
    "801170": "交通运输",
    "801180": "房地产",
    "801200": "商贸零售",
    "801210": "社会服务",
    "801230": "综合",
    "801710": "建筑材料",
    "801720": "建筑装饰",
    "801730": "电力设备",
    "801740": "国防军工",
    "801750": "计算机",
    "801760": "传媒",
    "801770": "通信",
    "801780": "银行",
    "801790": "非银金融",
    "801880": "汽车",
    "801890": "机械设备",
    "801950": "煤炭",
    "801960": "石油石化",
    "801970": "环保",
    "801980": "美容护理",
}


def init_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS stock_sw_industry_classification (
        code VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        sw_l1_code VARCHAR(16) NOT NULL,
        sw_l1_name VARCHAR(64) NOT NULL,
        source VARCHAR(64) DEFAULT 'akshare.index_component_sw',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (code, trade_date)
    );

    CREATE INDEX IF NOT EXISTS idx_stock_sw_industry_classification_code
    ON stock_sw_industry_classification (code);

    CREATE INDEX IF NOT EXISTS idx_stock_sw_industry_classification_industry_date
    ON stock_sw_industry_classification (sw_l1_code, trade_date);
    """
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def normalize_stock_code(code: str) -> str | None:
    """把 000001 / 000001.SZ / sh600000 等格式统一成 sh.600000 / sz.000001。"""
    if code is None:
        return None

    code = str(code).strip()
    if not code:
        return None

    lower = code.lower()
    if lower.startswith("sh.") or lower.startswith("sz."):
        return lower

    if lower.startswith("sh") and len(lower) >= 8:
        return "sh." + lower[-6:]
    if lower.startswith("sz") and len(lower) >= 8:
        return "sz." + lower[-6:]

    raw = code.split(".")[0].strip().zfill(6)

    if raw.startswith(("600", "601", "603", "605", "688", "689")):
        return "sh." + raw
    if raw.startswith(("000", "001", "002", "003", "300", "301")):
        return "sz." + raw

    # 只保存沪深 A 股；北交所等暂不进入本表。
    return None


def pick_stock_code_column(df: pd.DataFrame) -> str | None:
    candidates = [
        "成分券代码",
        "证券代码",
        "股票代码",
        "品种代码",
        "代码",
        "con_code",
    ]
    return next((c for c in candidates if c in df.columns), None)


def fetch_with_retry(index_code: str, max_retry: int = 5, sleep_sec: float = 2.0) -> pd.DataFrame:

    last_error = None
    for i in range(max_retry):
        try:
            df = ak.index_component_sw(symbol=index_code)
            if df is not None and not df.empty:
                return df
            last_error = RuntimeError(f"empty result for {index_code}")
        except (requests.exceptions.RequestException, ConnectionError, TimeoutError) as e:
            last_error = e
        except Exception as e:
            last_error = e

        wait = sleep_sec * (i + 1)
        print(f"{index_code} 获取失败，{wait:.1f}s 后重试: {last_error}")
        time.sleep(wait)

    raise RuntimeError(f"{index_code} 获取失败，已重试 {max_retry} 次") from last_error


def fetch_sw_l1_industry_map(as_of_date: date | None = None) -> pd.DataFrame:
    if as_of_date is None:
        as_of_date = date.today()

    rows = []

    for sw_l1_code, sw_l1_name in SW_L1_INDUSTRIES.items():
        print(f"拉取申万一级行业 {sw_l1_code} {sw_l1_name}")
        raw = fetch_with_retry(sw_l1_code)
        code_col = pick_stock_code_column(raw)

        if code_col is None:
            print(f"{sw_l1_code} 字段无法识别, columns={list(raw.columns)}")
            continue

        for code in raw[code_col].dropna().astype(str):
            normalized = normalize_stock_code(code)
            if normalized is None:
                continue
            rows.append({
                "code": normalized,
                "trade_date": as_of_date,
                "sw_l1_code": sw_l1_code,
                "sw_l1_name": sw_l1_name,
            })

        time.sleep(0.5)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["code", "trade_date"], keep="last")
    df = df.sort_values(["sw_l1_code", "code"])
    return df


def upsert_sw_industry_classification(df: pd.DataFrame):
    if df.empty:
        print("无申万行业分类数据可写入")
        return

    temp_table = "tmp_stock_sw_industry_classification"

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        conn.execute(text(f"""
            CREATE TEMP TABLE {temp_table} (
                code VARCHAR(16),
                trade_date DATE,
                sw_l1_code VARCHAR(16),
                sw_l1_name VARCHAR(64)
            ) ON COMMIT DROP
        """))

        df.to_sql(temp_table, conn, if_exists="append", index=False)

        conn.execute(text(f"""
            INSERT INTO stock_sw_industry_classification (
                code,
                trade_date,
                sw_l1_code,
                sw_l1_name,
                updated_at
            )
            SELECT
                code,
                trade_date,
                sw_l1_code,
                sw_l1_name,
                CURRENT_TIMESTAMP
            FROM {temp_table}
            ON CONFLICT (code, trade_date)
            DO UPDATE SET
                sw_l1_code = EXCLUDED.sw_l1_code,
                sw_l1_name = EXCLUDED.sw_l1_name,
                updated_at = CURRENT_TIMESTAMP
        """))


def update_sw_industry_classification():
    init_table()
    df = fetch_sw_l1_industry_map()
    upsert_sw_industry_classification(df)
    print(f"申万行业分类更新完成，共写入/更新 {len(df)} 条记录")


if __name__ == "__main__":
    update_sw_industry_classification()
