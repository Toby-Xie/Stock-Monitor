"""
A股常用数据接口示例
依赖:
    pip install tushare akshare pandas efinance

说明:
1) 你需要把 TUSHARE_TOKEN 换成自己的 token
2) AKShare/efinance 多数接口不需要 token，但部分网页源偶尔会限流或变动
3) “估值分位”“成交额占比”“基金仓位估算”通常不是现成字段，需要你自己基于原始数据计算
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd
import tushare as ts
import akshare as ak
import efinance as ef


# =========================
# 基础配置
# =========================
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "9ed745783d6f2ef10942aa35692f2eabc182d3978481b8d7cf3006ef")
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()


# =========================
# 1) 估值原始数据: PE / PB
#    用于后续自己计算“估值分位”
# =========================
def get_daily_valuation(
    trade_date: Optional[str] = None,
    ts_code: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    fields: str = "ts_code,trade_date,close,pe,pb,pe_ttm,total_mv,circ_mv",
) -> pd.DataFrame:
    """
    获取个股或全市场某日估值数据
    日期格式: YYYYMMDD
    例子:
        get_daily_valuation(trade_date="20260415")
        get_daily_valuation(ts_code="600519.SH", start_date="20240101", end_date="20260415")
    """
    df = pro.daily_basic(
        trade_date=trade_date,
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
    )
    return df


def calc_percentile_rank(series: pd.Series, latest_value: float) -> float:
    """
    计算某个值在历史序列中的分位百分比
    返回 0~100
    """
    s = series.dropna().sort_values()
    if s.empty:
        raise ValueError("历史序列为空，无法计算分位")
    rank = (s <= latest_value).mean() * 100
    return round(float(rank), 2)


def get_stock_valuation_percentile(
    ts_code: str,
    start_date: str,
    end_date: str,
    field: str = "pe_ttm",
) -> dict:
    """
    计算单只股票某估值字段的历史分位
    """
    df = get_daily_valuation_ak(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df.empty or field not in df.columns:
        raise ValueError(f"没有拿到 {ts_code} 的 {field} 数据")

    df = df.sort_values("trade_date")
    latest_value = df[field].dropna().iloc[-1]
    pct = calc_percentile_rank(df[field], latest_value)

    return {
        "ts_code": ts_code,
        "field": field,
        "latest_trade_date": df["trade_date"].iloc[-1],
        "latest_value": latest_value,
        "percentile": pct,
    }

def get_daily_valuation_ak(
    ts_code: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    使用 AKShare 获取个股估值数据（替代 tushare daily_basic）

    ts_code: "600519.SH" 或 "600519"
    日期格式: YYYYMMDD
    """

    if ts_code is None:
        raise ValueError("AKShare版本只支持单只股票，不支持全市场")

    # 👉 处理代码格式
    symbol = ts_code.split(".")[0]

    # =========================
    # 1️⃣ K线数据（含收盘价）
    # =========================
    k_df = ak.t(
        symbol=symbol,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust=""
    )

    if k_df.empty:
        return k_df

    k_df = k_df.rename(columns={
        "日期": "trade_date",
        "收盘": "close",
    })

    k_df["trade_date"] = pd.to_datetime(k_df["trade_date"]).dt.strftime("%Y%m%d")

    val_df = ak.stock_a_indicator_lg(symbol=symbol)

    if val_df.empty:
        return k_df

    val_df = val_df.rename(columns={
        "trade_date": "trade_date",
        "pe": "pe",
        "pb": "pb",
        "pe_ttm": "pe_ttm",
        "total_mv": "total_mv",
    })

    df = pd.merge(
        k_df,
        val_df,
        on="trade_date",
        how="left"
    )

    # 添加 ts_code
    df["ts_code"] = ts_code

    # 排序
    df = df.sort_values("trade_date")

    return df[[
        "ts_code",
        "trade_date",
        "close",
        "pe",
        "pb",
        "pe_ttm",
        "total_mv"
    ]]
    
# =========================
# 2) 北向资金
#    Tushare 日频更稳
# =========================
def get_northbound_flow_tushare(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    trade_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    沪深港通/北向南向资金流向
    """
    df = pro.moneyflow_hsgt(
        start_date=start_date,
        end_date=end_date,
        trade_date=trade_date,
    )
    if not df.empty and "trade_date" in df.columns:
        df = df.sort_values("trade_date")
    return df


def get_northbound_flow_akshare() -> pd.DataFrame:
    """
    AKShare 北向资金接口
    注意: AKShare 网页源接口变动相对更频繁，建议当作补充源
    """
    # 常见接口之一:
    # stock_hsgt_fund_flow_summary_em
    try:
        df = ak.stock_hsgt_fund_flow_summary_em()
        return df
    except Exception as e:
        raise RuntimeError(f"AKShare 北向资金接口调用失败: {e}") from e


# =========================
# 3) 融资融券
# =========================
def get_margin_tushare(
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    exchange_id: Optional[str] = None,
) -> pd.DataFrame:
    """
    两融汇总数据（沪深市场）
    备注:
      Tushare 两融相关接口较多，不同粒度有不同表。
      这里优先尝试 margin；如果你需要个股级别，可再接 detail 接口。
    """
    try:
        df = pro.margin(
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            exchange_id=exchange_id,
        )
        if not df.empty and "trade_date" in df.columns:
            df = df.sort_values("trade_date")
        return df
    except Exception as e:
        raise RuntimeError(f"Tushare 两融接口调用失败: {e}") from e


# =========================
# 4) 行业 / 指数 / ETF 行情
# =========================
def get_index_spot_akshare(symbol: str = "沪深重要指数") -> pd.DataFrame:
    """
    A股指数实时行情
    symbol 可选示例:
      - 沪深重要指数
      - 上证系列指数
      - 深证系列指数
      - 指数成份
      - 中证系列指数
    """
    return ak.stock_zh_index_spot_em(symbol=symbol)


def get_etf_spot_akshare() -> pd.DataFrame:
    """
    ETF 实时行情
    """
    return ak.fund_etf_spot_em()


def get_industry_board_spot_akshare() -> pd.DataFrame:
    """
    东方财富行业板块实时行情
    """
    return ak.stock_board_industry_name_em()


# =========================
# 5) 成交额占比
#    例子：ETF成交额 / 全市场ETF成交额
# =========================
def calc_turnover_share(
    df: pd.DataFrame,
    value_col: str,
    group_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    计算成交额占比
    - 如果不传 group_col: 直接算全表占比
    - 如果传 group_col: 按组计算占比
    """
    out = df.copy()
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce")

    if group_col is None:
        total = out[value_col].sum(skipna=True)
        out["turnover_share"] = out[value_col] / total if total else pd.NA
    else:
        total = out.groupby(group_col)[value_col].transform("sum")
        out["turnover_share"] = out[value_col] / total

    return out


# =========================
# 6) 社保重仓股（可选，偏补充源）
#    这类接口稳定性比日行情差，通常需要按当期公开披露口径抓
# =========================
def get_shareholder_top10_example(symbol: str = "600519") -> pd.DataFrame:
    """
    示例：AKShare 取个股股东相关公开信息
    注意:
      社保重仓股并非总有统一稳定的免费标准接口；
      更常见做法是先拿股东/基金披露，再自己筛“全国社保基金”。
    """
    try:
        df = ak.stock_gdfx_top_10_em(symbol=symbol)
        return df
    except Exception as e:
        raise RuntimeError(f"股东数据接口调用失败: {e}") from e


# =========================
# 7) efinance 作为补充行情源
# =========================
def get_realtime_quote_efinance(codes: list[str]) -> pd.DataFrame:
    """
    codes 例子: ["600519", "159915", "000300"]
    """
    return ef.stock.get_realtime_quotes(codes)


# =========================
# 示例 main
# =========================
def main() -> None:
    # print("=== 1. 单只股票估值分位示例 ===")
    # val_result = get_stock_valuation_percentile(
    #     ts_code="600519.SH",
    #     start_date="20250101",
    #     end_date="20260415",
    #     field="pe_ttm",
    # )
    # print(val_result)

    # print("\n=== 2. 北向资金（日频） ===")
    # north_df = get_northbound_flow_akshare()
    # print(north_df.tail())

    # print("\n=== 3. 融资融券 ===")
    # margin_df = get_margin_tushare(start_date="20260401", end_date="20260415")
    # print(margin_df.tail())

    # print("\n=== 4. 指数实时行情 ===")
    # index_df = get_index_spot_akshare(symbol="沪深重要指数")
    # print(index_df.tail())

    # print("\n=== 5. ETF 实时行情 + 成交额占比 ===")
    # etf_df = get_etf_spot_akshare()
    # # 常见列名一般是“成交额”，但不同接口偶尔会变；这里做兼容
    # turnover_col = "成交额" if "成交额" in etf_df.columns else None
    # if turnover_col:
    #     etf_df2 = calc_turnover_share(etf_df, value_col=turnover_col)
    #     print(etf_df2[[c for c in ["代码", "名称", turnover_col, "turnover_share"] if c in etf_df2.columns]].tail())
    # else:
    #     print("ETF 数据中未找到 '成交额' 列，请 print(etf_df.columns) 检查列名。")

    print("\n=== 6. 行业板块实时行情 ===")
    industry_df = get_industry_board_spot_akshare()
    print(industry_df.tail())

    # print("\n=== 7. efinance 补充行情 ===")
    # quote_df = get_realtime_quote_efinance(["600519", "159915", "000300"])
    # print(quote_df.head())


if __name__ == "__main__":
    main()