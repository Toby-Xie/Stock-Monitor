import akshare as ak
import time
# stock_board_industry_hist_em_df = ak.stock_board_industry_hist_em(symbol="小金属", start_date="20211201", end_date="20240222", period="日k", adjust="")
# print(stock_board_industry_hist_em_df)
# time.sleep(1)  # 避免请求过快被封

# stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
# print(stock_zh_a_spot_em_df)

# stock_gdfx_top_10_em_df = ak.stock_gdfx_top_10_em(symbol="sh688686", date="20251231")
# print(stock_gdfx_top_10_em_df)

# stock_institute_hold_df = ak.stock_institute_hold(symbol="20241")
# print(stock_institute_hold_df)

# fund_portfolio_hold_em_df = ak.fund_portfolio_hold_em(symbol="000001", date="2025")
# print(fund_portfolio_hold_em_df)

# fund_open_fund_info_em_df = ak.fund_open_fund_info_em(symbol="000001", indicator="单位净值走势")
# print(fund_open_fund_info_em_df)

# futures_hold_pos_sina_df = ak.futures_hold_pos_sina(symbol="成交量", contract="OI2605", date="20260415")
# print(futures_hold_pos_sina_df)

#计算volume 成交量
# stock_zh_index_daily_df = ak.stock_zh_index_daily(symbol="sz399552")
# print(stock_zh_index_daily_df)

# time.sleep(1)
# stock_zh_a_hist_df = ak.stock_zh_a_hist(
#     symbol="399552",
#     period="daily",
#     start_date="20260401",
#     end_date="200260417",
#     adjust="hfq"
# )
# print(stock_zh_a_hist_df)

# this data is not published after 2024-08-17
# stock_em_hsgt_hold_stock_df = ak.stock_hsgt_hold_stock_em(market="北向", indicator="今日排行")
# print(stock_em_hsgt_hold_stock_df)


# stock_margin_detail_szse_df = ak.stock_margin_detail_szse(date="20260417")
# print(stock_margin_detail_szse_df)

# stock_hsgt_fund_flow_summary_em_df = ak.stock_hsgt_fund_flow_summary_em()
# print(stock_hsgt_fund_flow_summary_em_df)

# stock_hsgt_hist_em_df = ak.stock_hsgt_hist_em(symbol="北向资金") # choice =  {"北向资金", "沪股通", "深股通", "南向资金", "港股通沪", "港股通深"}
# print(stock_hsgt_hist_em_df.tail(100))


# df = ak.stock_zh_a_spot_em()
# print(df[['代码', '名称', '总市值', '流通市值']])

#获取总股本数
# stock_zh_a_gbjg_em_df = ak.stock_zh_a_gbjg_em(symbol="510650.SH")
# print(stock_zh_a_gbjg_em_df)

stock_board_industry_name_em_df = ak.stock_board_industry_name_em()
print(stock_board_industry_name_em_df)