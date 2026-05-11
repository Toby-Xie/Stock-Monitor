from bigquantdai import dai

dai.login("RowmPfaAlCi9", "7Ql9Z6QIwA0JGMh42kwiduccT4KNbniYxr9lseB9m6h0d408VOIUcmxEJgldCVBI")

df = dai.query("""
SELECT *
FROM cn_stock_industry_component
WHERE industry='sw2021'
AND date='2026-04-24'
""").df()

print(df.head())