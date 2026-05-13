from fredapi import Fred

fred = Fred(api_key="5ed278101685f23afd6a3f4a03538039")

cpi = fred.get_series("CPIAUCSL")
print(cpi)