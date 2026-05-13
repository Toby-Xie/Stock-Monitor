# from fredapi import Fred

# fred = Fred(api_key="5ed278101685f23afd6a3f4a03538039")

# cpi = fred.get_series("CPIAUCSL")
# print(cpi)

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from datetime import datetime
import time

def parse_ff_date(day_str, year=2026):
    # 去掉星期，只保留 "May 4"
    parts = day_str.split(" ", 1)[1]  # "May 4"
    
    dt = datetime.strptime(f"{parts} {year}", "%b %d %Y")
    
    return dt.strftime("%Y%m%d")  # yyyymmdd

months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
years = [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023,2024,2025,2026]
urls = [
    f"https://www.forexfactory.com/calendar?month={month}.{year}"
    for month in months
    for year in years
]


headers = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

def parse_forex_factory_calendar(url, year):
    response = requests.get(url, headers=headers, timeout=20)
    soup = BeautifulSoup(response.text, "html.parser")

    rows = soup.find_all("tr", class_="calendar__row")

    events = []
    current_day = None
    current_time = None

    for row in rows:
        if "calendar__row--day-breaker" in row.get("class", []):
            current_day = row.get_text(" ", strip=True)
            continue

        date_cell = row.find("td", class_="calendar__date")
        if date_cell:
            date_text = date_cell.get_text(" ", strip=True)
            if date_text:
                current_day = parse_ff_date(date_text, year)

        try:
            time_cell = row.find("td", class_="calendar__time")
            currency_cell = row.find("td", class_="calendar__currency")
            event_cell = row.find("td", class_="calendar__event")
            actual_cell = row.find("td", class_="calendar__actual")
            forecast_cell = row.find("td", class_="calendar__forecast")
            previous_cell = row.find("td", class_="calendar__previous")
            impact_cell = row.find("td", class_="calendar__impact")

            if not currency_cell or not event_cell:
                continue

            time_text = time_cell.get_text(" ", strip=True) if time_cell else ""

            if time_text:
                current_time = time_text
            else:
                time_text = current_time

            currency = currency_cell.get_text(" ", strip=True)
            event = event_cell.get_text(" ", strip=True)
            actual = actual_cell.get_text(" ", strip=True) if actual_cell else ""
            forecast = forecast_cell.get_text(" ", strip=True) if forecast_cell else ""
            previous = previous_cell.get_text(" ", strip=True) if previous_cell else ""

            impact = ""
            if impact_cell:
                impact_span = impact_cell.find("span")
                impact_class = impact_span.get("class", []) if impact_span else []

                if "icon--ff-impact-yel" in impact_class:
                    impact = "1"
                elif "icon--ff-impact-org" in impact_class:
                    impact = "2"
                elif "icon--ff-impact-red" in impact_class:
                    impact = "3"
                else:
                    impact = "0"

            if impact == "0":
                continue

            events.append({
                "Day": current_day,
                "Time": time_text,
                "Currency": currency,
                "Impact": impact,
                "Event": event,
                "Actual": actual,
                "Forecast": forecast,
                "Previous": previous,
                "SourceURL": url,
            })

        except Exception as e:
            print("Skipping row:", repr(e))
            continue

    return pd.DataFrame(events)


all_dfs = []

for year in years:
    for month in months:
        if (month == "jan" or month == "feb" or month == "mar") and year == 2007:
            continue
        if (month == "jun" or month == "jul" or month == "aug" or month == "sep" or month == "oct" or month == "nov" or month == "dec") and year == 2026:
            continue
        url = f"https://www.forexfactory.com/calendar?month={month}.{year}"
        print(f"Fetching {month}.{year}: {url}")
        df_month = parse_forex_factory_calendar(url, year)
        all_dfs.append(df_month)

        # 避免请求太快
        time.sleep(1)

df_all = pd.concat(all_dfs, ignore_index=True)

print(df_all.head(20))
print(f"Total rows: {len(df_all)}")

df_all.to_csv(
    "./forex_factory_calendar_history.csv",
    index=False,
    encoding="utf-8-sig"
)