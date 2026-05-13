# from fredapi import Fred

# fred = Fred(api_key="5ed278101685f23afd6a3f4a03538039")

# cpi = fred.get_series("CPIAUCSL")
# print(cpi)

import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://www.forexfactory.com/calendar?week=this"

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(url, headers=headers)

soup = BeautifulSoup(response.text, "html.parser")

rows = soup.find_all("tr", class_="calendar__row")

events = []

for row in rows:
    try:
        currency = row.find("td", class_="calendar__currency").text.strip()
        event = row.find("td", class_="calendar__event").text.strip()
        actual = row.find("td", class_="calendar__actual").text.strip()
        forecast = row.find("td", class_="calendar__forecast").text.strip()
        previous = row.find("td", class_="calendar__previous").text.strip()

        events.append({
            "Currency": currency,
            "Event": event,
            "Actual": actual,
            "Forecast": forecast,
            "Previous": previous
        })

    except:
        pass

df = pd.DataFrame(events)
print(events)
print(df.head())