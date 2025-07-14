from nsepython import nse_quote
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Part 1 - nsepython usage
print("Fetching stock quote using nsepython...")
quote = nse_quote("RELIANCE")
print(quote["priceInfo"]["lastPrice"])

# Part 2 - Scrape holiday list
url = "https://www.nseindia.com/products-services/equity-market-holidays"
headers = {
    "User-Agent": "Mozilla/5.0"
}

# Get cookies first
s = requests.Session()
s.headers.update(headers)
s.get("https://www.nseindia.com")

res = s.get(url)
soup = BeautifulSoup(res.text, "html.parser")
tables = soup.find_all("table")

if tables:
    df = pd.read_html(str(tables[0]))[0]
    print(df)
else:
    print("No holiday table found.")
