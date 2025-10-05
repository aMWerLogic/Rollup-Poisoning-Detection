from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import re

options = webdriver.ChromeOptions()
options.add_argument("--headless")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
driver.get("https://explorer.zksync.io/tokens")
time.sleep(5)

soup = BeautifulSoup(driver.page_source, 'html.parser')

prices = []
for td in soup.find_all("td", {"data-heading": "Price"}):
    price_div = td.find("div", {"class": "token-price"})
    if price_div:
        txt = price_div.get_text(strip=True)
        m = re.search(r"([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)", txt)
        if m:
            val = float(m.group(1).replace(",", ""))
        else:
            val = float("nan")
        prices.append(val)

symbols = []
for td in soup.find_all("td", {"data-heading": "Token Name"}):
    symbol_div = td.find("div", {"class": "token-symbol"})
    if symbol_div:
        symbols.append(symbol_div.get_text(strip=True))

driver.quit()

with open("zkSync_token_symbols_prices.txt", "w", encoding="utf-8") as f:
    for symbol, price in zip(symbols, prices):
        f.write(f"{symbol},{price}\n")