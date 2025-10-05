from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import re

options = webdriver.ChromeOptions()
options.add_argument("--headless")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
driver.get("https://optimistic.etherscan.io/tokens?ps=100")
time.sleep(5)

soup = BeautifulSoup(driver.page_source, 'html.parser')

prices = []
for div in soup.find_all("div", {"class": "d-inline"}):
    price_str = div.get("data-bs-title")
    print(price_str)
    if price_str:
        m = re.search(r"([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)", price_str)
        if m:
            val = float(m.group(1).replace(",", ""))
        else:
            val = float("nan")
        prices.append(val)

symbols = []
for span in soup.find_all("span", {"class": "text-muted"}):
    sym = span.get_text(strip=True)
    print(sym)
    if sym.startswith("(") and sym.endswith(")"): 
        sym = sym[1:-1]
    symbols.append(sym)

driver.quit()

#Save Token Symbol + Price
with open("Optimism_token_symbols_prices.txt", "w", encoding="utf-8") as f:
    for symbol, price in zip(symbols, prices):
        f.write(f"{symbol},{price}\n")