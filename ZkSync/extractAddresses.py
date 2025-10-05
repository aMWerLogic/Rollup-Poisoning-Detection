#IT IS USED TO SCRAP ADDRESSES FROM ZKSYNC TOKEN LIST

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time

options = webdriver.ChromeOptions()
options.add_argument("--headless")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
driver.get("https://explorer.zksync.io/tokens")
time.sleep(5)

soup = BeautifulSoup(driver.page_source, 'html.parser')

addresses = []
for td in soup.find_all("td", {"data-heading": "L2 Token address"}):
    a_tag = td.find("a", href=True)
    if a_tag and a_tag["href"].startswith("/token/"):
        addr = a_tag["href"].split("/token/")[-1]
        addresses.append(addr)

driver.quit()

with open("zkSync_token_addresses.txt", "w") as f:
    for addr in addresses:
        f.write(addr + "\n")