import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import sys
from dotenv import load_dotenv

if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

if sys.argv[1]=="arbitrum":
    url = "https://3xpl.com/data/dumps/arbitrum-one-erc-20"
elif sys.argv[1]=="optimism":
    url = "https://3xpl.com/data/dumps/optimism-erc-20"
else:
    print("possible arguments: optimism or arbitrum")
    exit(1)

load_dotenv(dotenv_path="../.env")

TOKEN = os.getenv("xplToken")
headers = {"Authorization": f"Bearer {TOKEN}"}

start_date = datetime(2023, 1, 1)
end_date = datetime(2025, 1, 1)
response = requests.get(url, headers=headers)
response.raise_for_status()
soup = BeautifulSoup(response.content, "html.parser")

def extract_date_from_link(link):
    #Expects dates in format _YYYYMMDD before .tsv.zst
    import re
    match = re.search(r'_(\d{8})\.tsv\.zst$', link)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%d')
    return None

links = []
for a in soup.find_all("a", href=True):
    href = a['href']
    if href.endswith(".tsv.zst"):
        file_date = extract_date_from_link(href)
        if file_date and start_date <= file_date <= end_date:
            links.append(href)

folder_name = f"{sys.argv[1]}_erc20_dumps"
parent_path = os.path.join("..", folder_name)
os.makedirs(parent_path, exist_ok=True)

for link in links:
    if not link.startswith("http"):
        file_url = "https://3xpl.com" + link
    else:
        file_url = link

    filename = os.path.join(parent_path, os.path.basename(file_url))
    print(f"Downloading {file_url} -> {filename}")

    with requests.get(file_url, stream=True, headers=headers) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

