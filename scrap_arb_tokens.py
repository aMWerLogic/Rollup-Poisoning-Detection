import requests
from bs4 import BeautifulSoup
from get_tokens import GetTokens

def fetch_top_erc20_addresses(url, limit=70):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    addresses = []
    for a in soup.find_all("a", href=True):
        if a["href"].startswith("/token/") and len(a["href"]) > len("/token/40x"):
            addr = a["href"].split("/token/")[1]
            # Avoid duplicates
            if addr not in addresses:
                addresses.append(addr)
        if len(addresses) >= limit:
            break
    return addresses

def fetch_way_back(url, domain, limit=70):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    addresses = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if f"/{domain}/token/" in href:
            addr = href.split(f"/{domain}/token/")[1]
        elif href.startswith("/token/"):
            addr = href.split("/token/")[1]
        else:
            continue
        if addr not in addresses:
            addresses.append(addr)
        if len(addresses) >= limit:
            break
    return addresses

if __name__ == "__main__":
    #url = ["https://arbiscan.io/tokens?ps=100", "https://web.archive.org/web/20230211211722/https://arbiscan.io/tokens"]
    url = ["https://optimistic.etherscan.io/tokens?ps=100", "https://web.archive.org/web/20231127154637/https://optimistic.etherscan.io/tokens", "https://web.archive.org/web/20230218225145/https://optimistic.etherscan.io/tokens"]
    
    

    #filename = "top_arbitrum_erc20.txt"
    filename = "top_optimism_erc20.txt"


    #rpc = "https://arb1.arbitrum.io/rpc"
    rpc = "https://mainnet.optimism.io"


    #filename2 = "arbitrum_token_info.txt"
    filename2 = "optimism_token_info.txt"


    #domain = "arbiscan.io"
    domain = "optimistic.etherscan.io"
    

    ERC20_addresses = []
    for i in range(len(url)):
        print("processing",url[i])
        if "archive" in url[i]:
            top_addresses = fetch_way_back(url[i],domain,70)
        else:
            top_addresses = fetch_top_erc20_addresses(url[i],70)
        with open(filename, "a") as f:
            for addr in top_addresses:
                f.write(addr + "\n")
                ERC20_addresses.append(addr.lower())
        print(f"Saved {len(top_addresses)} token addresses to {filename}")

    tokenInfo = GetTokens(rpc)
    with open(filename2, "a", encoding="utf-8") as out_f:
        out_f.write("address,name,symbol,decimals\n")
        for addr in ERC20_addresses:
            #time.sleep(1)
            name, symbol, decimals = tokenInfo.safe_get_token_info(addr=addr, wait_time=3, info=1)
            out_f.write(f"{addr},{name},{symbol},{decimals}\n")
            print(f"Address: {addr}, Name: {name}, Symbol: {symbol}, Decimals: {decimals}")