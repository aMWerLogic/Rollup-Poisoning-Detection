import sys
from get_tokens import GetTokens 
from dotenv import load_dotenv
import os

if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

load_dotenv()
alchemy_token = os.getenv("alchemyToken")

if sys.argv[1]=="arbitrum":
    rpc_url="https://arb1.arbitrum.io/rpc"
    rpc_url2="https://arb1.arbitrum.io/rpc"
    name = "arbitrum"
if sys.argv[1]=="optimism":
    rpc_url="https://mainnet.optimism.io"
    rpc_url2=f"https://opt-mainnet.g.alchemy.com/v2/{alchemy_token}"
    name = "optimism"



if __name__ == "__main__":
    input_file = f"{sys.argv[1]}_fake.txt"
    output_file = f"symbols_{sys.argv[1]}_fake.txt"

    with open(input_file, "r") as f:
        ERC20_addresses = {line.strip().lower() for line in f if line.strip()}

    getTokens = GetTokens(rpc_url)
    with open(output_file, "w", encoding="utf-8") as out:
        for address in ERC20_addresses:
            name, symbol, decimals = getTokens.get_token_info(address)
            out.write(f"{address},{name},{symbol}\n")