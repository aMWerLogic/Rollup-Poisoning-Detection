import sys
import pandas as pd
from dotenv import load_dotenv
import os

if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

load_dotenv(dotenv_path="../.env")
alchemyToken=os.getenv("alchemyToken")

if sys.argv[1]=="arbitrum":
    rpc_url="https://arb1.arbitrum.io/rpc"
    rpc_url2="https://arb1.arbitrum.io/rpc"
    arg_name = "arbitrum"
if sys.argv[1]=="optimism":
    rpc_url="https://mainnet.optimism.io"
    rpc_url2=f"https://opt-mainnet.g.alchemy.com/v2/{alchemyToken}"
    arg_name = "optimism"

if __name__ == "__main__":
    ERC20_addr = set()
    with open(f"{arg_name}_token_info.txt", "r", encoding="utf-8") as f:
        next(f)
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 4:
                continue
            addr, name, symbol, decimals = parts
            decimals_int = int(decimals)
            if name and name != "None":
                ERC20_addr.add(addr.lower())

    csv_file_path = f"{arg_name}_zero_payouts_deduped.csv"
    df = pd.read_csv(csv_file_path)
    print(f"Loaded {len(df)} rows from {csv_file_path}")

    df = df[df['contract_address'].isin(ERC20_addr)]
    df.to_csv(csv_file_path, index=False)


    