import polars as pl
import sys
pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_rows(20)
import csv
import polars as pl
from web3 import Web3
import os
import pandas as pd


pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(20)

def convert_to_fixed_size(address):
    address = address[2:]
    if len(address) != 40:
        raise ValueError("Address should be 40 characters long without the '0x' prefix.")
    fixed_size_address = '0x' + address.zfill(64)
    return fixed_size_address.lower()

def to_ethereum_address(hex_string):
    return ("0x" + hex_string[-40:]).lower()

def get_zksync_data():
    code_dir = os.path.realpath(os.path.join(os.getcwd(), ".."))
    sys.path.append(code_dir)
    
    data_dir = os.path.abspath(os.path.join(code_dir, "data"))
    path_data = dict()
    path_data['transactions'] = os.path.abspath(os.path.join(
            data_dir, "transactions", "transactions_*.parquet"))
    path_data['receipts'] = os.path.abspath(os.path.join(
            data_dir, "tx_receipts", "tx_receipts_*.parquet"))
    path_data['logs'] = os.path.abspath(os.path.join(
            data_dir, "logs", "logs_*.parquet"))
    return path_data

import time
THRESHOLD = 1

if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

TRANSFER_EVENT_SIG = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
def split_ranges(latest_block: int, parts: int):
    step = latest_block // parts
    ranges = []
    start = 0
    for i in range(parts):
        end = (i + 1) * step if i < parts - 1 else latest_block
        ranges.append((start, end))
        start = end + 1
    return ranges

def get_logs_with_narrowing(w3: Web3, base_filter_params: dict, start: int, end: int, min_span: int = 1000):
    collected = []
    stack = [(start, end)]
    while stack:
        s, e = stack.pop()
        params = dict(base_filter_params)
        params["fromBlock"] = hex(s)
        params["toBlock"] = hex(e)
        try:
            logs = w3.eth.get_logs(params)
            collected.extend(logs)
        except Exception as err:
            span = max(0, e - s)
            if span <= min_span:
                print(f"Error getting logs for range {s}-{e}: {err}")
                continue
            mid = (s + e) // 2
            if mid <= s:
                print(f"Error getting logs for range {s}-{e}: {err}")
                continue
            stack.append((mid + 1, e))
            stack.append((s, mid))
    return collected

def safe_hex_to_int(value) -> int:
    try:
        if value is None:
            return 0
        if hasattr(value, 'hex') and callable(value.hex):
            hex_str = value.hex()
        else:
            hex_str = str(value)
        hex_str = hex_str.strip()
        if hex_str.startswith('0x') or hex_str.startswith('0X'):
            hex_str = hex_str[2:]
        if hex_str == '' or hex_str == '0x':
            return 0
        return int(hex_str, 16)
    except Exception:
        return 0

def process_csv_file(csv_file_path,ERC20_addresses,ERC20_decimals_map,ERC20_symbol_map,ERC20_price_map,name="zero"):
    df = pd.read_csv(csv_file_path)
    print(f"Loaded {len(df)} rows from {csv_file_path}")
    ZKSYNC_RPC = "https://mainnet.era.zksync.io"
    w3 = Web3(Web3.HTTPProvider(ZKSYNC_RPC))
    latest_block = w3.eth.block_number
    parts = 10
    ranges = split_ranges(latest_block, parts)
    output_dir = os.path.dirname(os.path.abspath(csv_file_path))
    payouts_file = os.path.join(output_dir, f"{name}_payouts.csv")
    payouts_fields = ['amount','address','txhash','suspicious_txhash','cost']
    if not os.path.exists(payouts_file):
        with open(payouts_file, 'w', newline='', encoding='utf-8') as pf:
            writer = csv.DictWriter(pf, fieldnames=payouts_fields)
            writer.writeheader()
    for col, default in [
        ('should_drop', False),
        ('contract', None),
        ('amount', None),
        ('droping_txhash', None),
        ('payout', None),
    ]:
        if col not in df.columns:
            df[col] = default
    
    total_rows = 0
    for idx, row in df.iterrows():
        print(f"\nProcessing row {idx+1}")
        key_str = row['key']
        victim = row['victim']
        attacker = row['attacker']
        txhash = key_str.split("'")[1] if "'" in key_str else key_str.split('"')[1]
        print(f"Victim: {victim}, Attacker: {attacker}, TX: {txhash}")

        try:
            tx = w3.eth.get_transaction(txhash)
            suspicious_block = tx['blockNumber']
            tx_from = (tx.get('from') or tx.get('fromAddress') or '').lower()
            if tx_from == str(victim).lower():
                print("Sender equals victim; removing row from CSV and skipping.")
                df.drop(index=idx, inplace=True)
                df.to_csv(csv_file_path, index=False)
                continue
            print(f"Suspicious transaction block: {suspicious_block}")
        except Exception as e:
            print(f"Error getting transaction {txhash}: {e}")
            continue
        victim_fixed = convert_to_fixed_size(victim)
        attacker_fixed = convert_to_fixed_size(attacker)
        all_logs = []
        for i, (start, end) in enumerate(ranges, 1):
            base_filter_params = {
                "topics": [
                    TRANSFER_EVENT_SIG,
                    [victim_fixed, attacker_fixed],
                    [victim_fixed, attacker_fixed],
                ]
            }
            logs = get_logs_with_narrowing(w3, base_filter_params, start, end, min_span=1000)
            all_logs.extend(logs)
        drop = False
        sig_no0x = TRANSFER_EVENT_SIG[2:] 
        payout = 0
        for log in all_logs:
            data_int = safe_hex_to_int(log.get("data"))
            if (data_int!=0 and log["address"].lower() in ERC20_addresses):
                symbol = ERC20_symbol_map.get(log["address"].lower())
                try:
                    price = float(ERC20_price_map.get(symbol, 0)) if symbol else 0.00001
                except (TypeError, ValueError):
                    price = 0.00001
                decimals = int(ERC20_decimals_map.get(log["address"].lower(), 18))
                amount = float(data_int) / float(10 ** decimals)
            else:
                amount = 0.0
                price = 0.0
            if (log["topics"][0].hex() == sig_no0x and
                log["topics"][1].hex()[-40:].lower() == victim_fixed[-40:].lower() and
                log["topics"][2].hex()[-40:].lower() == attacker_fixed[-40:].lower() and
                log["blockNumber"] < suspicious_block and
                data_int>0 and
                log["address"].lower() in ERC20_addresses
                ):
                drop = True
                print("Victim sent some legit tokens - dropping")

            elif (log["topics"][0].hex() == sig_no0x and
                log["topics"][1].hex()[-40:].lower() == attacker_fixed[-40:].lower() and
                log["topics"][2].hex()[-40:].lower() == victim_fixed[-40:].lower() and
                log["blockNumber"] < suspicious_block and
                log["address"].lower() in ERC20_addresses and
                amount*float(price)>10
                ):
                drop = True
                print("address:",log["address"])
                print('amount:', data_int)
                print("txhash:",log["transactionHash"].hex())
                print("attacker sent high value legit tokens - dropping")

            elif (log["topics"][0].hex() == sig_no0x and
                log["topics"][1].hex()[-40:].lower() == victim_fixed[-40:].lower() and
                log["topics"][2].hex()[-40:].lower() == attacker_fixed[-40:].lower() and
                log["blockNumber"] > suspicious_block and
                data_int>0
                ):
                print("address:",log["address"])
                print('amount:', data_int)
                print("txhash:",log["transactionHash"].hex())
                print("PAYOUT???")
                payout+=1
                try:
                    with open(payouts_file, 'a', newline='', encoding='utf-8') as pf:
                        writer = csv.DictWriter(pf, fieldnames=payouts_fields)
                        writer.writerow({
                            'amount': float(data_int) / float(10 ** int(ERC20_decimals_map.get(log["address"].lower(), 18))),
                            'address': log["address"].lower(),
                            'txhash': log["transactionHash"].hex(),
                            'suspicious_txhash': txhash,
                            'cost': float(amount) * float(price)
                        })
                except Exception as _:
                    pass
                continue

            else:
                continue
            df.loc[idx, 'should_drop'] = drop
            df.loc[idx, 'contract'] = log["address"].lower()
            df.loc[idx, 'amount'] = int(log["data"].hex(),16)
            df.loc[idx, 'droping_txhash'] = log["transactionHash"].hex()
            break
        if not drop:
            df.loc[idx, 'should_drop'] = False
            df.loc[idx, 'contract'] = None
            df.loc[idx, 'amount'] = None
            df.loc[idx, 'droping_txhash'] = None
        if payout==0:
            print("no payout detected")
            df.loc[idx, 'payout'] = False
        else:
            df.loc[idx, 'payout'] = True
        total_rows += 1
        time.sleep(0.1)
        df.to_csv(csv_file_path, index=False)
    df.to_csv(csv_file_path, index=False)
    return total_rows, csv_file_path

if __name__ == "__main__":
    ERC20_addr = set()
    ERC20_symbol_map = {}
    ERC20_decimals_map = {}
    with open("zkSync_token_info.txt", "r", encoding="utf-8") as f:
        next(f)
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 4:
                continue
            addr, name, symbol, decimals = parts
            # store decimals as integer for numeric math
            decimals_int = int(decimals)
            if name and name != "None":
                ERC20_symbol_map[addr.lower()] = symbol
                ERC20_addr.add(addr.lower())
            if decimals and decimals != "None":
                ERC20_decimals_map[addr.lower()] = decimals_int

    ERC20_price_map = {}
    with open("zkSync_token_symbols_prices.txt", "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 2:
                continue
            symbol, price = parts
            try:
                ERC20_price_map[symbol] = float(price)
            except ValueError:
                ERC20_price_map[symbol] = 0.00001
    if len(sys.argv) >= 2 and sys.argv[1].endswith('.csv'):
        csv_file = sys.argv[1]
        total_rows, out_path = process_csv_file(csv_file,ERC20_addr,ERC20_decimals_map,ERC20_symbol_map,ERC20_price_map,"fake")
        print(f"\nSummary (written to {out_path}):")
        print(f"Total rows processed: {total_rows}")
        
