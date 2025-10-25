import polars as pl
import sys
import csv
import polars as pl
from web3 import Web3
import os
import pandas as pd
import requests
import time
from dotenv import load_dotenv

load_dotenv(dotenv_path="../.env")
alchemy_token = os.getenv("alchemyToken")

pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(20)

def get_dump_data(rollup):
    code_dir = os.path.realpath(os.path.join(os.getcwd(), ".."))
    sys.path.append(code_dir)
    data_dir = os.path.abspath(os.path.join(code_dir, f"parquet_data_{rollup}"))
    path_data = dict()
    path_data['parquet_data'] = os.path.abspath(os.path.join(
            data_dir, "*.parquet"))
    return path_data

def safe_get_transaction(w3, txhash, retries=float("inf"), delay=30):
    attempt = 0
    while attempt < retries:
        if attempt%20==0:
            delay+=10
            w3 = Web3(Web3.HTTPProvider(rpc_url2))
        try:
            tx = w3.eth.get_transaction(txhash)
            return tx
        except (requests.exceptions.ConnectionError, ConnectionResetError) as e:
            attempt += 1
            print(f"[{attempt}] Connection error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            attempt += 1
            print(f"[{attempt}] Unexpected error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Max retries reached, still failing.")


def process_csv_file(attack_type,ERC20_addresses,ERC20_decimals_map,ERC20_price_map,name="optimism"):
    csv_file_path = f"{name}_{attack_type}_results_filtered.csv"
    df = pd.read_csv(csv_file_path)
    print(f"Loaded {len(df)} rows from {csv_file_path}")
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    path_data = get_dump_data(name)

    # prepare payouts csv in same directory as input
    output_dir = os.path.dirname(os.path.abspath(csv_file_path))
    payouts_file = os.path.join(output_dir, f"{name}_{attack_type}_payouts.csv")
    payouts_fields = ['amount','cost','contract_address','txhash','time','suspicious_txhash','victim','attacker']
    if not os.path.exists(payouts_file):
        with open(payouts_file, 'w', newline='', encoding='utf-8') as pf:
            writer = csv.DictWriter(pf, fieldnames=payouts_fields)
            writer.writeheader()
    # Ensure new columns exist with default values
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

    victims = set(df["victim"].str.lower())
    attackers = set(df["attacker"].str.lower())
    addresses = victims | attackers

    pre_scan_df = pl.scan_parquet(path_data['parquet_data']).filter(
            (pl.col("receiver").str.to_lowercase().is_in(addresses)) &
            (pl.col("sender").str.to_lowercase().is_in(addresses))
        ).collect(engine="streaming")

    drop_indices = []
    payouts_buffer = []
    updates = []
    for row in df.itertuples(index=True):
        print(f"\nProcessing row {total_rows}, type {attack_type}")
        total_rows += 1
        idx = row.Index
        victim = row.victim.lower()
        key_str = row.key
        attacker = row.attacker.lower()
        txhash = key_str.split("'")[1] if "'" in key_str else key_str.split('"')[1]
        print(f"Victim: {victim}, Attacker: {attacker}, TX: {txhash}")
        try:
            tx = safe_get_transaction(w3, txhash)
            suspicious_block = tx['blockNumber']
            tx_from = (tx.get('from') or tx.get('fromAddress') or '').lower()
            if tx_from == str(victim).lower():
                print("Sender equals victim; removing row from CSV and skipping.")
                drop_indices.append(idx)
                continue
            print(f"Suspicious transaction block: {suspicious_block}")
        except Exception as e:
            print(f"Error getting transaction {txhash}: {e}")
            continue
        drop = False
        payout = 0
        interactions = pre_scan_df.filter(
            (pl.col("receiver").str.to_lowercase().is_in([victim,attacker])) &
            (pl.col("sender").str.to_lowercase().is_in([victim,attacker]))
        )
        payout_detected=False
        payout_buffer_temp = []
        for interaction in interactions.iter_rows(named=True):
            if (int(interaction['amount'])!=0 and interaction["contract"].lower() in ERC20_addresses):
                try:
                    price = float(ERC20_price_map.get(interaction["contract"].lower(), 0.00001)) if symbol else 0.00001
                except (TypeError, ValueError):
                    price = 0.00001
                decimals = int(ERC20_decimals_map.get(interaction["contract"].lower(), 18))
                amount = float(interaction['amount']) / float(10 ** decimals)
            else:
                amount = float(interaction['amount']) / float(10 ** 18)
                price = 0.00001
            
            if (interaction['sender'].lower() == victim and 
                interaction['receiver'].lower() == attacker and
                interaction['blockNumber'] < suspicious_block and
                amount>0 and
                interaction["contract"].lower() in ERC20_addresses
                ):
                drop = True
                print("Victim sent some legit tokens - dropping")

            elif (interaction['sender'].lower() == attacker and 
               interaction['receiver'].lower() == victim and
               interaction['blockNumber'] < suspicious_block and
               amount*float(price)>10 and
               interaction["contract"].lower() in ERC20_addresses
               ):
               drop = True    
               print("address:",interaction["contract"])
               print('amount:', amount)
               print("txhash:",interaction["transactionHash"])
               print("attacker sent high value legit tokens - dropping")

            elif (interaction['sender'].lower() == victim and 
                interaction['receiver'].lower() == attacker and
                interaction['blockNumber'] > suspicious_block and
                amount>0 and
                interaction["contract"].lower() in ERC20_addresses
                ):
                print("address:",interaction["contract"])
                print('amount:', amount)
                print("txhash:",interaction["transactionHash"])
                print("PAYOUT???")
                payout+=1  
                payout_detected=True
                payout_buffer_temp.append({
                        'amount': amount,
                        'cost': float(amount) * float(price),
                        'contract_address': interaction["contract"],
                        'txhash': interaction["transactionHash"],
                        'time': interaction["time"],
                        'suspicious_txhash': txhash,
                        'victim': victim,
                        'attacker': attacker
                    })
                continue
            else:
                continue
            break
        # If we didn't mark for drop within the logs loop, ensure None values are written
        if not drop:
            updates.append({
                "idx": idx,
                "should_drop": False,
                "contract": None,
                "amount": None,
                "droping_txhash": None,
                "payout": payout_detected
            })
            payouts_buffer.extend(payout_buffer_temp)
        else:
            updates.append({
                "idx": idx,
                "should_drop": drop,
                "contract": interaction["contract"],
                "amount": amount,
                "droping_txhash": interaction["transactionHash"],
                "payout": False
                })
            payout_buffer_temp = []
    df = df.drop(index=drop_indices)
    updates_df = pd.DataFrame(updates).set_index("idx")
    df.update(updates_df)
    df.to_csv(csv_file_path, index=False)
    try:  
        with open(payouts_file, 'a', newline='', encoding='utf-8') as pf:
            writer = csv.DictWriter(pf, fieldnames=payouts_fields)
            writer.writerows(payouts_buffer)
    except Exception as e:
        print(f"EXCEPTION WHILE WRITING TO PAYOUT FILE: {e}")
        pass
    return total_rows, csv_file_path


if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

if sys.argv[1]=="arbitrum":
    rpc_url="https://arb1.arbitrum.io/rpc"
    rpc_url2="https://arb1.arbitrum.io/rpc"
    arg_name = "arbitrum"
if sys.argv[1]=="optimism":
    rpc_url="https://mainnet.optimism.io"
    rpc_url2=f"https://opt-mainnet.g.alchemy.com/v2/{alchemy_token}"
    arg_name = "optimism"

if __name__ == "__main__":
    #jeżeli victim wysłał 1 transfer do atakującego przed suspicious z legit contractu o amount >0, to drop
    #jeżeli atakujący wysłał do victima przed suspicious legit amount > 25$, to drop
    #jeżeli powyższe niespełnione, i po suspicious, victim wysłał do atakującego amount > 0, to zapisz jako payout wszystkie takie kolejne transfery
    #jeżeli victim nie wysłal transferu po suspicious to zapisz jako atak bez payoutu
    path_data = get_dump_data(arg_name)
    #senders = pl.scan_parquet(path_data['parquet_data']).head(1).collect(engine="streaming")
    #print(senders)
    ERC20_addr = set()
    ERC20_decimals_map = {}
    with open(f"{arg_name}_token_info.txt", "r", encoding="utf-8") as f:
        next(f)
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 4:
                continue
            addr, name, symbol, decimals = parts
            # store decimals as integer for numeric math
            decimals_int = int(decimals)
            if name and name != "None":
                ERC20_addr.add(addr.lower())
            if decimals and decimals != "None":
                ERC20_decimals_map[addr.lower()] = decimals_int

    ERC20_price_map = {}
    with open(f"{arg_name}_token_symbols_prices.txt", "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 3:
                continue
            address, symbol, price = parts
            try:
                ERC20_price_map[address] = float(price)
            except ValueError:
                ERC20_price_map[address] = 0.00001

    #attack_types = ["dust","fake","zero"]
    attack_types = ["zero"]

    for type in attack_types:
        print(f"processing {type}")
        total_rows, out_path = process_csv_file(type,ERC20_addr,ERC20_decimals_map,ERC20_price_map,arg_name)
        # Print summary (file has been written incrementally)
        print(f"\nSummary (written to {out_path}):")
        print(f"Total rows processed: {total_rows}")
        
