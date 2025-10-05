import polars as pl
import sys
pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_rows(20)
import csv
import math
import polars as pl
from web3 import Web3
import os
import requests
import time
import sys
from dotenv import load_dotenv

#1. for step2 (address similarity) apply score = math.floor(math.log(x + c, b)) - 3, where x is the number of chars that are similar, b is the base of log, c is the offset to start with 1; 
# thanks to that address with 3 chars match in prefix will have a score of 1 in this step, also this step is very important as bigger similairity lowers the chance of bening "attacker" address
# so this is the only step that can have higher than 1 score, for most frequent in analyzed datasets x it gives log(x + c, b)) - 3
#2. for all cases calculate score based on steps (all steps counted as 1 excluding similarity with its own scoring that can be greater than >1)
#3. if score greater than threshold then add to further verification (we chose th>1)
#4. for those added for further verification (API maybe because not many cases):
#  4.0 IF SENDER OF FAKE OR ZERO TRANSFER IS NOT THE SENDER OF WHOLE TRANSACTION, otherwise WE CAN LABEL TRANSFER as LEGIT (CASUE VICTIM WOULD NEVER ATTACK HIMSELF WITH A POISONING TRANSFER)
#  4.1 if 4.0 is satisfied check if victim is not a system/utility/dex contract or most active accounts with tens of thousands of transfers (high activity accounts with possible birthday paradox)
#  4.2 if 4.1 is satisfied check if phishing address had sent or received a transaction prior to suspisious transfer; if not, check if it interacted only with the victim address prior to suspicious transfer; (this step is done because in our system we check only in a given period o time; here we check in all of history using API) (if it did interact only with it then +1)
#  4.3 if 4.2 is satisifed mark as Poisoning and check for payout transfers (if exists) and get the value in $USD at the time of assets
#  4.4 exclude typos cases (manually, in dust transfers)

#bonus for posioning transfers count unique fake token contracts; check transfer count and unique addresses that interacted with it (founders) and count how many receivers (lookalike)
#check for time distribution between legit transfer and posioning
#check what is most preferable type of attack
#check for address re-use on different chains (take phishing address and check behaviour on different rollups and ethereum)
#what type of victims are targetted (active ones or not)

def get_dump_data(rollup):
    code_dir = os.path.realpath(os.path.join(os.getcwd(), ".."))
    sys.path.append(code_dir)
    data_dir = os.path.abspath(os.path.join(code_dir, f"parquet_data_{rollup}"))
    path_data = dict()
    path_data['parquet_data'] = os.path.abspath(os.path.join(
            data_dir, "*.parquet"))
    return path_data

def filter_steps_by_step2_keys(type: str, name: str, steps: tuple[int, ...] = (1, 3, 4, 5)) -> None:
    step2_path = f"{name}_{type}_step2.csv"
    try:
        with open(step2_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            step2_keys = {row["transaction_key"].strip() for row in reader if row.get("transaction_key")}
    except FileNotFoundError:
        print(f"missing {step2_path}")
        return

    for step in steps:
        path = f"{name}_{type}_step{step}.csv"
        try:
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or []
                if step!=1:
                    rows = [row for row in reader if row.get("address") and row["address"].strip() in step2_keys]
                else:
                    rows = [row for row in reader if row.get("transaction_key") and row["transaction_key"].strip() in step2_keys]

            # Write back in place
            with open(path, "w", newline="", encoding="utf-8") as f_out:
                writer = csv.DictWriter(f_out, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        except FileNotFoundError:
            print(f"missing {path}")
            continue
    

def dedupe_csv(path_in: str, path_out: str | None = None):
    try:
        path_out = path_out or path_in
        df = pl.read_csv(path_in, schema_overrides={"score": pl.Float64})
        before = df.height
        df_unique = df.unique()
        after = df_unique.height
        deleted = before - after
        if deleted > 0:
            print(f"{path_in}: removed {deleted} duplicate rows (from {before} to {after})")
        df_unique.write_csv(path_out)
    except FileNotFoundError:
        print(f"missing {path_in}")

def compare_csv(csv1: str, csv2: str):
    df1 = pl.read_csv(csv1)
    df2 = pl.read_csv(csv2)
    print(df1.height)
    print(df2.height)
    # Rows in df1 but not in df2
    only_in_df1 = df1.join(df2, on=df1.columns, how="anti")
    # Rows in df2 but not in df1
    only_in_df2 = df2.join(df1, on=df1.columns, how="anti")
    return only_in_df1, only_in_df2


def get_contract_name(contract_address):
    name_abi = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    }
    ]
    contract = w3.eth.contract(address=contract_address, abi=name_abi)
    try:
        name = contract.functions.name().call()
        print("Contract name:", name)
    except Exception as e:
        print("No name() function found or call failed:", e)

def calc_score(x):
    if x == 3:
        return 1
    b = 1.5
    c = 2
    return math.floor(math.log(x + c, b)) - 3  #based on how hard it is to find longer similar addresses

def extract_txhash_from_key(key):
    if isinstance(key, tuple) and len(key) > 0:
        return str(key[0]).strip()
    if isinstance(key, str):
        s = key.strip()
        if s.startswith("(") and "," in s:
            first = s[1:].split(",", 1)[0]
            return first.strip().strip("'").strip('"')
        return s
    return str(key)

def get_sent_tx_count(address: str, block_tag: str = "latest") -> int:
    hex_count = w3.eth.get_transaction_count(address, block_identifier=block_tag)
    return hex_count

def get_interactions(path_data,block_number,victim,attacker):
    
    senders = pl.scan_parquet(path_data['parquet_data']).filter(
            (pl.col("blockNumber") >= 1) & (pl.col("blockNumber") < block_number) & 
            (pl.col("receiver").str.to_lowercase() == victim.lower())
        ).select(pl.col("sender")).collect(engine="streaming")
    receivers = pl.scan_parquet(path_data['parquet_data']).filter(
            (pl.col("blockNumber") >= 1) & (pl.col("blockNumber") < block_number) & 
            (pl.col("sender").str.to_lowercase() == victim.lower())
        ).select(pl.col("receiver")).collect(engine="streaming")
    
    senders = senders.rename({"sender": "address"})
    receivers = receivers.rename({"receiver": "address"})

    all_addresses = pl.concat([senders, receivers], how="vertical")
    unique_total = all_addresses.select(pl.col("address").n_unique()).item()

    if attacker in all_addresses:
        return unique_total-1
    else:
        return unique_total


def safe_get_transaction(w3, txhash, victim, attacker, retries=float("inf"), delay=30):
    attempt = 0
    while attempt < retries:
        if attempt%20==0:
            delay+=10
            w3 = Web3(Web3.HTTPProvider(rpc_url2))
        try:
            tx = w3.eth.get_transaction(txhash)
            block_number = tx.blockNumber
            code = w3.eth.get_code(Web3.to_checksum_address(victim))
            attacker_code = w3.eth.get_code(Web3.to_checksum_address(attacker))
            return tx, block_number, code, attacker_code
        except (requests.exceptions.ConnectionError, ConnectionResetError) as e:
            attempt += 1
            print(f"[{attempt}] Connection error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            #Catch anything unexpected
            attempt += 1
            print(f"[{attempt}] Unexpected error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Max retries reached, still failing.")

THRESHOLD = 1

if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

load_dotenv(dotenv_path="../.env")
alchemyToken=os.getenv("alchemyToken")

if sys.argv[1]=="arbitrum":
    rpc_url="https://arb1.arbitrum.io/rpc"
    rpc_url2="https://arb1.arbitrum.io/rpc"
    name = "arbitrum"
if sys.argv[1]=="optimism":
    rpc_url="https://mainnet.optimism.io"
    rpc_url2=f"https://opt-mainnet.g.alchemy.com/v2/{alchemyToken}"
    name = "optimism"


w3 = Web3(Web3.HTTPProvider(rpc_url))
attack_types = ["dust","zero", "fake"]

pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(20)
if __name__ == "__main__":

    for type in attack_types:
        filter_steps_by_step2_keys(type, name)
    
    ###clears result datasets in case of some error
    for i in range(1,6):
        dedupe_csv(f"{name}_zero_step{i}.csv",f"{name}_zero_step{i}.csv")
        dedupe_csv(f"{name}_dust_step{i}.csv",f"{name}_dust_step{i}.csv")
        dedupe_csv(f"{name}_fake_step{i}.csv",f"{name}_fake_step{i}.csv")

    cached_attackers = set()
    cached_victims = set()
    for type in attack_types:
        result = {}

        with open(f"{name}_{type}_step1.csv", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row["transaction_key"].strip()
                attacker = row["attacker"].strip()
                victim = row["victim"].strip()
                result[key] = {
                    "score": 1,
                    "attacker": attacker,
                    "victim": victim
                }

        with open(f"{name}_{type}_step2.csv", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row["transaction_key"].strip()
                score = calc_score(int(float(row["score"])))
                #score = math.floor(math.log2(int(row["score"])))  
                if key in result:
                    result[key]["score"] += score
                else:
                    attacker = row["attacker"].strip()
                    victim = row["victim"].strip()
                    result[key] = {
                    "score": score,
                    "attacker": attacker,
                    "victim": victim
                    }

        with open(f"{name}_{type}_step3.csv", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row["address"].strip() 
                score_step = float(row["score"].strip())
                if key in result:
                    result[key]["score"] += score_step

        with open(f"{name}_{type}_step5.csv", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                txaddr = row["address"].strip() 
                for key in result: 
                    if txaddr in key:
                        result[key]["score"] += 1

        step4_set = set()
        with open(f"{name}_{type}_step4.csv", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row["address"].strip()
                step4_set.add(key)

        for key in list(result.keys()): #4.0 if not in step4 then legit
            if key not in step4_set:
                result.pop(key,None)

        with open(f"{name}_{type}_results.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["key", "score", "victim", "attacker"])
            for key in result:
                if result[key]["score"]>THRESHOLD:
                    writer.writerow([key, result[key]["score"], result[key]["victim"], result[key]["attacker"]])

        ###################
        #ADVANCED FILTERING
        path_data = get_dump_data(name)
        results_filtered_path = f"{name}_{type}_results_filtered.csv"
        if not os.path.exists(results_filtered_path):
            with open(results_filtered_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["key", "score", "victim", "attacker", "poor_activity_attacker", "poor_activity_victim", "possible_utility_victim"])
        i=0
        with open(f"{name}_{type}_results.csv", newline="") as f: #if sender or receiver is a contract, get its name and add given row for furthere verification
            reader = csv.DictReader(f)
            for row in reader:
                i+=1
                if i%50==0:
                    print("name:",name,"type:", type," i= ", i)
                key = row["key"].strip()
                score = row["score"].strip()
                victim = row["victim"].strip()
                attacker = row["attacker"].strip()
                txhash = extract_txhash_from_key(key)
                tx, block_number, code, attacker_code = safe_get_transaction(w3, txhash, victim, attacker)
                if attacker in cached_attackers:
                    with open(f"{name}_{type}_removedAttackers.csv", "a", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow([key,attacker])
                    continue
                if victim in cached_victims:
                    continue
                unique_accounts=get_interactions(path_data,block_number,attacker,victim)
                poor_activity = False
                if unique_accounts<1:
                    poor_activity = True
                if attacker_code != b'' and unique_accounts > 1000: #then most likely it is a utility contract
                    with open(f"{name}_{type}_removedAttackers.csv", "a", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow([key,attacker])
                        cached_attackers.add(attacker)
                    continue
                unique_accounts=get_interactions(path_data,block_number,victim,attacker)
                poor_activity_victim = False
                if unique_accounts < 1:
                    poor_activity_victim = True
                if unique_accounts > 60000:
                    print("block_number",block_number)
                    print("interacted account number",unique_accounts)
                    print("victim",victim)
                    cached_victims.add(victim)
                    continue
                possible_utility = False
                if code != b'' and unique_accounts>1000:
                    print("POSSIBLE UTILITY CONTRACT VICTIM:", victim)
                    possible_utility = True
                with open(results_filtered_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([key, score, victim, attacker, poor_activity,poor_activity_victim, possible_utility])
