import polars as pl
import sys
pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_rows(20)
import csv
import math
import polars as pl
from web3 import Web3
import os

#1. for step2 (address similarity) apply score = math.floor(math.log(x + c, b)) - 3, where x is the number of chars that are similar, b is the base of log, c is the offset to start with 1; 
# thanks to that address with 3 chars match in prefix will have a score of 1 in this step, also this step is very important as bigger similairity lowers the chance of bening "attacker" address
# so this is the only step that can have higher than 1 score, for most frequent in analyzed datasets x it gives log2(x)
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

def filter_steps_by_step2_keys(name: str, steps: tuple[int, ...] = (1, 3, 4, 5)) -> None:
    step2_path = f"{name}_step2.csv"
    try:
        with open(step2_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            step2_keys = {row["transaction_key"].strip() for row in reader if row.get("transaction_key")}
    except FileNotFoundError:
        print(f"missing {step2_path}")
        return

    for step in steps:
        path = f"{name}_step{step}.csv"
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
    path_out = path_out or path_in
    df = pl.read_csv(path_in)
    before = df.height
    df_unique = df.unique()
    after = df_unique.height
    deleted = before - after
    if deleted > 0:
        print(f"{path_in}: removed {deleted} duplicate rows (from {before} to {after})")
    df_unique.write_csv(path_out)

def compare_csv(csv1: str, csv2: str):
    df1 = pl.read_csv(csv1)
    df2 = pl.read_csv(csv2)
    print(df1.height)
    print(df2.height)
    exit(0)
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

def get_interactions(block_number,victim,attacker):
    path_data = get_zksync_data()
    victim_fixed = convert_to_fixed_size(victim)
    ether=convert_to_fixed_size("0x0000000000000000000000000000000000008001")
    senders = pl.scan_parquet(path_data['logs']).filter(
            (pl.col("blockNumber") >= 1) & (pl.col("blockNumber") < block_number) & 
            (pl.col("topics_0").str.to_lowercase() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") &
            (pl.col("topics_2").str.to_lowercase() == victim_fixed.lower()) &
            (pl.col("topics_1").str.to_lowercase() != ether)
        ).select(pl.col("topics_1")).collect(engine="streaming")
    receivers = pl.scan_parquet(path_data['logs']).filter(
            (pl.col("blockNumber") >= 1) & (pl.col("blockNumber") < block_number) & 
            (pl.col("topics_0").str.to_lowercase() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") &
            (pl.col("topics_1").str.to_lowercase() == victim_fixed.lower()) &
            (pl.col("topics_2").str.to_lowercase() != ether)
        ).select(pl.col("topics_2")).collect(engine="streaming")
    
    senders = senders.rename({"topics_1": "address"})
    receivers = receivers.rename({"topics_2": "address"})

    all_addresses = pl.concat([senders, receivers], how="vertical")
    unique_total = all_addresses.select(pl.col("address").n_unique()).item()

    if convert_to_fixed_size(attacker).lower() in all_addresses:
        return unique_total-1
    else:
        return unique_total
    
THRESHOLD = 1

if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

if sys.argv[1]=="dust":
    name = "dust"
if sys.argv[1]=="zero":
    name = "zero"
if sys.argv[1]=="fake":
    name = "fake"


if __name__ == "__main__":
    
    filter_steps_by_step2_keys(name)
    
    ###clears result datasets in case of some error
    for i in range(1,6):
        dedupe_csv(f"zero_step{i}.csv",f"zero_step{i}.csv")
        dedupe_csv(f"dust_step{i}.csv",f"dust_step{i}.csv")
        dedupe_csv(f"fake_step{i}.csv",f"fake_step{i}.csv")
    
    
    result = {}
    with open(f"{name}_step1.csv", newline="") as f:
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

    with open(f"{name}_step2.csv", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["transaction_key"].strip()
            score = calc_score(int(row["score"]))
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

    with open(f"{name}_step3.csv", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["address"].strip() 
            score_step = float(row["score"].strip())
            if key in result:
                result[key]["score"] += score_step
    
    with open(f"{name}_step5.csv", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            txaddr = row["address"].strip() 
            for key in result: 
                if txaddr in key:
                    result[key]["score"] += 1


    if name!="dust":
        step4_set = set()
        with open(f"{name}_step4.csv", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row["address"].strip()
                step4_set.add(key)
                if key in result:
                    result[key]["score"] += 1
        
        for key in list(result.keys()):
            if key not in step4_set:
                result.pop(key,None)

    with open(f"{name}_results.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["key", "score", "victim", "attacker"])
        for key in result:
            if result[key]["score"]>THRESHOLD:
                writer.writerow([key, result[key]["score"], result[key]["victim"], result[key]["attacker"]])

    
    
    
    ZKSYNC_RPC = "https://mainnet.era.zksync.io"
    w3 = Web3(Web3.HTTPProvider(ZKSYNC_RPC))

    with open(f"{name}_results_filtered.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["key", "score", "victim", "attacker", "poor_activity"])
    results_set = set()
    i=0

    cached_attackers = set()
    cached_victims = set()

    with open(f"{name}_results.csv", newline="") as f: #if sender or receiver is a contract, get its name and add given row for furthere verification
        reader = csv.DictReader(f)
        for _ in range(200):
            next(reader, None)
        for row in reader:
            i+=1
            if i%50==0:
                print(i)
            key = row["key"].strip()
            score = row["score"].strip()
            results_set.add(key)
            victim = row["victim"].strip()
            attacker = row["attacker"].strip()
            txhash = extract_txhash_from_key(key)
            tx = w3.eth.get_transaction(txhash)
            block_number = tx.blockNumber
            code = w3.eth.get_code(Web3.to_checksum_address(victim))
            attacker_code = w3.eth.get_code(Web3.to_checksum_address(attacker))
            if attacker in cached_attackers:
                results_set.remove(key)
                with open(f"{name}_removedAttackers.csv", "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([key,attacker])
                continue
            unique_accounts=get_interactions(block_number,attacker,victim)
            poor_activity = False
            if unique_accounts<1:
                poor_activity = True
            if attacker_code != b'' and unique_accounts > 1000: #then most likely it is a utility contract
                results_set.remove(key)
                with open(f"{name}_removedAttackers.csv", "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([key,attacker])
                    cached_attackers.add(attacker)
                continue
            if victim in cached_victims:
                results_set.remove(key)
                continue
            unique_accounts=get_interactions(block_number,victim,attacker)
            if unique_accounts > 60000:
                results_set.remove(key)
                print("block_number",block_number)
                print("interacted account number",unique_accounts)
                print("victim",victim)
                cached_victims.add(victim)
                continue
            if code != b'' and unique_accounts>1000:
                print("POSSIBLE UTILITY CONTRACT VICTIM:", victim)
                with open(f"{name}_possibleUtility.csv", "a", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow([key, victim])
            with open(f"{name}_results_filtered.csv", "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([key, score, victim, attacker, poor_activity])
    
    print(len(results_set))