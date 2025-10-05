#FIND TXHASH AND ID AND GET CONTRACT FIELD AND ADD TO THE Xslx

import polars as pl
import sys
import polars as pl
import os
import pandas as pd
import sys
from dotenv import load_dotenv


pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(20)

def get_dump_data(rollup):
    code_dir = os.path.realpath(os.path.join(os.getcwd(), "../.."))
    sys.path.append(code_dir)
    data_dir = os.path.abspath(os.path.join(code_dir, f"parquet_data_{rollup}"))
    path_data = dict()
    path_data['parquet_data'] = os.path.abspath(os.path.join(
            data_dir, "*.parquet"))
    return path_data


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

def extract_txhash_and_id(key):
    if isinstance(key, tuple) and len(key) >= 2:
        return str(key[0]).strip(), int(key[1])
    if isinstance(key, str):
        s = key.strip()
        if s.startswith("(") and "," in s:
            parts = s[1:-1].split(",", 1)
            txhash = parts[0].strip().strip("'").strip('"')
            id_val = parts[1].strip()
            try:
                id_val = int(id_val)
            except ValueError:
                pass
            return txhash, id_val
        return s, None
    return str(key), None

def process_csv_file(name="optimism"):
    csv_file_path = f"{name}_zero_results_filtered.xlsx"
    df = pd.read_excel(csv_file_path)
    print(f"Loaded {len(df)} rows from {csv_file_path}")
    path_data = get_dump_data(name)

    df[["transactionHash", "id"]] = df["key"].apply(
        lambda k: pd.Series(extract_txhash_and_id(k))
    )
    lookup_pl = pl.DataFrame(df[["id", "transactionHash"]])
    pre_scan_df = (
        pl.scan_parquet(path_data['parquet_data'])
        .join(lookup_pl.lazy(), on=["id", "transactionHash"], how="inner")
        .collect(engine="streaming")
    )
    pre_scan_pd = pre_scan_df.to_pandas()
    pre_scan_pd = pre_scan_pd.rename(columns={"contract": "contract_from_parquet"})
    df = df.merge(
        pre_scan_pd[["id", "transactionHash", "contract_from_parquet"]],
        on=["id", "transactionHash"],
        how="left"
    )

    df["contract_from_parquet"] = df["contract_from_parquet"].astype(str).str.lower()
    df.to_excel(csv_file_path, index=False)
    return csv_file_path

    """
    for row in df.itertuples(index=True):
        print(f"\nProcessing row {total_rows}")
        idx = row.Index
        key_str = row.key
        key,id=extract_txhash_and_id(key_str)
        interactions = pre_scan_df.filter(
            (pl.col("id")==id) & (pl.col("transactionHash")==key)
        )
        contract = interactions["contract"][0].lower()
        updates.append({
                "idx": idx,
                "contract": contract,
            })
        total_rows+=1
    updates_df = pd.DataFrame(updates).set_index("idx")
    df.update(updates_df)
    df.to_csv(csv_file_path, index=False)
    """
    


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
    path_data = get_dump_data(arg_name)
    out_path = process_csv_file(arg_name)
    print(f"\nSummary (written to {out_path}):")
        
