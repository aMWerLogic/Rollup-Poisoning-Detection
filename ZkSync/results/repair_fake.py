import polars as pl
import sys
pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_rows(20)
import polars as pl
import os
import pandas as pd

def extract_txhash_from_key(key):
    if isinstance(key, tuple) and len(key) > 0:
        tx = str(key[0]).strip()
        idx = 0
        if len(key) > 1:
            try:
                idx = int(str(key[1]).strip())
            except Exception:
                idx = 0
        return tx, idx
    if isinstance(key, str):
        s = key.strip()
        if s.startswith("(") and "," in s:
            inner = s[1:].rsplit(")", 1)[0]
            first, rest = inner.split(",", 1)
            tx = first.strip().strip("'").strip('"')
            try:
                idx = int(rest.strip().strip(")").strip())
            except Exception:
                idx = 0
            return tx, idx
        return s, 0
    return str(key), 0

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

if __name__ == "__main__":
    df = pd.read_csv("fake_results_filtered_analyzed_final.csv")
    path_data = get_zksync_data()
    if 'original_contract' not in df.columns:
        df['original_contract'] = None
    for idx, row in df.iterrows():
        print(idx)
        txhash,logIndex=extract_txhash_from_key(row["key"])
        txhash = str(txhash).lower()
        q = (
            pl.scan_parquet(path_data['logs'])
            .filter(
                 (pl.col("transactionHash").str.to_lowercase() == txhash)
                & (pl.col("logIndex").cast(pl.Utf8).str.to_lowercase() == str(logIndex))
            )
            .select(pl.col("address"))
            .limit(1)
        )
        result = q.collect()
        addr = result["address"][0].lower() if result.height > 0 else None
        df.at[idx, 'original_contract'] = addr
        if idx % 10 == 0:
            df.to_csv("fake_results_filtered_analyzed_final.csv", index=False)

    df.to_csv("fake_results_filtered_analyzed_final.csv", index=False)



