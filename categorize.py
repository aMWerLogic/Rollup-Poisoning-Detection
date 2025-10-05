import polars as pl
import os
import sys
from get_tokens import GetTokens
from web3 import Web3
import csv

pl.Config.set_tbl_cols(20)

def get_dump_data(rollup):
    code_dir = os.path.realpath(os.path.join(os.getcwd(), ".."))
    sys.path.append(code_dir)
    data_dir = os.path.abspath(os.path.join(os.getcwd(), f"parquet_data_{rollup}"))
    path_data = dict()
    path_data['parquet_data'] = os.path.abspath(os.path.join(
            data_dir, "*.parquet"))
    return path_data

#TODO: DECIMAL SENSITIVE
# if Decimals:
# <6,10) then <100
# <10-14) then <10000
# <14,rest) then <1000000
def dust_transfer(logs, ERC20_decimals_map):
    dust_logs = logs.filter(
    pl.col("contract").str.to_lowercase().is_in(ERC20_decimals_map)
        ).with_columns([
            pl.col("contract")
              .str.to_lowercase()
              .replace(ERC20_decimals_map, return_dtype=pl.Utf8)
              .alias("decimals")
        ])

    result = dust_logs.filter(
        pl.when(pl.col("decimals").str.len_chars() < 10)
        .then(pl.col("amount").cast(float) < (10**(pl.col("decimals").str.len_chars()-4)))
        .when(pl.col("decimals").str.len_chars() < 14)
        .then(pl.col("amount").cast(float) < (10**(pl.col("decimals").str.len_chars()-6)))
        .otherwise(pl.col("amount").cast(float) < (10**(pl.col("decimals").str.len_chars()-8)))
    ).drop("decimals")

    return result
    
def zero_transfer(logs, ERC20_decimals_map):
    logs = logs.filter(
    pl.col("contract").str.to_lowercase().is_in(ERC20_decimals_map)
        )
    return logs.filter(logs["amount"].map_elements(lambda x: int(x) == 0 if x is not None else False, return_dtype=pl.Boolean))


def fake_transfer(logs, ERC20_addresses_lower, ERC20_name, ERC20_symbol,fake_tokens,cached_tokens,API,rollup):
    contractInfoExtractor = GetTokens(API)
    logs = logs.filter(~pl.col("contract").str.to_lowercase().is_in(ERC20_addresses_lower))
    unique_addresses = logs["contract"].to_list()
    
    unique_addresses = [Web3.to_checksum_address(addr) for addr in unique_addresses]
    address_to_name = {}
    address_to_symbol = {}
    for addr in set(unique_addresses):
        if addr.lower() in cached_tokens:
            continue
        else:
            name, symbol, _ = contractInfoExtractor.safe_get_token_info(addr)
            if ( name in ERC20_name or symbol in ERC20_symbol or symbol in ERC20_name or name in ERC20_symbol ):
                address_to_name[addr.lower()] = name
                address_to_symbol[addr.lower()] = symbol
                fake_tokens.add(addr.lower())
                with open(f"{rollup}_fake_tokens.csv", "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([addr.lower()])
            with open(f"{rollup}_cached_tokens.csv", "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([addr.lower()])
            cached_tokens.add(addr.lower())
    print("fake_transfer gathered")
    return logs.filter(pl.col("contract").str.to_lowercase().is_in(list(fake_tokens)))