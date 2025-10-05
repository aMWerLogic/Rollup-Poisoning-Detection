import polars as pl
import os
import sys
from getSymbol import get_token_info
from web3 import Web3

pl.Config.set_tbl_cols(20)

def get_zksync_data():
    code_dir = os.path.realpath(os.path.join(os.getcwd(), ".."))
    sys.path.append(code_dir)
    data_dir = os.path.abspath(os.path.join(os.getcwd(), "data"))
    path_data = dict()
    path_data['transactions'] = os.path.abspath(os.path.join(
            data_dir, "transactions", "transactions_*.parquet"))
    path_data['receipts'] = os.path.abspath(os.path.join(
            data_dir, "tx_receipts", "tx_receipts_*.parquet"))
    path_data['logs'] = os.path.abspath(os.path.join(
            data_dir, "logs", "logs_*.parquet"))
    return path_data

# if Decimals:
# <6,10) then <100
# <10-14) then <10000
# <14,rest) then <1000000
def dust_transfer(logs, ERC20_decimals_map):
    dust_logs = logs.filter(
    pl.col("address").str.to_lowercase().is_in(ERC20_decimals_map)
        ).with_columns([
            pl.col("address")
              .str.to_lowercase()
              .replace(ERC20_decimals_map, return_dtype=pl.Utf8)
              .alias("decimals")
        ])
    
    result = dust_logs.filter(
        pl.when(pl.col("decimals").str.len_chars() < 10)
        .then(pl.col("data_decimal").cast(float) < (10**(pl.col("decimals").str.len_chars()-4)))
        .when(pl.col("decimals").str.len_chars() < 14)
        .then(pl.col("data_decimal").cast(float) < (10**(pl.col("decimals").str.len_chars()-6)))
        .otherwise(pl.col("data_decimal").cast(float) < (10**(pl.col("decimals").str.len_chars()-8)))
    ).drop("decimals")

    return result
    
def zero_transfer(logs):
    return logs.filter(logs["data_decimal"].map_elements(lambda x: int(x) == 0 if x is not None else False, return_dtype=pl.Boolean))

def fake_transfer(logs, ERC20_addresses, ERC20_name, ERC20_symbol,fake_tokens,cached_tokens):
    ERC20_addresses_lower = [addr.lower() for addr in ERC20_addresses]
    logs = logs.filter(~pl.col("address").str.to_lowercase().is_in(ERC20_addresses_lower))
    unique_addresses = logs["address"].to_list()
    
    unique_addresses = [Web3.to_checksum_address(addr) for addr in unique_addresses]
    address_to_name = {}
    address_to_symbol = {}
    for addr in set(unique_addresses):
        if addr.lower() in cached_tokens:
            continue
        else:
            name, symbol, _ = get_token_info(addr)
            if name is not None:
                name = name.lower()
            if symbol is not None:
                symbol = symbol.lower()
                if name in ERC20_name or symbol in ERC20_symbol or symbol in ERC20_name or name in ERC20_symbol:
                    address_to_name[addr.lower()] = name
                    address_to_symbol[addr.lower()] = symbol
                    fake_tokens.add(addr.lower())
            cached_tokens.add(addr.lower())
    return logs.filter(pl.col("address").str.to_lowercase().is_in(list(fake_tokens)))