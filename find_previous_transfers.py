import polars as pl
from datetime import datetime, timedelta

def find_previous_transfers(dust_df, path_data,current_time_min,current_time_max,ERC20_decimals_map,dust_bool,time_multiplier):

    logs_path = path_data['parquet_data']
    seven_days = timedelta(days=7)
    if dust_bool == True:
        unique_victims = dust_df["receiver"].unique().to_list()
    else:
        unique_victims = dust_df["sender"].unique().to_list() #victim is a sender, attakcer is a receiver in fake or zero
    victim_set = [addr.lower() for addr in unique_victims]

    all_transfers = pl.scan_parquet(logs_path).filter(
        (pl.col("time") >= pl.lit(current_time_min-seven_days))
        & (pl.col("time") <= pl.lit(current_time_max))  #those two filters are done to restrict the size of dataframe 
        & (pl.col("sender").is_in(victim_set))
        & (pl.col("amount").cast(float) > 0)
        & (pl.col("contract").str.to_lowercase().is_in(ERC20_decimals_map))
    ).collect()

    dfs = dust_df.partition_by("blockNumber")
    result_df = pl.DataFrame()
    for dust in dfs: #filtering per block, it is fast because all_transfers df is already collected
        current = dust["blockNumber"][0]
        block_transfers = all_transfers.filter(
            (pl.col("blockNumber") < current) &
            (pl.col("blockNumber") >= (current-(500000*time_multiplier)))
        )

        transfers_with_proximity = block_transfers.with_columns(
            (pl.lit(current) - pl.col("blockNumber")).alias("block_proximity")
        )

        sorted_transfers = transfers_with_proximity.sort(
            by=["sender", "block_proximity"]
        ).unique(subset=["sender", "receiver"]).group_by("sender").head(10)

        result = sorted_transfers.select([
            "sender",
            "receiver",
            "blockNumber"
        ]) 

        result = result.with_columns([pl.lit(current).alias("ID")]) #add column iter to distinguish cases where two same addresses are victims within a batch

        result_df = pl.concat([result_df, result], how="vertical")
    return result_df
