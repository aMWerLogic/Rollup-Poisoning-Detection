import polars as pl
from helpers import to_ethereum_address

#THIS FUNCTION IS DEDICATED FOR BATCHING (multiple blocks in dust_df)
def find_previous_transfers(dust_df, path_data,current_block_min,current_block_max,ERC20_decimals_map,dust_bool):
    logs_path = path_data['logs']
    if dust_bool == True:
        unique_victims = dust_df["receiver"].unique().to_list()
    else:
        unique_victims = dust_df["sender"].unique().to_list() #victim is a sender, attakcer is a receiver in fake or zero
    victim_set = [addr.lower() for addr in unique_victims]

    all_transfers = pl.scan_parquet(logs_path).filter(
        (pl.col("topics_0").str.to_lowercase() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") &
        (pl.col("blockNumber") < current_block_max) & 
        (pl.col("blockNumber") >= (current_block_min-500000)) #those two block filters are done to restrict the size of dataframe 
    ).with_columns([
        pl.col("topics_1").alias("sender"),
        pl.col("topics_2").alias("receiver")
    ]).select(
            pl.col("blockNumber"),
            pl.col("topics_0"),
            pl.col("sender"),
            pl.col("receiver"),
            pl.col("data")
                .str.strip_prefix("0x")
                .map_elements(lambda x: str(int(x, 16)) if x is not None and x.strip() != "" else None, return_dtype=pl.Utf8)
                .alias("data_decimal"),
            pl.col("address"),
            pl.col("transactionHash"),
            pl.col("transactionIndex"),
            pl.col("logIndex")
        ).filter(
        (pl.col("sender").is_in(victim_set)) &
        (pl.col("data_decimal").cast(float) > 0) &
        (pl.col("address").str.to_lowercase().is_in(ERC20_decimals_map))
    ).collect()


    dfs = dust_df.partition_by("blockNumber")
    result_df = pl.DataFrame()
    for dust in dfs: #filtering per block, it is fast because all_transfers df is already collected
        current = dust["blockNumber"][0]
        block_transfers = all_transfers.filter(
            (pl.col("blockNumber") < current) &
            (pl.col("blockNumber") >= (current-500000))
        )

        transfers_with_proximity = block_transfers.with_columns(
            (pl.lit(current_block_max) - pl.col("blockNumber")).alias("block_proximity")
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
