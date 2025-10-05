import polars as pl
from categorize import dust_transfer, zero_transfer, fake_transfer, get_zksync_data
from helpers import convert_to_fixed_size
import time
from steps_runner import StepsRunner
import sys

pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_rows(20)

# System contracts to exclude from analysis (like free collectors etc.) and also known DEX addresses
SYSTEM_CONTRACTS = {
    "0x0000000000000000000000000000000000000000",
    "0x000000000000000000000000000000000000800a",
    "0x000000000000000000000000000000000000800b",
    "0x0000000000000000000000000000000000008006",
    "0x0000000000000000000000000000000000008007",
    "0x0000000000000000000000000000000000008009",
    "0x000000000000000000000000000000000000800c",
    "0x0000000000000000000000000000000000008001",
    "0x0000000000000000000000000000000000008002",
    "0x0000000000000000000000000000000000002935",
    "0x000000000000000000000000000000000000ac02",
    "0x0000000000000000000000000000000000008001",
    "0x000000000000000000000000000000000000dead",
    "0x621425a1ef6abe91058e9712575dcc4258f8d091", #SyncSwapVault 
    "0x000000000000000000000000000000000000800b",
    "0x0000000000000000000000000000000000008002",
    "0x0000000000000000000000000000000000008006",
    "0x0000000000000000000000000000000000008003",
    "0x0000000000000000000000000000000000008004",
    "0x0000000000000000000000000000000000008005",
    "0x0000000000000000000000000000000000008008",
    "0x000000000000000000000000000000000000800a",
    "0x0000000000000000000000000000000000008009",
    "0x0000000000000000000000000000000000008010",
    "0x000000000000000000000000000000000000800c",
    "0x000000000000000000000000000000000000800d",
    "0x000000000000000000000000000000000000800e",
    "0x0000000000000000000000000000000000008001",
    "0x0000000000000000000000000000000000000001",
    "0x0000000000000000000000000000000000000002",
    "0x0000000000000000000000000000000000000000",
    "0x32400084c286cf3e17e7b677ea9583e60a000324",
    "0x303a465b659cbb0ab36ee643ea362c509eeb5213",
    "0xd7f9f54194c633f36ccd5f3da84ad4a1c38cb2cb",
    "0xd059478a564df1353a54ac0d0e7fc55a90b92246",
    "0xf3acf6a03ea4a914b78ec788624b25cec37c14a4",
    "0x63b5ec36b09384ffa7106a80ec7cfdcca521fd08",
    "0xb465882f67d236dcc0d090f78ebb0d838e9719d8",
    "0xa8cb082a5a689e0d594d7da1e2d72a3d63adc1bd",
    "0x66a5cfb2e9c529f14fe6364ad1075df3a649c0a5",
    "0xb91d905a698c28b73c61af60c63919b754fcf4de",
    "0xe79a6d29bb0520648f25d11d65e29fb06b195f0f",
    "0x0c0dc1171258694635aa50cec5845ac1031ca6d7",
    "0x8fda5a7a8dca67bbcdd10f02fa0649a937215422",
	"0x0616e5762c1e7dc3723c50663df10a162d690a86",
	"0x28731bcc616b5f51dd52cf2e4df0e78dd1136c06",
	"0x8cb537fc92e26d8ebbb760e632c95484b6ea3e28",
	"0x611841b24e43c4acfd290b427a3d6cf1a59dac8e",
	"0xf84268fa8eb857c2e4298720c1c617178f5e78e1",
	"0xe10ff11b809f8ee07b056b452c3b2caa7fe24f89",
	"0x0c68a7c72f074d1c45c16d41fa74eebc6d16a65c",
    "0xfa995b6540cbbe2becc00f00d5b8ce73523d1b51",
    "0x621425a1ef6abe91058e9712575dcc4258f8d091",
	"0x2da10a1e27bf85cedd8ffb1abbe97e53391c0295",
	"0xbb05918e9b4ba9fe2c8384d223f0844867909ffb",
	"0xf2dad89f2788a8cd54625c60b55cd3d2d0aca7cb",
	"0x5b9f21d407f35b10cbfddca17d5d84b129356ea3",
	"0x9b5def958d0f3b6955cbea4d5b7809b2fb26b059",
	"0xfdfe03bae6b8113ee1002d2be453fb71ca5783d3",
	"0x0a34fbdf37c246c0b401da5f00abd6529d906193",
	"0x81251524898774f5f2fcae7e7ae86112cb5c317f",
	"0x63ad090242b4399691d3c1e2e9df4c2d88906ebb",
	"0x52a1865eb6903bc777a02ae93159105015ca1517",
	"0x3de80d2d9dca6f6357c77ef89ee1f7db3bba3c3f",
	"0x7f4cb0666b700df62e7fd0ab30e7c354aa0a1890",
	"0x48237655efc513a79409882643ec987591dd6a81",
    "0x2da10a1e27bf85cedd8ffb1abbe97e53391c0295",
    "0x9b5def958d0f3b6955cbea4d5b7809b2fb26b059",
    "0x621425a1ef6abe91058e9712575dcc4258f8d091",
    "0xf2dad89f2788a8cd54625c60b55cd3d2d0aca7cb",
    "0x5b9f21d407f35b10cbfddca17d5d84b129356ea3",
    "0x28731bcc616b5f51dd52cf2e4df0e78dd1136c06",
    "0x8fda5a7a8dca67bbcdd10f02fa0649a937215422",
    "0x0c68a7c72f074d1c45c16d41fa74eebc6d16a65c",
    "0x3de80d2d9dca6f6357c77ef89ee1f7db3bba3c3f",
    "0x52a1865eb6903bc777a02ae93159105015ca1517",
    "0x7f4cb0666b700df62e7fd0ab30e7c354aa0a1890",
    "0x48237655efc513a79409882643ec987591dd6a81",
    "0xb1ef06bcc2a8f63597d5779c00d72b2ae4bb592c",
    "0xfa995b6540cbbe2becc00f00d5b8ce73523d1b51",
    "0x389c30dafaec0a0c637c33f39e1bdea75c4ba0e6"
}


if __name__ == "__main__":

    SYSTEM_CONTRACTS_HEX = set()
    for addr in SYSTEM_CONTRACTS:
        SYSTEM_CONTRACTS_HEX.add(convert_to_fixed_size(addr))

    with open("zkSync_token_addresses.txt", "r") as f:
        ERC20_addresses = {line.strip().lower() for line in f if line.strip()}

    ERC20_addresses_lower = [addr.lower() for addr in ERC20_addresses]
    
    ERC20_name = set()
    ERC20_symbol = set()
    ERC20_decimals_map = {}
    with open("zkSync_token_info.txt", "r", encoding="utf-8") as f:
        next(f)
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 4:
                continue
            addr, name, symbol, decimals = parts
            decimals_str = "1" * int(decimals)
            if name and name != "None":
                ERC20_name.add(name.lower())
            if symbol and symbol != "None":
                ERC20_symbol.add(symbol.lower())
            if decimals and decimals != "None":
                ERC20_decimals_map[addr.lower()] = decimals_str

    path_data = get_zksync_data()
    print(path_data)


    fake_tokens = set()
    cached_tokens = set()
    
    for i in range(1,29710000,100):
        start = time.perf_counter()
        logs = pl.scan_parquet(path_data['logs']).filter(
            (pl.col("blockNumber") >= i) & (pl.col("blockNumber") < i+1000) & (pl.col("topics_0").str.to_lowercase() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
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
            ~(pl.col("sender").is_in(list(SYSTEM_CONTRACTS_HEX))) &
            ~(pl.col("receiver").is_in(list(SYSTEM_CONTRACTS_HEX)))
        ).collect()
        print(i)
        #FIRST GOES FAKE TRANSFER IDENTIFICATION THEN ZERO AND THEN DUST, EACH ONE REMOVES INSTANCES FROM THE LOGS DATAFRAME
        fake_df = fake_transfer(logs, ERC20_addresses, ERC20_name, ERC20_symbol, fake_tokens, cached_tokens)
        logs = logs.join(fake_df, on=["blockNumber", "topics_0", "data_decimal", "address", "transactionHash"], how="anti")
        zero_df = zero_transfer(logs)
        logs = logs.join(zero_df, on=["blockNumber", "topics_0", "data_decimal", "address", "transactionHash"], how="anti")
        dust_df = dust_transfer(logs, ERC20_decimals_map)
        logs = logs.join(dust_df, on=["blockNumber", "topics_0", "data_decimal", "address", "transactionHash"], how="anti")

        batch = StepsRunner(dust_df=dust_df,zero_df=zero_df,fake_df=fake_df,path_data=path_data, ERC20_decimals_map=ERC20_decimals_map,SYSTEM_CONTRACTS_HEX=SYSTEM_CONTRACTS_HEX)
        batch.run_detection()
        
        del logs, batch
        end = time.perf_counter()
        print("Elapsed time: ", end - start, "seconds")
