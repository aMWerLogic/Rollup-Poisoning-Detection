import csv
import os
import polars as pl
from categorize import dust_transfer, zero_transfer, fake_transfer, get_dump_data
from datetime import datetime, timedelta
import time
from steps_runner import StepsRunner

import sys

if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

if sys.argv[1]=="arbitrum":
    rpc_url="https://arb1.arbitrum.io/rpc"
    # System contracts to exclude from analysis https://docs.arbitrum.io/build-decentralized-apps/reference/contract-addresses
    SYSTEM_CONTRACTS = {
        "0x5288c571Fd7aD117beA99bF60FE0846C4E84F933".lower(),
        "0x09e9222E96E7B4AE2a407B98d48e330053351EEe".lower(),
        "0x096760F208390250649E3e8763348E783AEF5562".lower(),
        "0x6c411aD3E74De3E7Bd422b94A27770f5B86C623B".lower(),
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1".lower(),
        "0xd570aCE65C43af47101fC6250FD6fC63D1c22a86".lower(),
        "0x467194771dAe2967Aef3ECbEDD3Bf9a310C76C65".lower(),
        "0x6D2457a4ad276000A615295f7A80F79E48CcD318".lower(),
        "0x0000000000000000000000000000000000000066".lower(),
        "0x000000000000000000000000000000000000006D".lower(),
        "0x0000000000000000000000000000000000000068".lower(),
        "0x000000000000000000000000000000000000006C".lower(),
        "0x0000000000000000000000000000000000000065".lower(),
        "0x0000000000000000000000000000000000000070".lower(),
        "0x000000000000000000000000000000000000006b".lower(),
        "0x000000000000000000000000000000000000006E".lower(),
        "0x000000000000000000000000000000000000006F".lower(),
        "0x0000000000000000000000000000000000000064".lower(),
        "0x0000000000000000000000000000000000000071".lower(),
        "0x0000000000000000000000000000000000000072".lower(),
        "0x00000000000000000000000000000000000000C8".lower(),
        "0x842eC2c7D803033Edf55E478F461FC547Bc54EB2".lower(),
        "0x0000000000000000000000000000000000000000".lower(),
        "0x000000000000000000000000000000000000dead".lower()
    }
    time_multiplier = 4 #1 block per 0.25s
elif sys.argv[1]=="optimism":
    rpc_url="https://mainnet.optimism.io"
    #https://specs.optimism.io/protocol/predeploys.html
    SYSTEM_CONTRACTS = {
        "0xbeb5fc579115071764c7423a4f12edde41f106ed".lower(),
        "0x25ace71c97b33cc4729cf772ae268934f7ab5fa1".lower(),
        "0x99c9fc46f92e8a1c0dec1b1747d010903e884be1".lower(),
        "0x5a7749f83b81b301cab5f48eb8516b986daef23d".lower(),
        "0x75505a97bd334e7bd3c476893285569c4136fa0f".lower(),
        "0xff00000000000000000000000000000000000010".lower(),
        "0xdfe97868233d1aa22e815a266982f2cf17685a27".lower(),
        "0x6887246668a3b87f54deb3b94ba47a6f63f32985".lower(),
        "0x473300df21d047806a082244b417f96b32f13a33".lower(),
        "0x229047fed2591dbec1ef1118d64f7af3db9eb290".lower(),
        "0xe5965ab5962edc7477c8520243a95517cd252fa9".lower(),
        "0x1c68ecfbf9c8b1e6c0677965b3b9ecf9a104305b".lower(),
        "0x21429af66058bc3e4ae4a8f2ec4531aac433ecbc".lower(),
        "0x323dfc63c9b83cb83f40325aab74b245937cbdf0".lower(),
        "0x1ae178ebfeecd51709432ea5f37845da0414edfe".lower(),
        "0x5738a876359b48a65d35482c93b43e2c1147b32b".lower(),
        "0xf027f4a985560fb13324e943edf55ad6f1d15dc1".lower(),
        "0x1fb8cdfc6831fc866ed9c51af8817da5c287add3".lower(),
        "0x4200000000000000000000000000000000000000".lower(),
        "0x4200000000000000000000000000000000000001".lower(),
        "0x4200000000000000000000000000000000000002".lower(),
        "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000".lower(),
        "0x4200000000000000000000000000000000000006".lower(),
        "0x4200000000000000000000000000000000000007".lower(),
        "0x4200000000000000000000000000000000000010".lower(),
        "0x4200000000000000000000000000000000000011".lower(),
        "0x4200000000000000000000000000000000000012".lower(),
        "0x4200000000000000000000000000000000000013".lower(),
        "0x4200000000000000000000000000000000000014".lower(),
        "0x420000000000000000000000000000000000000f".lower(),
        "0x4200000000000000000000000000000000000015".lower(),
        "0x4200000000000000000000000000000000000016".lower(),
        "0x4200000000000000000000000000000000000017".lower(),
        "0x4200000000000000000000000000000000000018".lower(),
        "0x4200000000000000000000000000000000000019".lower(),
        "0x420000000000000000000000000000000000001a".lower(),
        "0x420000000000000000000000000000000000001b".lower(),
        "0x4200000000000000000000000000000000000020".lower(),
        "0x4200000000000000000000000000000000000021".lower(),
        "0x4200000000000000000000000000000000000022".lower(),
        "0x4200000000000000000000000000000000000023".lower(),
        "0x000f3df6d732807ef1319fb7b8bb8522d0beac02".lower(),
        "0x000000000000000000000000000000000000dead".lower(),
        "0x0000000000000000000000000000000000000000".lower()
    }
    time_multiplier = 0.5 #1 block per 2s
else:
    print(sys.argv[1])
    print("possible arguments: optimism or arbitrum")
    exit(1)


os.makedirs("results", exist_ok=True)

pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_rows(20)

start_time = datetime(2023, 2, 14, 0, 0, 0) #WE HAD BEGUN AT 2023, 2, 14, 0, 0, 0
end_time = datetime(2024, 3, 24, 0, 0, 0)
one_day = timedelta(days=1)


#arbitrum - 1 block per 250 ms
if __name__ == "__main__":

    with open(f"top_{sys.argv[1]}_erc20.txt", "r") as f:
        ERC20_addresses = {line.strip().lower() for line in f if line.strip()}
    
    ERC20_addresses_lower = [addr.lower() for addr in ERC20_addresses]

    ERC20_name = set() #name and symbol are not lower (some legit tokens have case sensitive symbols like USDS and USDs)
    ERC20_symbol = set()
    ERC20_decimals_map = {}
    with open(f"{sys.argv[1]}_token_info.txt", "r", encoding="utf-8") as f:
        next(f)
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 4:
                continue
            addr, name, symbol, decimals = parts
            decimals_str = "1" * int(decimals)
            if name and name != "None":
                ERC20_name.add(name)
            if symbol and symbol != "None":
                ERC20_symbol.add(symbol)
            if decimals and decimals != "None":
                ERC20_decimals_map[addr.lower()] = decimals_str

    path_data = get_dump_data(sys.argv[1])
    print(path_data)

    fake_tokens = set()
    cached_tokens = set()
    if os.path.exists(f"{sys.argv[1]}_fake_tokens.csv"):
        with open(f"{sys.argv[1]}_fake_tokens.csv", "r", encoding="utf-8") as f:
            fake_tokens = {row[0].strip().lower() for row in csv.reader(f) if row}
    if os.path.exists(f"{sys.argv[1]}_cached_tokens.csv"):
        with open(f"{sys.argv[1]}_cached_tokens.csv", "r", encoding="utf-8") as f:
            cached_tokens = {row[0].strip().lower() for row in csv.reader(f) if row}
    current = start_time
    while current < end_time:
        start = time.perf_counter()
        next_day = current + timedelta(hours=0.25)
        logs = (
            pl.scan_parquet(path_data['parquet_data'])
            .filter(
                (pl.col("time") >= pl.lit(current))
                & (pl.col("time") < pl.lit(next_day)) &
                ~(pl.col("sender").str.to_lowercase().is_in(list(SYSTEM_CONTRACTS))) &
                ~(pl.col("receiver").str.to_lowercase().is_in(list(SYSTEM_CONTRACTS)))
            )
        ).collect()

        print(current)
        current = next_day
        print(logs.height)
        fake_df = fake_transfer(logs, ERC20_addresses_lower, ERC20_name, ERC20_symbol, fake_tokens, cached_tokens, rpc_url, sys.argv[1])
        logs = logs.join(fake_df, on=["blockNumber", "time", "sender", "receiver", "transactionHash", "contract", "amount"], how="anti")
        zero_df = zero_transfer(logs,ERC20_decimals_map)
        logs = logs.join(zero_df, on=["blockNumber", "time", "sender", "receiver", "transactionHash", "contract", "amount"], how="anti")
        dust_df = dust_transfer(logs, ERC20_decimals_map)
        logs = logs.join(dust_df, on=["blockNumber", "time", "sender", "receiver", "transactionHash", "contract", "amount"], how="anti")
        print(fake_df.height)
        print(zero_df.height)
        print(dust_df.height)
        batch = StepsRunner(dust_df=dust_df,zero_df=zero_df,fake_df=fake_df,path_data=path_data,
                            ERC20_decimals_map=ERC20_decimals_map,SYSTEM_CONTRACTS_LOWER=SYSTEM_CONTRACTS,
                            rollup_name=sys.argv[1],rpc=rpc_url,time_multiplier=time_multiplier)
        batch.run_detection()

        del logs, batch
        end = time.perf_counter()
        print("Elapsed time: ", end - start, "seconds")