import polars as pl
from find_previous_transfers import find_previous_transfers
import csv
import os
import time
from web3 import Web3
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import HTTPError

class StepsRunner:
    def __init__(self, dust_df=None, zero_df=None, fake_df=None, path_data=None, ERC20_decimals_map=None, SYSTEM_CONTRACTS_LOWER=None, previous_df=None, rollup_name=None, rpc=None, time_multiplier=None):
        self.dust_df = dust_df
        self.zero_df = zero_df
        self.fake_df = fake_df
        self.path_data = path_data
        self.ERC20_decimals_map = ERC20_decimals_map
        self.SYSTEM_CONTRACTS_LOWER = SYSTEM_CONTRACTS_LOWER
        self.previous_df = previous_df
        self.rollup_name = rollup_name
        self.rpc = rpc
        self.time_multiplier = time_multiplier

    @staticmethod
    def calculate_similarity_score(attacker, victim):
        if not attacker or not victim:
            return -1000, victim
        if len(attacker) != len(victim):
            return -1000, victim
        attacker = attacker.lower().removeprefix("0x")
        victim = victim.lower().removeprefix("0x")
        if attacker == victim:
            return 0, "0x"+victim
        prefix_match = 0
        for i in range(len(attacker)):
            if attacker[i] == victim[i]:
                prefix_match += 1
            else:
                break
        if prefix_match <= 2:
            return 0, "0x"+victim
        suffix_match = 0
        for i in range(len(attacker)-1, -1, -1):
            if i < prefix_match:
                break
            if attacker[i] == victim[i]:
                suffix_match += 1
            else:
                break
        return (prefix_match + suffix_match), "0x"+victim
    

    def transfer_similarity(self, victim_address, receiver_addresses, attacker_address):
        score, top_address = self.calculate_similarity_score(attacker_address, victim_address)
        for addr in receiver_addresses:
            a, b = self.calculate_similarity_score(attacker_address, addr)
            if a > score:
                score = a
                top_address = b
        return score, top_address
    

    def block_similarity_score(self, attack_df=None, previous_df=None, attack_type=None):
        if attack_type == "dust":
            victims = attack_df["receiver"].to_list()
            attackers = attack_df["sender"].to_list()
        else:
            victims = attack_df["sender"].to_list()
            attackers = attack_df["receiver"].to_list()
        txhashes = attack_df["transactionHash"].to_list()
        log_indices = attack_df["id"].to_list()
        if len(victims) != len(attackers) != len(txhashes) != len(log_indices):
            print("length of dust_df arrays is different")
            exit(1)
        result_map = {}
        for i in range(len(attackers)):
            receivers = previous_df.filter((pl.col("sender") == victims[i]))
            single_receivers = receivers.partition_by("ID")
            try:
                group = single_receivers[0]
                previous_df = previous_df.join(single_receivers[0], on=["sender", "receiver", "blockNumber", "ID"], how="anti")
                receivers_list = single_receivers[0]["receiver"].to_list()
            except IndexError:
                receivers_list = []
            score, top_address = self.transfer_similarity(victims[i], receivers_list, attackers[i])
            if score > 2:
                result_map[(txhashes[i], log_indices[i])] = {
                    "score": score,
                    "top_address": top_address,
                    "attacker": attackers[i],
                    "victim": victims[i]
                }
        self.save_map_to_csv(result_map, f"{self.rollup_name}_{attack_type}_step2.csv", key_name="transaction_key")
        return result_map
    


    def check_time_before_poisoning(self, attack_df=None, previous_df=None, attack_type=None):
        if attack_type == "dust":
            victims = attack_df["receiver"].to_list()
            attackers = attack_df["sender"].to_list()
        else:
            victims = attack_df["sender"].to_list()
            attackers = attack_df["receiver"].to_list()
        log_indices = attack_df["id"].to_list()
        txhashes = attack_df["transactionHash"].to_list()
        result_map = {}
        for i in range(len(victims)):
            receivers = previous_df.filter((pl.col("sender") == victims[i]))
            single_receivers = receivers.partition_by("ID")
            try:
                group = single_receivers[0]
                previous_df = previous_df.join(single_receivers[0], on=["sender", "receiver", "blockNumber", "ID"], how="anti")
            except IndexError:
                continue
            receivers = group.sort("blockNumber", descending=True)
            blocks_before_poisoning = receivers.head(1)["blockNumber"][0]
            block_at_poisoning = receivers["ID"][0]
            delta = block_at_poisoning - blocks_before_poisoning
            if delta < (1200*self.time_multiplier): #An L2 block is generated every 0.25 second, so based on previous works we set it to 20 minutes
                result_map[(txhashes[i], log_indices[i])] = {
                    "time": delta,
                    "attacker": attackers[i],
                    "victim": victims[i]
                }
        self.save_map_to_csv(result_map, f"{self.rollup_name}_{attack_type}_step1.csv", key_name="transaction_key")
        return result_map
    

    def check_behaviour_dust(self,current_block_max, attack_df=None):
        dust_df = attack_df
        path_data = self.path_data
        ERC20_decimals_map = self.ERC20_decimals_map
        logs_path = path_data['parquet_data']
        attackers = dust_df["sender"].to_list()
        blockNumbers = dust_df["blockNumber"].to_list()
        unique_attackers = dust_df["sender"].unique().to_list()
        log_indices = dust_df["id"].to_list()
        txhashes = dust_df["transactionHash"].to_list()
        attacker_set = [addr.lower() for addr in unique_attackers]
        step3_map = {}
        all_transfers = pl.scan_parquet(logs_path).filter(
            (pl.col("blockNumber") < current_block_max) &
            (pl.col("blockNumber") >= (current_block_max-(1000000*self.time_multiplier))) &
            ((pl.col("sender").is_in(attacker_set)) |
            (pl.col("receiver").is_in(attacker_set)))
        ).collect()

        #if only one OUT tranfer then +1 - return
        for i in range(len(attackers)):
            num_of_out = all_transfers.filter(
                (pl.col("blockNumber") < blockNumbers[i]) &
                (pl.col("sender") == attackers[i])
            ).height
            if num_of_out<2:
                step3_map[(txhashes[i], log_indices[i])] = {"score": 1, "attacker": attackers[i]}
        #if dust transfer very close in time to funding transfer +1 - return
        for i in range(len(attackers)):
            if (txhashes[i], log_indices[i]) in step3_map:
                continue
            current_block = blockNumbers[i]
            funding_transfer = all_transfers.filter(
                (pl.col("blockNumber") < blockNumbers[i]) &
                (pl.col("receiver") == attackers[i])
            ).sort(by=["blockNumber"], descending=True)
            if funding_transfer.height > 0:
                funding_block = funding_transfer.head(1)["blockNumber"][0]
                delta = current_block - funding_block
            else:
                delta = float('inf')
            if delta < (1200*self.time_multiplier): #same as before
                step3_map[(txhashes[i], log_indices[i])] = {"score": 1, "attacker": attackers[i]}
        for i in range(len(attackers)):
            if (txhashes[i], log_indices[i]) in step3_map:
                continue
            current_block = blockNumbers[i]
            funding_transfer_len = all_transfers.filter(
                (pl.col("blockNumber") < blockNumbers[i]) &
                (pl.col("receiver") == attackers[i])
            ).height
            #if more than one OUT transfer but only one IN funding transfer in +0.5
            if funding_transfer_len < 2:
                if (txhashes[i], log_indices[i]) in step3_map:
                    step3_map[(txhashes[i], log_indices[i])]["score"] += 0.5
                else:
                    step3_map[(txhashes[i], log_indices[i])] = {"score": 0.5, "attacker": attackers[i]}
            #if more than one OUT but all transfers are dust +0.5
            all_transfers = all_transfers.filter(
                pl.col("contract").str.to_lowercase().is_in(ERC20_decimals_map)
            ).with_columns([
                pl.col("contract")
                    .str.to_lowercase()
                    .replace(ERC20_decimals_map, return_dtype=pl.Utf8)
                    .alias("decimals")
            ])
            all_out = all_transfers.filter(
                (pl.col("blockNumber") < blockNumbers[i]) &
                (pl.col("sender") == attackers[i]) &
                (pl.col("amount").cast(float) < (10**(pl.col("decimals").str.len_chars()-5)))
            )
            if all_out.height < 1:
                if (txhashes[i], log_indices[i]) in step3_map:
                    step3_map[(txhashes[i], log_indices[i])]["score"] += 0.5
                else:
                    step3_map[(txhashes[i], log_indices[i])] = {"score": 0.5, "attacker": attackers[i]}
        self.save_map_to_csv(step3_map, f"{self.rollup_name}_dust_step3.csv", key_name="address")
        return step3_map

    def check_behaviour_zero(self, current_block_max, attack_df=None, attack_type=None):
        path_data = self.path_data
        logs_path = path_data['parquet_data']
        attackers = attack_df["receiver"].to_list()
        blockNumbers = attack_df["blockNumber"].to_list()
        log_indices = attack_df["id"].to_list()
        txhashes = attack_df["transactionHash"].to_list()
        unique_attackers = attack_df["receiver"].unique().to_list()
        attacker_set = [addr.lower() for addr in unique_attackers]
        step3_map = {}
        all_transfers = pl.scan_parquet(logs_path).filter(
            (pl.col("blockNumber") < current_block_max) &
            (pl.col("blockNumber") >= (current_block_max-(1000000*self.time_multiplier))) &
            ((pl.col("sender").is_in(attacker_set)) |
            (pl.col("receiver").is_in(attacker_set)))
        ).collect()

        for i in range(len(attackers)): #if no OUT or IN tranfers then +1 - return
            num_of_out = all_transfers.filter(
                (pl.col("blockNumber") < blockNumbers[i]) &
                (pl.col("sender") == attackers[i])
            ).height
            if num_of_out==0:
                step3_map[(txhashes[i], log_indices[i])] = {"score": 1, "attacker": attackers[i]}
            num_of_out = all_transfers.filter(
                (pl.col("blockNumber") < blockNumbers[i]) &
                (pl.col("receiver") == attackers[i])
            ).height
            if num_of_out==0:
                step3_map[(txhashes[i], log_indices[i])] = {"score": 1, "attacker": attackers[i]}
        self.save_map_to_csv(step3_map, f"{self.rollup_name}_{attack_type}_step3.csv", key_name="address")
        return step3_map


    #if there are many transfers to different receipents in the same transaction +score
    def check_if_batched(self, attack_df=None, attack_type=None):
        step5_map = {}
        partitioned_df = attack_df.partition_by("transactionHash")
        for partition in partitioned_df:
            if partition.height > 1:
                if attack_type == "dust":
                    senders = partition["sender"].unique().to_list()
                else:
                    senders = partition["receiver"].unique().to_list()
                for i in range(len(senders)):
                    step5_map[partition["transactionHash"][0]] = {"score": 1, "attacker": senders[i]}
        self.save_map_to_csv(step5_map, f"{self.rollup_name}_{attack_type}_step5.csv", key_name="address")
        return step5_map
    

    @staticmethod
    def save_map_to_csv(data_map, filename, key_name="key"):
        if not data_map:
            print(f"No data to save to {filename}")
            return
        all_fields = set()
        for key, value in data_map.items():
            if isinstance(value, dict):
                all_fields.update(value.keys())
            else:
                all_fields.add('value')
        all_fields.add(key_name)
        fieldnames = [key_name] + sorted([f for f in all_fields if f != key_name])
        filename = os.path.join("results", filename)
        file_exists = os.path.exists(filename)
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for key, value in data_map.items():
                row = {key_name: key}
                if isinstance(value, dict):
                    row.update(value)
                else:
                    row['value'] = value
                writer.writerow(row)

    def get_tx_sender(self,tx_hash,provider):
        i=1
        while True:
            try:
                tx = provider.eth.get_transaction(tx_hash)
                return tx["from"]
            except ChunkedEncodingError:
                print(f"ChunkedEncodingError failed for {tx_hash}, retrying...")
                time.sleep(3*i)
                continue
            except HTTPError as e:
                i+=1
                time.sleep(3*i)
                continue
    
    def check_if_sender_of_tx(self, attack_df=None, attack_type=None):
        step4_map = {}
        txhashes = attack_df["transactionHash"].to_list()
        log_indices = attack_df["id"].to_list()
        if attack_type != "dust":
            attackers = attack_df["sender"].to_list()
        else:
            attackers = attack_df["receiver"].to_list()
        hash_map_attackers = {}
        hash_map_log = {}
        for i in range(len(txhashes)):
            if txhashes[i] in hash_map_attackers:
                hash_map_attackers[txhashes[i]].append(attackers[i])
            else:
                hash_map_attackers[txhashes[i]] = [attackers[i]]
            if txhashes[i] in hash_map_log:
                hash_map_log[txhashes[i]].append(log_indices[i])
            else:
                hash_map_log[txhashes[i]] = [log_indices[i]]
        txhashes = attack_df["transactionHash"].unique().to_list()
        w3 = Web3(Web3.HTTPProvider(self.rpc, request_kwargs={"timeout": 120}))
        for i in range(len(txhashes)):
            address = self.get_tx_sender(txhashes[i],w3)
            for j in range(len(hash_map_attackers[txhashes[i]])):
                if address.lower() != hash_map_attackers[txhashes[i]][j].lower():
                    step4_map[(txhashes[i], hash_map_log[txhashes[i]][j])] = {"score": 1, "attacker": hash_map_attackers[txhashes[i]][j].lower()}
        self.save_map_to_csv(step4_map, f"{self.rollup_name}_{attack_type}_step4.csv", key_name="address")
        return step4_map
    
    def run_detection(self):
        attack_types = ["dust","fake","zero"]
        attack_filters = ["sender", "receiver", "receiver"]
        df_types = [self.dust_df,self.fake_df,self.zero_df]

        for i in range(0,3):
            print(f"processing {attack_types[i]}")
            if df_types[i].height > 0:
                self.previous_df = find_previous_transfers(df_types[i],self.path_data,df_types[i]["time"].min(),df_types[i]["time"].max(),self.ERC20_decimals_map,attack_types[i]=="dust",self.time_multiplier)
                scores_map = self.block_similarity_score(df_types[i],self.previous_df,attack_types[i])
                df_types[i] = df_types[i].filter(
                    #pl.col(attack_filters[i]).is_in([value["attacker"] for value in scores_map.values()])
                    (pl.col("transactionHash") + pl.col("id").cast(pl.Utf8)).is_in([key[0] + str(key[1]) for key in scores_map.keys()])
                )
                if df_types[i].height > 0:
                    self.check_time_before_poisoning(df_types[i],self.previous_df,attack_types[i])
                    del self.previous_df
                    if attack_types[i] == "dust":
                        self.check_behaviour_dust(df_types[i]["blockNumber"].max(), df_types[i])
                    else:
                        self.check_behaviour_zero(df_types[i]["blockNumber"].max(), df_types[i], attack_types[i])
                    self.check_if_sender_of_tx(df_types[i],attack_types[i])
                    self.check_if_batched(df_types[i],attack_types[i])
            if attack_types == "dust":
                del self.dust_df
            if attack_types == "fake":
                del self.fake_df
            if attack_types == "zero":
                del self.zero_df

           