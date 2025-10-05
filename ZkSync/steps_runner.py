import polars as pl
from find_previous_transfers import find_previous_transfers
from helpers import convert_to_fixed_size, to_ethereum_address
import csv
import os

class StepsRunner:
    def __init__(self, dust_df=None, zero_df=None, fake_df=None, path_data=None, ERC20_decimals_map=None, SYSTEM_CONTRACTS_HEX={"0x0000000000000000000000000000000000000000"}, previous_df=None):
        self.dust_df = dust_df
        self.zero_df = zero_df
        self.fake_df = fake_df
        self.path_data = path_data
        self.ERC20_decimals_map = ERC20_decimals_map
        self.SYSTEM_CONTRACTS_HEX = SYSTEM_CONTRACTS_HEX
        self.previous_df = previous_df

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
        score, top_address = self.calculate_similarity_score(to_ethereum_address(attacker_address), to_ethereum_address(victim_address))
        for addr in receiver_addresses:
            a, b = self.calculate_similarity_score(to_ethereum_address(attacker_address), to_ethereum_address(addr))
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
        log_indices = attack_df["logIndex"].to_list()
        if len(victims) != len(attackers) != len(txhashes) != len(log_indices):
            print("length of dust_df arrays is different")
            exit(1)
        print(len(victims))
        print(len(attackers))
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
                    "attacker": to_ethereum_address(attackers[i]),
                    "victim": to_ethereum_address(victims[i])
                }
        return result_map
    
    def check_time_before_poisoning(self, attack_df=None, previous_df=None, attack_type=None):
        if attack_type == "dust":
            victims = attack_df["receiver"].to_list()
            attackers = attack_df["sender"].to_list()
        else:
            victims = attack_df["sender"].to_list()
            attackers = attack_df["receiver"].to_list()
        log_indices = attack_df["logIndex"].to_list()
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
            if delta < 1200: #An L2 block is generated every 1 second, so based on previous work we set it to 20 minutes
                result_map[(txhashes[i], log_indices[i])] = {
                    "time": delta,
                    "attacker": to_ethereum_address(attackers[i]),
                    "victim": to_ethereum_address(victims[i])
                }
        self.save_map_to_csv(result_map, f"{attack_type}_step1.csv", key_name="transaction_key")
        return result_map
    
    def check_behaviour_dust(self,current_block_max, attack_df=None):
        dust_df = attack_df
        path_data = self.path_data
        ERC20_decimals_map = self.ERC20_decimals_map
        SYSTEM_CONTRACTS_HEX = self.SYSTEM_CONTRACTS_HEX
        logs_path = path_data['logs']
        attackers = dust_df["sender"].to_list()
        blockNumbers = dust_df["blockNumber"].to_list()
        unique_attackers = dust_df["sender"].unique().to_list()
        log_indices = dust_df["logIndex"].to_list()
        txhashes = dust_df["transactionHash"].to_list()
        attacker_set = [addr.lower() for addr in unique_attackers]
        step3_map = {}
        all_transfers = pl.scan_parquet(logs_path).filter(
            (pl.col("topics_0").str.to_lowercase() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") &
            (pl.col("blockNumber") < current_block_max) &
            (pl.col("blockNumber") >= (current_block_max-1000000))
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
            ((pl.col("sender").is_in(attacker_set)) |
            (pl.col("receiver").is_in(attacker_set))) &
            ~(pl.col("sender").is_in(SYSTEM_CONTRACTS_HEX))
        ).collect()

        #if only one OUT tranfer then +1 - return
        for i in range(len(attackers)):
            num_of_out = all_transfers.filter(
                (pl.col("blockNumber") < blockNumbers[i]) &
                (pl.col("sender") == attackers[i])
            ).height
            if num_of_out<2:
                step3_map[(txhashes[i], log_indices[i])] = {"score": 1, "attacker": to_ethereum_address(attackers[i])}
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
            if delta < 1200: #same as before
                step3_map[(txhashes[i], log_indices[i])] = {"score": 1, "attacker": to_ethereum_address(attackers[i])}
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
                    step3_map[(txhashes[i], log_indices[i])] = {"score": 0.5, "attacker": to_ethereum_address(attackers[i])}
            #if more than one OUT but all transfers are dust +0.5
            all_transfers = all_transfers.filter(
                pl.col("address").str.to_lowercase().is_in(ERC20_decimals_map)
            ).with_columns([
                pl.col("address")
                    .str.to_lowercase()
                    .replace(ERC20_decimals_map, return_dtype=pl.Utf8)
                    .alias("decimals")
            ])
            all_out = all_transfers.filter(
                (pl.col("blockNumber") < blockNumbers[i]) &
                (pl.col("sender") == attackers[i]) &
                (pl.col("data_decimal").cast(float) < (10**(pl.col("decimals").str.len_chars()-5)))
            )
            if all_out.height < 1:
                if (txhashes[i], log_indices[i]) in step3_map:
                    step3_map[(txhashes[i], log_indices[i])]["score"] += 0.5
                else:
                    step3_map[(txhashes[i], log_indices[i])] = {"score": 0.5, "attacker": to_ethereum_address(attackers[i])}
        self.save_map_to_csv(step3_map, "dust_step3.csv", key_name="address")
        return step3_map
    
    
    def check_behaviour_zero(self, current_block_max, attack_df=None, attack_type=None):
        path_data = self.path_data
        SYSTEM_CONTRACTS_HEX = self.SYSTEM_CONTRACTS_HEX
        logs_path = path_data['logs']
        attackers = attack_df["receiver"].to_list()
        blockNumbers = attack_df["blockNumber"].to_list()
        log_indices = attack_df["logIndex"].to_list()
        txhashes = attack_df["transactionHash"].to_list()
        unique_attackers = attack_df["receiver"].unique().to_list()
        attacker_set = [addr.lower() for addr in unique_attackers]
        step3_map = {}
        all_transfers = pl.scan_parquet(logs_path).filter(
            (pl.col("topics_0").str.to_lowercase() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") &
            (pl.col("blockNumber") < current_block_max) &
            (pl.col("blockNumber") >= (current_block_max-1000000))
        ).with_columns([
            pl.col("topics_1").alias("sender"),
            pl.col("topics_2").alias("receiver")
        ]).filter(
            ((pl.col("sender").is_in(attacker_set)) |
            (pl.col("receiver").is_in(attacker_set))) &
            ~(pl.col("sender").is_in(SYSTEM_CONTRACTS_HEX))
        ).collect()

        for i in range(len(attackers)): #if no OUT or IN tranfers then +1 - return
            num_of_out = all_transfers.filter(
                (pl.col("blockNumber") < blockNumbers[i]) &
                (pl.col("sender") == attackers[i])
            ).height
            if num_of_out==0:
                step3_map[(txhashes[i], log_indices[i])] = {"score": 1, "attacker": to_ethereum_address(attackers[i])}
            num_of_out = all_transfers.filter(
                (pl.col("blockNumber") < blockNumbers[i]) &
                (pl.col("receiver") == attackers[i])
            ).height
            if num_of_out==0:
                step3_map[(txhashes[i], log_indices[i])] = {"score": 1, "attacker": to_ethereum_address(attackers[i])}
        self.save_map_to_csv(step3_map, f"{attack_type}_step3.csv", key_name="address")
        return step3_map
    
    #get tx receipts df and get rows by transactionhash 
    #(join to dust_df the from column by tx hash)
    #then for each attacker, check if for txhash[i] ''from'' is equal to attacker[i]
    #if sedner of tx is not a sender of a transfer +1 (in all cases: dust, fake, zero)
    def check_if_sender_of_tx(self, attack_df=None, attack_type=None):
        step4_map = {}
        if attack_type != "dust":
            path_data = self.path_data
            receipts_path = path_data['receipts']
            senders = attack_df["sender"].to_list()
            txhashes = attack_df["transactionHash"].to_list()
            log_indices = attack_df["logIndex"].to_list()
            attackers = attack_df["sender"].to_list()
            all_transfers = pl.scan_parquet(receipts_path).filter(
                (pl.col("transactionHash").str.to_lowercase().is_in(txhashes))
            ).collect()
            for i in range(len(txhashes)):
                from_field = all_transfers.filter((pl.col("transactionHash") == txhashes[i]))
                if to_ethereum_address(from_field["from"][0].lower()) != to_ethereum_address(senders[i].lower()):
                    step4_map[(txhashes[i], log_indices[i])] = {"score": 1, "attacker": to_ethereum_address(attackers[i])}
        self.save_map_to_csv(step4_map, f"{attack_type}_step4.csv", key_name="address")
        return step4_map
    
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
                    step5_map[partition["transactionHash"][0]] = {"score": 1, "attacker": to_ethereum_address(senders[i])}
        self.save_map_to_csv(step5_map, f"{attack_type}_step5.csv", key_name="address")
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

    def run_detection(self):
        attack_types = ["dust","fake","zero"]
        attack_filters = ["sender", "receiver", "receiver"]
        df_types = [self.dust_df,self.fake_df,self.zero_df]

        for i in range(0,3):
            if df_types[i].height > 0:
                self.previous_df = find_previous_transfers(df_types[i],self.path_data,df_types[i]["blockNumber"].min(),df_types[i]["blockNumber"].max(),self.ERC20_decimals_map,attack_types[i]=="dust")
                scores_map = self.block_similarity_score(df_types[i],self.previous_df,attack_types[i])
                df_types[i] = df_types[i].filter(
                    #pl.col(attack_filters[i]).is_in([convert_to_fixed_size(value["attacker"]) for value in scores_map.values()])
                    (pl.col("transactionHash") + pl.col("logIndex").cast(pl.Utf8)).is_in([key[0] + str(key[1]) for key in scores_map.keys()])
                )
                if df_types[i].height > 0:
                    self.check_time_before_poisoning(df_types[i],self.previous_df,attack_types[i])
                    if attack_types[i] == "dust":
                        self.check_behaviour_dust(df_types[i]["blockNumber"].max(), df_types[i])
                    else:
                        self.check_behaviour_zero(df_types[i]["blockNumber"].max(), df_types[i], attack_types[i])
                    self.check_if_sender_of_tx(df_types[i],attack_types[i])
                    self.check_if_batched(df_types[i],attack_types[i])
