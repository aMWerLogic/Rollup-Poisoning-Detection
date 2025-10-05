import pandas as pd
df = pd.read_excel("arbitrum_zero_payouts.xlsx")
df_unique = df.drop_duplicates(
    subset=["victim", "attacker", "amount", "contract_address", "txhash"],
    keep="first"
)
df_unique.to_excel("arbitrum_zero_payouts_deduped.xlsx", index=False)
print(f"Original rows: {len(df)}")
print(f"Unique rows: {len(df_unique)}")
print("Cleaned file saved as output.xlsx")