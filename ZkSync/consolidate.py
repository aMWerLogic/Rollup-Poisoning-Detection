import pandas as pd
import glob
import re
import os

folder = "results/" 
files = glob.glob(os.path.join(folder, "*.csv"))
groups = {}
for f in files:
    name = os.path.basename(f)
    match = re.search(r"(?:T2)?([A-Za-z]+)_step(\d+)", name)
    if match:
        print(match)
        attack_type = match.group(1).lower()   # dust, fake, zero
        step = match.group(2)                 # step number
        key = (attack_type, step)
        groups.setdefault(key, []).append(f)

for (attack_type, step), group_files in groups.items():
    dfs = [pd.read_csv(f) for f in group_files]
    merged = pd.concat(dfs, ignore_index=True)
    out_name = f"{attack_type}_step{step}_merged.csv"
    merged.to_csv(out_name, index=False)
    print(f"Merged {group_files} → {out_name}")
