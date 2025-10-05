import polars as pl
from pathlib import Path
pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_rows(5)
pl.Config.set_tbl_cols(12)
import sys

if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

if sys.argv[1]=="arbitrum":
    contract_column="^arbitrum-one-erc-20/"
elif sys.argv[1]=="optimism":
    contract_column="^optimism-erc-20/"
else:
    print("possible arguments: optimism or arbitrum")
    exit(1)

# Paths
tsv_dir = Path(__file__).parent / f"../data_{sys.argv[1]}"  # folder with .tsv files
parquet_dir = Path(__file__).parent / f"../parquet_data_{sys.argv[1]}"
parquet_dir.mkdir(exist_ok=True)

for tsv_file in tsv_dir.glob("*.tsv"):
    print(f"Processing {tsv_file.name}...")

    dtypes = {
    "0": pl.Utf8,
    "1": pl.Utf8,
    "2": pl.UInt64,
    "3": pl.Utf8,
    "4": pl.UInt32,
    "5": pl.Utf8,
    "6": pl.Utf8,
    "7": pl.Utf8,
    "8": pl.Utf8,
    "9": pl.Utf8,
    "10": pl.Utf8,
    "11": pl.Utf8,
    }

    # Read TSV without headers
    df = pl.read_csv(
        tsv_file,
        separator="\t",
        has_header=False,
        null_values=["\\N"],
        dtypes=dtypes
    )

    df = df.with_columns(
        pl.col("5").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
    )

    # Drop first 2 columns + last column
    df = df.select(df.columns[2:-1])

    # Clean column 8 (index 7 after dropping 2 cols)
    col8_name = df.columns[5]
    df = df.with_columns(
        pl.col(col8_name).str.replace(contract_column, "", literal=False)
    )

    # Rename columns
    df = df.rename({
        df.columns[0]: "blockNumber",
        df.columns[1]: "transactionHash",
        df.columns[2]: "id",
        df.columns[3]: "time",
        df.columns[4]: "address",   # will become receiver
        df.columns[5]: "contract",
        df.columns[6]: "sign",
        df.columns[7]: "amount",
        df.columns[8]: "valid"
    })
    

    df = df.with_row_count(name="row_idx")

    # Create a 'sender' column by shifting 'address' column by -1 (next row's address)
    df = df.with_columns(receiver=pl.col("address").shift(-1))

    # Filter to keep only even rows (0-based indexing)
    df = df.filter(pl.col("row_idx") % 2 == 0)

    # Rename original address to receiver
    df = df.rename({"address": "sender"})

    # Drop helper column
    df = df.drop("row_idx")
    df = df.drop("sign")

    # Save as Parquet
    parquet_path = parquet_dir / f"{tsv_file.stem}.parquet"
    df.write_parquet(parquet_path, compression="snappy")
    print(df)
