import os
import zstandard as zstd
import sys

if len(sys.argv) != 2:
    print("wrong arguments")
    exit(1)

if sys.argv[1]!="arbitrum" and sys.argv[1]!="optimism":
    print("possible arguments: optimism or arbitrum")
    exit(1)

def extract_all_zst(source_dir, target_dir):
    os.makedirs(target_dir, exist_ok=True)

    for filename in os.listdir(source_dir):
        if filename.endswith(".tsv.zst"):
            zst_path = os.path.join(source_dir, filename)
            tsv_filename = filename.replace(".tsv.zst", ".tsv")
            tsv_path = os.path.join(target_dir, tsv_filename)

            with open(zst_path, "rb") as compressed:
                dctx = zstd.ZstdDecompressor()
                with open(tsv_path, "wb") as decompressed:
                    dctx.copy_stream(compressed, decompressed)

            print(f"Extracted: {tsv_path}")

folder_name = f"{sys.argv[1]}_erc20_dumps"
#folder_name = f"{sys.argv[1]}_temp"
source_folder = os.path.join("..", folder_name)

folder_name2 = f"data_{sys.argv[1]}"
target_folder = os.path.join("..", folder_name2)

extract_all_zst(source_folder, target_folder)
