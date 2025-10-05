import os

def load_symbols_from_prices(path: str) -> set[str]:
    symbols: set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(",")
            if parts:
                symbols.add(parts[0].strip())
    return symbols


def load_symbols_from_info(path: str) -> set[str]:
    symbols: set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        header_skipped = False
        for line in f:
            if not header_skipped:
                header_skipped = True
                continue
            parts = [p.strip() for p in line.strip().split(",")]
            if len(parts) < 3:
                continue
            sym = parts[2]
            if sym and sym != "None":
                symbols.add(sym)
    return symbols


def main():
    prices_file = os.path.join("ZkSync", "zkSync_token_symbols_prices.txt")
    info_file = os.path.join("ZkSync", "zkSync_token_info.txt")
    if not os.path.exists(prices_file):
        prices_file = "zkSync_token_symbols_prices.txt"
    if not os.path.exists(info_file):
        info_file = "zkSync_token_info.txt"

    symbols_prices = load_symbols_from_prices(prices_file)
    symbols_info = load_symbols_from_info(info_file)

    only_in_prices = sorted(symbols_prices - symbols_info)
    only_in_info = sorted(symbols_info - symbols_prices)

    print("Only in zkSync_token_symbols_prices.txt:")
    print(only_in_prices)
    print("\nOnly in zkSync_token_info.txt:")
    print(only_in_info)

    out_path = os.path.join("ZkSync", "symbol_diffs.txt")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("Only in zkSync_token_symbols_prices.txt:\n")
            for s in only_in_prices:
                f.write(s + "\n")
            f.write("\nOnly in zkSync_token_info.txt:\n")
            for s in only_in_info:
                f.write(s + "\n")
        print(f"\nSaved diff lists to {out_path}")
    except Exception:
        pass


if __name__ == "__main__":
    main()


