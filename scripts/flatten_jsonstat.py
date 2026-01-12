import json
import pandas as pd
from pathlib import Path

def flatten(table_id: str):
    raw_path = Path(f"data/raw/{table_id}.json")
    out_path = Path(f"data/processed/{table_id}.csv")

    print(f"[INFO] Flattening table {table_id}…")

    with open(raw_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Flattening logikk (din eksisterende eller den forbedrede)
    # …
    # (bruk gjerne den forbedrede versjonen vi laget)

    print(f"[OK] Saved processed CSV → {out_path}")

def main():
    config = json.load(open("scripts/tables/config.json"))
    for table_id in config.keys():
        flatten(table_id)

if __name__ == "__main__":
    main()
