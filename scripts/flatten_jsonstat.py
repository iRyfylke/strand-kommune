import json
import pandas as pd
from pathlib import Path

def flatten(table_id: str):
    raw_path = Path(f"data/raw/{table_id}.json")
    out_path = Path(f"data/processed/{table_id}.csv")

    print(f"[INFO] Flattening table {table_id}…")

    with open(raw_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    dims = data["dimension"]
    order = data["id"]
    sizes = data["size"]
    values = data["value"]

    # Hent labels for alle dimensjoner
    labels = {}
    for dim in order:
        cat = dims[dim]["category"]
        labels[dim] = {
            "codes": list(cat["index"].keys()),
            "texts": cat["label"]
        }

    rows = []
    idx = 0

    # Dynamisk nested loop basert på dimensjonsrekkefølgen
    def recurse(level, current):
        nonlocal idx
        if level == len(order):
            rows.append(current | {"verdi": values[idx]})
            idx += 1
            return

        dim = order[level]
        for code in labels[dim]["codes"]:
            recurse(level + 1, current | {
                f"{dim}_kode": code,
                dim: labels[dim]["texts"][code]
            })

    recurse(0, {})

    df = pd.DataFrame(rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(f"[OK] Saved processed CSV → {out_path}")

def main():
    config = json.load(open("scripts/tables/config.json"))
    for table_id in config.keys():
        flatten(table_id)

if __name__ == "__main__":
    main()
