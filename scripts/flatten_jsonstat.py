import json
import pandas as pd
from pathlib import Path

def flatten(table_id: str):
    raw_path = Path(f"data/raw/{table_id}.json")
    out_path = Path(f"data/processed/{table_id}.csv")

    print(f"[INFO] Flattening table {table_id}…")

    with open(raw_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    dim = data["dimension"]
    values = data["value"]

    regnskapsbegrep = list(dim["KOKartkap0000"]["category"]["label"].items())
    years = list(dim["Tid"]["category"]["label"].keys())

    rows = []
    idx = 0

    for code, label in regnskapsbegrep:
        for year in years:
            rows.append({
                "regnskapsbegrep_kode": code,
                "regnskapsbegrep": label,
                "år": year,
                "verdi": values[idx]
            })
            idx += 1

    df = pd.DataFrame(rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(f"[OK] Saved processed CSV → {out_path}")

if __name__ == "__main__":
    flatten("13566")
