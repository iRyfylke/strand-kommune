import json
import requests
from pathlib import Path

CONFIG_PATH = Path("scripts/tables/config.json")

def fetch_table(table_id: str, query_path: str):
    print(f"[INFO] Fetching table {table_id} from SSB…")

    with open(query_path, "r", encoding="utf-8") as f:
        query = json.load(f)

    url = f"https://data.ssb.no/api/v0/no/table/{table_id}/"

    r = requests.post(url, json=query)
    if r.status_code != 200:
        raise RuntimeError(
            f"SSB API error {r.status_code}: {r.text}"
        )

    out_path = Path(f"data/raw/{table_id}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(r.text, encoding="utf-8")

    print(f"[OK] Saved raw data → {out_path}")

def main():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

    for table_id, query_path in config.items():
        fetch_table(table_id, query_path)

if __name__ == "__main__":
    main()
