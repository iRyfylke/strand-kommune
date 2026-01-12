import json
import requests
from pathlib import Path

BASE_URL = "https://data.ssb.no/api/v0/no/table"

def fetch_table(table_id: str):
    query_path = Path(f"scripts/tables/{table_id}.json")
    raw_out = Path(f"data/raw/{table_id}.json")

    print(f"[INFO] Fetching table {table_id} from SSB…")

    with open(query_path, "r", encoding="utf-8") as f:
        query = json.load(f)

    url = f"{BASE_URL}/{table_id}/"
    response = requests.post(url, json=query)

    if response.status_code != 200:
        raise RuntimeError(
            f"SSB API error {response.status_code}: {response.text}"
        )

    raw_out.parent.mkdir(parents=True, exist_ok=True)
    raw_out.write_text(response.text, encoding="utf-8")

    print(f"[OK] Saved raw data → {raw_out}")

if __name__ == "__main__":
    fetch_table("13566")
