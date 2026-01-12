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

    # Dimensjoner
    regnskapsdim = dim["KOKartkap0000"]["category"]["label"]
    årdim = dim["Tid"]["category"]["label"]
    regiondim = dim["KOKkommuneregion0000"]["category"]["label"]
    omfangdim = dim["KOKregnskapsomfa0000"]["category"]["label"]
    contentsdim = dim["ContentsCode"]["category"]["label"]

    regnskapskoder = list(regnskapsdim.keys())
    årkoder = list(årdim.keys())
    regionkoder = list(regiondim.keys())
    omfangkoder = list(omfangdim.keys())
    contentskoder = list(contentsdim.keys())

    rows = []
    idx = 0

    # Rekkefølgen følger id:["KOKartkap0000","KOKkommuneregion0000","KOKregnskapsomfa0000","ContentsCode","Tid"]
    # Men siden region, omfang og contents har størrelse 1, er det effektivt:
    # for år in årkoder:
    #   for regnskapsbegrep i regnskapskoder:
    #      value[idx]; idx += 1

    regionkode = regionkoder[0]
    omfangkode = omfangkoder[0]
    contentskode = contentskoder[0]

    for år in årkoder:
        for regnskapskode in regnskapskoder:
            verdi = values[idx]
            idx += 1

            rows.append({
                "år": år,
                "region_kode": regionkode,
                "region": regiondim[regionkode],
                "regnskapsomfang_kode": omfangkode,
                "regnskapsomfang": omfangdim[omfangkode],
                "statistikkvariabel_kode": contentskode,
                "statistikkvariabel": contentsdim[contentskode],
                "regnskapsbegrep_kode": regnskapskode,
                "regnskapsbegrep": regnskapsdim[regnskapskode],
                "verdi": verdi
            })

    df = pd.DataFrame(rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(f"[OK] Saved processed CSV → {out_path}")

if __name__ == "__main__":
    flatten("13566")
