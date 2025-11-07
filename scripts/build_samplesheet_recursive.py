#!/usr/bin/env python3
import json, argparse, pandas as pd
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--meta", required=True, help="metadata.cart.json")
parser.add_argument("--indir", required=True, help="directory containing subfolders of UUIDs")
parser.add_argument("--out", default="samplesheet.csv")
args = parser.parse_args()

meta = json.load(open(args.meta))
rows = []

# Build index of all .rna_seq.augmented_star_gene_counts.tsv files recursively
tsv_index = {}
for p in Path(args.indir).rglob("*.rna_seq.augmented_star_gene_counts.tsv"):
    uuid = p.stem.split(".")[0]
    tsv_index[uuid] = str(p)

for entry in meta:
    file_id = entry.get("file_id")
    if file_id not in tsv_index:
        continue

    tsv = tsv_index[file_id]

    sample_id = None
    if "samples" in entry and entry["samples"]:
        sample_id = entry["samples"][0].get("sample_id")
    if not sample_id:
        sample_id = file_id

    plate = "0"
    try:
        aliquot_sid = entry["aliquots"][0]["submitter_id"]
        parts = aliquot_sid.split("-")
        if len(parts) >= 5:
            plate = parts[4]
    except:
        pass

    rows.append((sample_id, plate, tsv))

df = pd.DataFrame(rows, columns=["sample_id","plate","path"])
df = df.sort_values(by=["sample_id","plate"])
df = df.groupby("sample_id").tail(1)

df.to_csv(args.out, index=False)
print(f"[OK] samplesheet written → {args.out} (n={len(df)})")
