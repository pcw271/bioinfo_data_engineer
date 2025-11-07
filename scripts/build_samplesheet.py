#!/usr/bin/env python3
import json, argparse, pandas as pd
from pathlib import Path

parser = argparse.ArgumentParser(description="Build samplesheet for TCGA RNA-Seq augmented STAR counts")
parser.add_argument("--meta", required=True, help="Path to metadata.cart.json")
parser.add_argument("--indir", required=True, help="Directory containing downloaded folders")
parser.add_argument("--out", default="samplesheet.csv", help="Output samplesheet CSV")
args = parser.parse_args()

# Load metadata
meta = json.load(open(args.meta))

indir = Path(args.indir)

# Index all tsv files by exact file_name
tsv_index = {}
for p in indir.rglob("*.rna_seq.augmented_star_gene_counts.tsv"):
    tsv_index[p.name] = str(p)

rows = []

for entry in meta:
    file_name = entry.get("file_name", "")
    if not file_name.endswith("rna_seq.augmented_star_gene_counts.tsv"):
        continue

    if file_name not in tsv_index:
        continue

    path = tsv_index[file_name]

    # extract sample_id (S1) + plate for R1 replicate rule
    try:
        aliquot = entry["associated_entities"][0]["entity_submitter_id"]
        parts = aliquot.split("-")
        sample_id = "-".join(parts[:4])  # TCGA-XX-XXXX-01A
        plate = parts[4] if len(parts) >= 5 else "0"
    except Exception:
        sample_id = file_name.split(".")[0]
        plate = "0"

    rows.append((sample_id, plate, path))

df = pd.DataFrame(rows, columns=["sample_id","plate","path"])

# R1 replicate rule: keep highest lexicographical plate per sample_id
df = df.sort_values(by=["sample_id","plate"])
df = df.groupby("sample_id").tail(1)

df.to_csv(args.out, index=False)
print(f"[OK] samplesheet written → {args.out} (n={len(df)})")
