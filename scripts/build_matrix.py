#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--counts_dir", required=True, help="Directory containing *.clean.tsv files")
parser.add_argument("--output_dir", default=".", help="Output directory")
args = parser.parse_args()

# Location of cleaned per-sample files
COUNTS_DIR = Path(args.counts_dir)
# Collect all .clean.tsv files
files = sorted(COUNTS_DIR.glob("*.clean.tsv"))
if not files:
    raise FileNotFoundError(f"No cleaned files found in {COUNTS_DIR}")

print(f"Found {len(files)} samples")

count_tables = []
tpm_tables = []

for f in files:
    sample_id = f.stem.replace(".clean","")

    df = pd.read_csv(f, sep="\t")

    # Count matrix (integer approx – raw counts)
    count_tables.append(df.set_index("gene_name")["unstranded"].rename(sample_id))

    # TPM matrix (normalized expression)
    tpm_tables.append(df.set_index("gene_name")["tpm_unstranded"].rename(sample_id))

# Combine into matrices
count_matrix = pd.concat(count_tables, axis=1).fillna(0)
tpm_matrix   = pd.concat(tpm_tables, axis=1).fillna(0)

# Make output directories
#count_matrix.to_parquet("count_matrix.parquet")
#tpm_matrix.to_parquet("tpm_matrix.parquet")

outdir = Path(args.output_dir)
outdir.mkdir(parents=True, exist_ok=True)

count_matrix.to_parquet(outdir / "count_matrix.parquet")
tpm_matrix.to_parquet(outdir / "tpm_matrix.parquet")

print("✅ Done!")
print("Raw count matrix:", count_matrix.shape)
print("TPM matrix:", tpm_matrix.shape)

