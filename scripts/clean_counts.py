#!/usr/bin/env python3
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

# Read STAR augmented counts table
df = pd.read_csv(args.input, sep="\t", comment='#')
print(df.head())
# Remove rows that are not actual genes
df = df[~df['gene_id'].str.startswith('N_')]

# Keep only relevant expression columns
df = df[['gene_name', 'unstranded', 'tpm_unstranded']]

# Save cleaned version
df.to_csv(args.output, sep="\t", index=False)

print(f"[CLEAN_COUNTS] Wrote cleaned file → {args.output}")
