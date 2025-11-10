#!/usr/bin/env python3
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import argparse
from pathlib import Path
import os


parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output_prefix", required=True)
args = parser.parse_args()

# Load batch-corrected TPM matrix
df = pd.read_parquet(args.input)

# Log transform (avoid log(0))
df = df + 1
df = df.applymap(float).apply(np.log2)

# PCA
pca = PCA(n_components=2)
pc = pca.fit_transform(df.T)

pc_df = pd.DataFrame(pc, index=df.columns, columns=['PC1','PC2'])
print(args.output_prefix)

outdir = Path(args.output_prefix)
outdir.mkdir(parents=True, exist_ok=True)
pc_df.to_csv(outdir/"pca.tsv", sep="\t")

# Plot
plt.figure(figsize=(6,6))
plt.scatter(pc_df['PC1'], pc_df['PC2'], s=10)
plt.xlabel(f"PC1 ({pca.explained_variance_ratio_[0]*100:.1f}%)")
plt.ylabel(f"PC2 ({pca.explained_variance_ratio_[1]*100:.1f}%)")
plt.savefig(outdir/"pca.png", dpi=200)