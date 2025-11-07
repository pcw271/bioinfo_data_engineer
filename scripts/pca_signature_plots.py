#!/usr/bin/env python3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.decomposition import PCA
from pathlib import Path

# Load corrected matrix
tpm = pd.read_parquet("results/matrix/tpm_matrix.batch_resid.parquet")

# Load signature scores
scores = pd.read_csv("results/qc/signature_scores.tsv", sep="\t", index_col=0)

# Log2 TPM already in corrected matrix
log_tpm = tpm
log_tpm = log_tpm.replace([np.inf, -np.inf], np.nan).fillna(0)

# Filter low-variance genes
gene_var = log_tpm.var(axis=1)
log_tpm_filtered = log_tpm.loc[gene_var > 0.05]

# PCA
pca = PCA(n_components=2)
pcs = pca.fit_transform(log_tpm_filtered.T)
pc_df = pd.DataFrame(pcs, index=log_tpm_filtered.columns, columns=["PC1","PC2"])

# Merge signatures into PCA table
plot_df = pc_df.join(scores)

# Save directory
outdir = Path("results/qc/pca_signature_plots")
outdir.mkdir(parents=True, exist_ok=True)

def plot_sig(name):
    plt.figure(figsize=(7,6))
    sns.scatterplot(
        data=plot_df,
        x="PC1", y="PC2",
        hue=name,
        palette="viridis",
        s=25, alpha=0.9
    )
    plt.title(f"PCA colored by {name}")
    plt.legend(loc="right", bbox_to_anchor=(1.2, 1))
    plt.tight_layout()
    plt.savefig(outdir / f"PCA_{name}.png", dpi=300)
    plt.close()

for sig in ["immune_cd8", "proliferation", "emt_stromal"]:
    plot_sig(sig)

print("✅ Saved plots to:", outdir)
