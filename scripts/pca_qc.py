#!/usr/bin/env python3
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
from pathlib import Path

# Load batch-corrected TPM matrix
tpm = pd.read_parquet("results/matrix/tpm_matrix.batch_resid.parquet")

# Log-transform
log_tpm = tpm
log_tpm = log_tpm.replace([np.inf, -np.inf], np.nan).fillna(0)

# Recompute variance after ComBat
gene_var = log_tpm.var(axis=1)

# Use lighter filtering now
log_tpm_filtered = log_tpm.loc[gene_var > 0.05]

print("Filtered matrix:", log_tpm_filtered.shape)

# PCA
pca = PCA(n_components=2)
pcs = pca.fit_transform(log_tpm_filtered.T)
exp = pca.explained_variance_ratio_ * 100

# Plot
plt.figure(figsize=(7,6))
plt.scatter(pcs[:,0], pcs[:,1], s=10, alpha=0.75)
plt.xlabel(f"PC1 ({exp[0]:.1f}%)")
plt.ylabel(f"PC2 ({exp[1]:.1f}%)")
plt.title("PCA after ComBat batch correction")
plt.tight_layout()

Path("results/qc").mkdir(parents=True, exist_ok=True)
plt.savefig("results/qc/pca_overview.png", dpi=300)

print("✅ PCA saved → results/qc/pca_overview.png")
