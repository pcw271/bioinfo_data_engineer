#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path

print("➡️ Loading original TPM matrix…")
tpm = pd.read_parquet("results/matrix/tpm_matrix.parquet")

# Log-transform TPM → convert to NumPy *right here*
print("➡️ Log2-transforming TPM…")
X = np.log2(tpm + 1).to_numpy().T     # shape: samples × genes, pure NumPy

# Extract batch (TCGA tissue source site = positions 6–7)
print("➡️ Extracting batch variable (TSS site)…")
batch = tpm.columns.str.slice(5,7).to_numpy().reshape(-1, 1)

# One-hot encode batch (drop first to avoid collinearity)
print("➡️ One-hot encoding batch categories…")
cats = sorted(np.unique(batch))
B = np.zeros((batch.shape[0], len(cats)-1))
for i, c in enumerate(cats[1:]):
    B[:, i] = (batch[:,0] == c).astype(float)

print("➡️ Performing OLS regression to remove batch effects…")
BtB_inv = np.linalg.pinv(B.T @ B)
beta = BtB_inv @ B.T @ X             # coefficients
residuals = X - B @ beta             # samples × genes, batch removed

# Re-center gene expression (preserves biological structure)
print("➡️ Re-centering gene means…")
mu = np.mean(X, axis=0, keepdims=True)
residuals_adj = residuals + mu       # samples × genes

# Convert back to DataFrame genes × samples
corrected = pd.DataFrame(residuals_adj.T, index=tpm.index, columns=tpm.columns)

# Save corrected matrix
out = Path("results/matrix/tpm_matrix.batch_resid.parquet")
out.parent.mkdir(parents=True, exist_ok=True)
corrected.to_parquet(out)

print(f"✅ Batch-corrected matrix saved → {out}")
print("Shape:", corrected.shape)
print("Any NaN:", corrected.isna().any().any())
print("Nonzero entries:", (corrected.values != 0).sum())

