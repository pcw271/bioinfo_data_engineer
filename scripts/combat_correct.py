#!/usr/bin/env python3
import scanpy as sc
import pandas as pd
import numpy as np
from pathlib import Path

# Load TPM matrix & log transform
tpm = pd.read_parquet("results/matrix/tpm_matrix.parquet")
log_tpm = np.log2(tpm + 1)

# Batch = TCGA tissue source site (barcode positions 6–7)
batches = tpm.columns.str.slice(5,7)

# Convert to AnnData (samples = rows, genes = columns)
adata = sc.AnnData(log_tpm.T)
adata.var_names_make_unique()
adata.obs['batch'] = batches.values

# Run ComBat
sc.pp.combat(adata, key='batch')

# ✅ Correct way to retrieve corrected data
corrected = adata.to_df().T  # samples x genes → transpose back to genes x samples

# Save
outdir = Path("results/matrix")
outdir.mkdir(parents=True, exist_ok=True)
corrected.to_parquet(outdir / "tpm_matrix.combat.parquet")

print("✅ Batch-corrected matrix saved → results/matrix/tpm_matrix.combat.parquet")
print("Matrix shape:", corrected.shape)
