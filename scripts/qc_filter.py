#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path

tpm = pd.read_parquet("results/matrix/tpm_matrix.parquet")

# Calculate library size proxy (sum of TPM values per sample)
lib_size = tpm.sum(axis=0)

print("Library size summary:")
print(lib_size.describe())

# Filter threshold: keep samples above 1e5 total TPM signal
threshold = 1e5
keep_samples = lib_size[lib_size > threshold].index

print(f"Keeping {len(keep_samples)} / {tpm.shape[1]} samples")

# Save filtered matrix
outdir = Path("results/matrix")
outdir.mkdir(exist_ok=True, parents=True)

tpm_filt = tpm[keep_samples]
tpm_filt.to_parquet(outdir / "tpm_matrix.filtered.parquet")

print("✅ Saved filtered matrix:", tpm_filt.shape)
