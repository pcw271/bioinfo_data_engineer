#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path
import os

tpm = pd.read_parquet("results/matrix/tpm_matrix.batch_resid.parquet")
log = tpm  # already log2 scaled

## Use ORIGINAL TPM (not batch residuals)
#tpm = pd.read_parquet("results/matrix/tpm_matrix.parquet")
#log = np.log2(tpm + 1)


genes = tpm.index

signatures = {
    "immune_cd8": ["CD8A", "CD8B", "GZMB", "IFNG", "CXCL9", "CXCL10"],
    "proliferation": ["MKI67", "TOP2A", "CDK1", "BUB1B", "UBE2C"],
    "emt_stromal": ["COL1A1", "COL3A1", "VIM", "SNAI2", "FN1", "TGFBI"]
}

scores = {}
for name, sig in signatures.items():
    sig_genes = [g for g in sig if g in genes]
    print(name, "matched genes:", sig_genes)
    scores[name] = log.loc[sig_genes].mean(axis=0)

scores_df = pd.DataFrame(scores)
Path("results/qc").mkdir(parents=True, exist_ok=True)
scores_df.to_csv("results/qc/signature_scores.tsv", sep="\t")

print("✅ Signature scores saved → results/qc/signature_scores.tsv")
