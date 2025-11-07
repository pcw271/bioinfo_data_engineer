#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path

# Load signature scores (log TPM or raw TPM based)
scores = pd.read_csv("results/qc/signature_scores.tsv", sep="\t", index_col=0)

# Z-score normalize to compare across samples
Z = (scores - scores.mean()) / scores.std()

# Main biological axis: Immune – Stromal balance
balance = Z["immune_cd8"] - Z["emt_stromal"]

# Assign TME classes based on tertiles
labels = pd.cut(balance, bins=3, labels=["Stromal_Cold","Neutral","Immune_Hot"])
labels.name = "TME_state"

# Save per-sample
tme = labels.to_frame()
tme.to_csv("tme_states.tsv", sep="\t")

print("✅ TME states saved → tme_states.tsv")
print(tme["TME_state"].value_counts())
