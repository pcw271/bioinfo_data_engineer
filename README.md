# TCGA-LUAD Tumor Microenvironment & Survival Analysis Pipeline

Workflow for processing TCGA Lung Adenocarcinoma (LUAD) bulk RNA-seq data, constructing gene expression matrices, extracting tumor microenvironment (TME) phenotypes, and evaluating clinical relevance via PCA clustering and survival modeling.

This pipeline is:
- **Reproducible** (Nextflow-based processing)
- **Configurable** (conda environments & modular scripts)
- **Clinically interpretable** (survival + Cox regression)

## 🔬 Biological Motivation

Bulk RNA-seq reflects the **cellular ecosystem** of tumor tissue.
From LUAD expression profiles, we infer **Tumor Microenvironment (TME)** phenotypes:

| TME State | Biology | Clinical Implication |
|---|---|---|
| **Immune_Hot** | High CD8 T-cell infiltration | Often **sensitive** to immune checkpoint inhibitors |
| **Neutral** | No dominant program | Intermediate outcomes |
| **Stromal_Cold** | High EMT, fibroblast, TGF-β; immune-excluded | Often **resistant** to immunotherapy and associated with worse prognosis |

## 🧱 Pipeline Overview

Raw TCGA → Nextflow → Clean Count Matrix → TPM → PCA → TME Assignment → KM/Cox Survival

## 📁 Project Structure

bioinfo_data_engineer/
 ├─ main.nf
 ├─ nextflow.config
 ├─ scripts/
 │   ├─ build_matrix.py
 │   ├─ pca_qc.py
 │   ├─ km_by_tme.py
 │   ├─ km_cox_by_tme.py
 ├─ results/        # ignored
 ├─ data_ingest/    # ignored
 └─ README.md

## ⚙️ Running

conda activate bioinfo_data
nextflow run main.nf
python3 scripts/build_matrix.py
python3 scripts/pca_qc.py
python3 scripts/km_by_tme.py
python3 scripts/km_cox_by_tme.py

## 📊 Key Findings

- PCA separates immune-infiltrated vs stromal-excluded tumors.
- Kaplan–Meier: **Stromal_Cold** exhibits worse survival than **Immune_Hot**.
- Cox model (adjusted for age) indicates ~60% higher mortality risk in Stromal_Cold.

## 🔐 Data Policy

Raw TCGA data is not included; must be downloaded from the GDC Data Portal.

## 📣 Citation

Thorsson et al., Cell 2018 — *The Immune Landscape of Cancer*.
