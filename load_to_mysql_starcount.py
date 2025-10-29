import os, glob, pandas as pd
import csv
from sqlalchemy import create_engine, text

BASE_DIR = os.path.dirname(os.path.abspath('/Users/pei-chen_wu/bioinfo_data_engineer/database_pipeline'))
DATA_INGEST_DIR = os.path.join(BASE_DIR, 'data_ingest/TCGA/')  # ../data_ingest relative to script
SAMPLE_SHEET = os.path.join(BASE_DIR, "data_ingest/TCGA/gdc_sample_sheet.2025-10-29.tsv")
MYSQL_URL = "mysql+pymysql://root:password@localhost:3306/bioinfo_db"

engine = create_engine(MYSQL_URL, pool_pre_ping=True, isolation_level="AUTOCOMMIT")

with engine.connect() as conn:
    try:
        conn.execute(text("SELECT 1"))
        print("[OK] MySQL connection successful")
    except Exception as e:
        print("[ERROR] Connection test failed, rolling back...")
        conn.rollback()
        raise e

sheet = pd.read_csv(SAMPLE_SHEET, sep = "\t")
id2sample = dict(zip(sheet["File ID"], sheet["Sample ID"]))

def build_uuid_to_sample_map(sample_sheet_path):
    df = pd.read_csv(sample_sheet_path, sep = "\t")
    cols = {c.lower().strip(): c for c in df.columns}
    # Common variants seen from GDC exports
    file_id_col  = cols.get("file id") or cols.get("file_id") or cols.get("id") or cols.get("uuid")
    file_name_col= cols.get("file name") or cols.get("file_name") or cols.get("filename")
    sample_col   = cols.get("sample id") or cols.get("sample_id") or cols.get("sample submitter id") \
                   or cols.get("sample submitter_id") or cols.get("sample") or cols.get("entity id")
    # Build maps we can try in order
    mappers = []
    if file_id_col and sample_col:
        mappers.append(dict(zip(df[file_id_col].astype(str), df[sample_col].astype(str))))
    if file_name_col and sample_col:
        mappers.append(dict(zip(df[file_name_col].astype(str), df[sample_col].astype(str))))
    return mappers, (file_id_col, file_name_col, sample_col, list(df.columns))

def detect_star_genecounts_head(sample_path):
    # Peek a few rows/cols to detect STAR geneCounts signature
    peek = pd.read_csv(sample_path, sep="\t", nrows=6, comment="#")
    # STAR geneCounts typically has N_* in first rows col0 and 4 columns total
    looks_star = (peek.shape[1] in (2,3,4)) and any(str(peek.iloc[i,0]).startswith("N_") for i in range(min(4, len(peek))))
    return looks_star

def load_one_expression_file(fpath):
    # Decide if STAR geneCounts
    if detect_star_genecounts_head(fpath):
        df = pd.read_csv(fpath, sep="\t", header=None,
                         names=["gene_id","gene_name", "gene_type", "unstranded"])
        df = df.iloc[4:]  # drop N_* rows
     
        df = df[["gene_id","gene_name", "gene_type", "unstranded"]].rename(columns={"unstranded":"raw_count"})
        df["raw_count"] = pd.to_numeric(df["raw_count"], errors="coerce").fillna(0)
        print(df)
        return df[["gene_id","gene_name", "gene_type", "raw_count"]]
   
    # Else, assume headered HTSeq-style augmented table
    df = pd.read_csv(fpath, sep="\t", comment="#")
    df.columns = [c.strip() for c in df.columns]
    # Pick gene column
    gene_col = None
    for c in df.columns:
        cl = c.lower().replace(" ", "")
        if cl in ("ensembl_gene_id","gene_id","ensemblgeneid","geneid","gene"):
            gene_col = c; break
    if gene_col is None:
        gene_col = df.columns[0]
    # Pick count column
    count_col = None
   
    for name in ("unstranded","raw_count","count","htseq_counts","counts"):
        for c in df.columns:
            if c.lower().replace(" ", "") == name:
                count_col = c; break
        if count_col: break
    if count_col is None:
        # fallback first numeric non-length
        numeric = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        for c in numeric:
            if "length" not in c.lower():
                count_col = c; break
    if count_col is None:
        raise ValueError("No suitable count column found.")
    # Drop summary rows like "__no_feature" or "N_"
    mask = ~df[gene_col].astype(str).str.startswith(("__", "N_"))
    df = df.loc[mask, [gene_col, count_col]].copy()
    df.rename(columns={gene_col:"ensembl_gene_id", count_col:"raw_count"}, inplace=True)
    df["raw_count"] = pd.to_numeric(df["raw_count"], errors="coerce").fillna(0)
    return df[["ensembl_gene_id","raw_count"]]

rows = []
files = glob.glob(os.path.join(DATA_INGEST_DIR, "**", "*.tsv"), recursive=True)

for fpath in files:
    # UUID is the parent folder name
    file_id = os.path.basename(os.path.dirname(fpath))
    sample_id = id2sample.get(file_id)

    if not sample_id:
        # skip files we can't map
        continue
    try:
        sub = load_one_expression_file(fpath)
        sub["sample_id"] = sample_id
        rows.append(sub[["sample_id","ensembl_gene_id","raw_count"]])
    except Exception as e:
        print(f"[SKIP] {file_id}: {e}")

if not rows:
    raise SystemExit("No rows collected. Check UUID→Sample mapping and file patterns.")

merged = pd.concat(rows, ignore_index=True)

# Load raw counts (replace table)
merged.to_sql("rna_counts_raw", engine, if_exists="replace", index=False)
print(f"Loaded {len(merged):,} rows from {merged['sample_id'].nunique()} samples into rna_counts_raw")


