import os, glob, pandas as pd
import csv
from sqlalchemy import create_engine, text
import json

BASE_DIR = os.path.dirname(os.path.abspath('/Users/pei-chen_wu/bioinfo_data_engineer/database_pipeline'))
DATA_INGEST_DIR = os.path.join(BASE_DIR, 'data_ingest/TCGA/')  # ../data_ingest relative to script
SAMPLE_SHEET = os.path.join(BASE_DIR, "data_ingest/TCGA/gdc_sample_sheet.2025-10-29.tsv")
MYSQL_URL = "mysql+pymysql://root:password@localhost:3306/bioinfo_db"
META_DATA = os.path.join(BASE_DIR, "data_ingest/metadata.cart.2025-10-29.json")

engine = create_engine(MYSQL_URL, pool_pre_ping=True, isolation_level="AUTOCOMMIT")

with engine.connect() as conn:
    try:
        conn.execute(text("SELECT 1"))
        print("[OK] MySQL connection successful")
    except Exception as e:
        print("[ERROR] Connection test failed, rolling back...")
        conn.rollback()
        raise e

"""
Load TCGA RNA-seq augmented STAR gene count data into MySQL
with robust metadata parsing, replicate filtering, and derived IDs.
"""


# --- STEP 1. MAP UUID → SAMPLE BARCODE ---
def build_uuid_to_sample_map(metadata_json_path):
    """
    Robust extraction of UUID → full sample barcode.
    Handles entity_submitter_id (28-char) and fallback to sample_id.
    """
    uuid_map = {}
    with open(metadata_json_path) as f:
        metadata = json.load(f)

    for entry in metadata:
        file_id = entry.get("file_id")
        entity_id = None

        # Prefer entity_submitter_id if available
        try:
            entity_id = entry["associated_entities"][0]["entity_submitter_id"]
        except (KeyError, IndexError, TypeError):
            pass

        # Fallbacks for nested schema variants
        if entity_id is None:
            try:
                entity_id = entry["cases"][0]["samples"][0]["portions"][0]["analytes"][0]["aliquots"][0]["submitter_id"]
            except Exception:
                pass
        if entity_id is None:
            try:
                entity_id = entry["cases"][0]["samples"][0]["submitter_id"]
            except Exception:
                pass
        if entity_id is None:
            try:
                entity_id = entry["cases"][0]["submitter_id"]
            except Exception:
                pass

        if file_id and entity_id:
            uuid_map[file_id] = entity_id

    print(f"[INFO] Mapped {len(uuid_map)} UUIDs → sample barcodes.")
    return uuid_map


# --- STEP 2. ID hierarchy ---
def derive_id_hierarchy(sample_id):
    case_id = sample_id[:12] if len(sample_id) >= 12 else sample_id
    sample_barcode16 = sample_id[:16] if len(sample_id) >= 16 else sample_id
    aliquot_barcode28 = sample_id[:28] if len(sample_id) >= 28 else sample_id
    return case_id, sample_barcode16, aliquot_barcode28


# --- STEP 3. Detect file format ---
def detect_star_genecounts_head(sample_path):
    """
    Detects TCGA RNA-seq augmented STAR gene counts (*.rna_seq.augmented_star_gene_counts.tsv).
    """
    try:
        peek = pd.read_csv(sample_path, sep="\t", nrows=5, skiprows=1)
    except Exception as e:
        print(f"[WARN] Could not read {sample_path}: {e}")
        return None

    expected_cols = {"gene_id", "gene_name", "gene_type", "unstranded", "tpm_unstranded"}
    if expected_cols.intersection(peek.columns):
        return "STAR_GDC_AUGMENTED"
    return None


# --- STEP 4. LOAD DATA ---
print("[INFO] Building UUID → sample mapping...")
uuid_to_sample = build_uuid_to_sample_map(META_DATA)

records = []
print("[INFO] Loading RNA-seq gene count files...")

for uuid, sample_id in uuid_to_sample.items():
    uuid_dir = os.path.join(DATA_INGEST_DIR, uuid)
    if not os.path.isdir(uuid_dir):
        continue

    for file in os.listdir(uuid_dir):
        if not file.endswith(".tsv"):
            continue

        file_path = os.path.join(uuid_dir, file)
        file_type = detect_star_genecounts_head(file_path)
        if file_type != "STAR_GDC_AUGMENTED":
            print(f"[SKIP] {file} (not augmented STAR format)")
            continue

        try:
            df = pd.read_csv(
                file_path,
                sep="\t",
                comment="#",
                usecols=[
                    "gene_id",
                    "gene_name",
                    "gene_type",
                    "unstranded",
                    "stranded_first",
                    "stranded_second",
                    "tpm_unstranded",
                    "fpkm_unstranded",
                    "fpkm_uq_unstranded",
                ],
            )
            df = df[~df["gene_id"].str.startswith("N_")]
            df.rename(columns={"unstranded": "raw_count"}, inplace=True)

            case_id, sample16, aliquot28 = derive_id_hierarchy(sample_id)
            df["file_uuid"] = uuid
            df["case_id"] = case_id
            df["sample_barcode16"] = sample16
            df["aliquot_barcode28"] = aliquot28
            df["sample_id"] = sample_id

            records.append(df)
            print(f"[LOAD] Loaded {uuid} → {sample_id} ({len(df)} genes)")
        except Exception as e:
            print(f"[ERROR] Failed to load {file_path}: {e}")

if not records:
    raise SystemExit("[ERROR] No RNA-seq STAR gene count files found!")

# --- STEP 5. COMBINE AND SAVE ---
df_all = pd.concat(records, ignore_index=True)
print(f"[INFO] Combined {len(df_all):,} rows from {df_all['sample_id'].nunique()} unique samples.")
df_all.to_sql("rna_counts_raw", engine, if_exists="replace", index=False)
print("✅ Uploaded 'rna_counts_raw' table to MySQL.")


# --- STEP 6. SELECT LATEST REPLICATE PER SAMPLE ---
def select_latest_replicates(engine):
    print("[INFO] Selecting latest replicates per sample_barcode16...")
    query = text("""
        CREATE OR REPLACE TABLE rna_counts_unique AS
        SELECT t.*
        FROM rna_counts_raw t
        INNER JOIN (
            SELECT sample_barcode16, MAX(aliquot_barcode28) AS latest_aliquot
            FROM rna_counts_raw
            GROUP BY sample_barcode16
        ) AS sub
        ON t.sample_barcode16 = sub.sample_barcode16
        AND t.aliquot_barcode28 = sub.latest_aliquot;
    """)
    with engine.begin() as conn:
        conn.execute(query)
    print("✅ Created 'rna_counts_unique' table with one replicate per sample.")


select_latest_replicates(engine)
print("🎯 Pipeline completed successfully.")
