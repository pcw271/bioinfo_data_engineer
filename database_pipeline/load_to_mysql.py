import os
import pandas as pd
import csv
from sqlalchemy import create_engine, text

# --- Dynamically set paths ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # folder of this script
DATA_INGEST_DIR = os.path.join(BASE_DIR, '..', 'data_ingest')  # ../data_ingest relative to script

DB_URI = 'mysql+pymysql://root:password@localhost:3306/bioinfo_db'
engine = create_engine(DB_URI)

for file in os.listdir(DATA_INGEST_DIR):
    if not file.endswith(('.csv', '.txt')):
        print(f"[SKIP] Not CSV/TXT: {file}")
        continue

    csv_path = os.path.join(DATA_INGEST_DIR, file)
    table_name = os.path.splitext(file)[0]
    print("Insert table is ", table_name)

    print(f"[LOAD] Loading {csv_path} ...")
    try:
        rows = []
        with open(csv_path, 'r') as f:
            reader = csv.reader(f, delimiter='\t')  # adjust if comma-delimited
            for row in reader:
                rows.append(row)

        if not rows:
            print(f"[SKIP] No data after parsing: {file}")
            continue

        # --- Pad rows ---
        max_cols = max(len(r) for r in rows)
        for i, r in enumerate(rows):
            if len(r) < max_cols:
                rows[i] = r + [''] * (max_cols - len(r))

        df = pd.DataFrame(rows)
        df.columns = [f'col{i+1}' for i in range(max_cols)]
        df.iloc[:, 0] = df.iloc[:, 0].str.lstrip('!')

        # --- Pivot: first column as column names ---
        new_columns = df.iloc[:, 0].tolist()
        df_values = df.iloc[:, 1:].T
        df_values.columns = new_columns
        df_new = df_values.reset_index(drop=True)
        
        # --- Drop table if exists to avoid column conflicts ---
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`;"))

        # --- Replace blank column names ---
        df_new.columns = [c if c.strip() != "" else f"col_{i+1}" for i, c in enumerate(df_new.columns)]


        cols = pd.Series(df_new.columns)
        for dup in cols[cols.duplicated()].unique(): 
            cols[cols == dup] = [f"{dup}_{i+1}" for i in range(sum(cols == dup))]
        df_new.columns = cols


        # --- Load into MySQL ---
        df_new.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"[LOAD] Loaded {file} into MySQL table `{table_name}`")

    except Exception as e:
        print(f"[ERROR] Failed to process {file}: {e}")

