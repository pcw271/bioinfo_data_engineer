#!/usr/bin/env python3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from lifelines import KaplanMeierFitter, statistics, CoxPHFitter

# ---- INPUT ----
clinical_file = "clinical.tsv"
tme_file = "tme_states.tsv"
output_km = "KM_by_TME.png"
output_cox = "COX_model_results.tsv"

# ---- LOAD CLINICAL ----
# ---- LOAD CLINICAL ----
clin = pd.read_csv(clinical_file, sep="\t", dtype=str)
clin.columns = [c.lower() for c in clin.columns]

# detect patient ID
for col in ["cases.submitter_id","submitter_id","case_submitter_id","bcr_patient_barcode","patient_id"]:
    if col in clin.columns:
        id_col = col
        break
else:
    raise ValueError("❌ Could not find patient ID column in clinical.tsv")

clin["patient_id"] = clin[id_col].str[:12]

# Convert numeric fields
def to_num(x):
    try: return float(x)
    except: return np.nan

clin["event"] = clin["demographic.vital_status"].str.lower().isin(["dead","deceased"]).astype(int)
clin["death"] = clin["demographic.days_to_death"].map(to_num) if "demographic.days_to_death" in clin.columns else np.nan
clin["follow"] = clin["diagnoses.days_to_last_follow_up"].map(to_num) if "diagnoses.days_to_last_follow_up" in clin.columns else np.nan

# USE MOST RECENT FOLLOW-UP PER PATIENT
clin["time"] = np.where(clin["event"]==1, clin["death"], clin["follow"])

# ✅ collapse to one row per patient: keep the *longest follow-up record*
clin = clin.sort_values("time", ascending=False).drop_duplicates(subset="patient_id", keep="first")

surv = clin[["patient_id","time","event"]].dropna()


# ---- LOAD TME ----
tme = pd.read_csv(tme_file, sep="\t")
tme.columns = ["sample_id","TME_state"]
tme["patient_id"] = tme["sample_id"].str[:12]

merged = surv.merge(tme[["patient_id","TME_state"]], on="patient_id", how="inner")

print("✅ Merged samples:", len(merged))
print("TME counts:\n", merged["TME_state"].value_counts(), "\n")

# ---- KM PLOT ----
km = KaplanMeierFitter()
plt.figure(figsize=(7,6))

for group in merged["TME_state"].unique():
    dt = merged[merged["TME_state"] == group]
    km.fit(dt["time"], event_observed=dt["event"], label=group)
    km.plot(ci_show=False)

plt.title("Kaplan–Meier Survival by TME State (LUAD)")
plt.xlabel("Days")
plt.ylabel("Survival Probability")
plt.legend(title="TME State")
plt.tight_layout()
plt.savefig(output_km, dpi=300)
plt.close()
print(f"✅ Saved KM plot → {output_km}")

# ---- Log-rank tests ----
groups = merged["TME_state"].unique()
print("🔍 Pairwise Log-rank Tests:")
for i,g1 in enumerate(groups):
    for g2 in groups[i+1:]:
        dt1 = merged[merged["TME_state"] == g1]
        dt2 = merged[merged["TME_state"] == g2]
        p = statistics.logrank_test(dt1["time"], dt2["time"], dt1["event"], dt2["event"]).p_value
        print(f"{g1} vs {g2}: p = {p:.3e}")
print()

# ---- COX REGRESSION ----
from lifelines import CoxPHFitter

cox_df = merged.copy()   # merged already contains patient_id, time, event, TME_state

# ---- Add Age if available ----
for col in ["demographic.age_at_index","demographic.age_at_diagnosis"]:
    if col in clin.columns:
        clin[col] = clin[col].map(to_num)
        cox_df = cox_df.merge(clin[["patient_id", col]], on="patient_id", how="left")
        cox_df.rename(columns={col:"age"}, inplace=True)
        break

# ---- Add Tumor Stage if available ----
stage_col = next((c for c in clin.columns if "tumor_stage" in c), None)
if stage_col:
    cox_df = cox_df.merge(clin[["patient_id", stage_col]], on="patient_id", how="left")
    cox_df.rename(columns={stage_col:"stage"}, inplace=True)
    cox_df["stage"] = cox_df["stage"].astype("category")

# ---- Encode TME as categorical (one-hot) ----
cox_df["TME_state"] = cox_df["TME_state"].astype("category")
cox_df = pd.get_dummies(cox_df, columns=["TME_state"], drop_first=True)

# ✅ Keep ONLY valid Cox input columns
cox_covariates = ["time", "event"] + [c for c in ["age", "stage"] if c in cox_df.columns] + \
                  [c for c in cox_df.columns if c.startswith("TME_state_")]

cox_input = cox_df[cox_covariates].dropna()

# Convert stage to dummies if present
if "stage" in cox_input.columns:
    cox_input = pd.get_dummies(cox_input, columns=["stage"], drop_first=True)

# ---- Run Cox ----
cph = CoxPHFitter()
cph.fit(cox_input, duration_col="time", event_col="event")
cph.print_summary()

cph.summary.to_csv("COX_model_results.tsv", sep="\t")
print("✅ Cox regression results saved → COX_model_results.tsv")

