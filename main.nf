nextflow.enable.dsl=2

params.reads = "data_ingest/TCGA/**/*rna_seq.augmented_star_gene_counts.tsv"
params.outdir = "results"

process PROCESS_COUNTS {
    input:
    path file

    output:
    path "counts/*.clean.tsv"

    script:
    sample_id = file.name.split('.')[0]
    """
    mkdir -p counts
    python3 << 'EOF'
import pandas as pd
df = pd.read_csv("${file}", sep="\t", comment="#")
df = df[['gene_name','unstranded','tpm_unstranded']].drop_duplicates()
df.to_csv(f"counts/${sample_id}.clean.tsv", sep="\t", index=False)
EOF
    """
}

workflow {
    Channel
        .fromPath(params.reads)
        | PROCESS_COUNTS
    .view()
}

