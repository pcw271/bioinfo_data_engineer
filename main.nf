#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

params.samplesheet = params.samplesheet ?: "samplesheet.csv"
params.outdir      = params.outdir ?: "results"

//
// Define input channel (top level)
//
/*
samples_ch = Channel
    .fromPath(params.samplesheet)
    .splitCsv(header:true)
    .map { row ->
        def f = file(row.path)
        if( !f.exists() )
            error "❌ File does not exist: ${f}"
        tuple(row.sample_id as String, f)
    }
samples_ch.view()
*/
//
// CLEAN_COUNTS process
//
process CLEAN_COUNTS {
    tag "$sample_id"
    publishDir "${params.outdir}/counts", mode: 'copy'

    input:
    tuple val(sample_id), path(count_file)

    output:
    tuple val(sample_id), path("${sample_id}.clean.tsv")

    conda "/Users/pei-chen_wu/miniconda3/envs/bioinfo_data"


    script:
    """
    echo ">>> CLEANING: ${sample_id}"
    echo "PROJECT DIR = ${projectDir}"
    python3 ${projectDir}/scripts/clean_counts.py \
        --input ${count_file} \
        --output ${sample_id}.clean.tsv
    """
}

process BUILD_MATRICES {
    tag "matrix_build"
    publishDir "${params.outdir}/matrix", mode: 'copy'

    input:
    val cleaned_files

    output:
    path("tpm_matrix.parquet"),   emit: tpm_matrix
    path("count_matrix.parquet"), emit: count_matrix

    conda "/Users/pei-chen_wu/miniconda3/envs/bioinfo_data"

    script:
    """
    python3 ${projectDir}/scripts/build_matrix.py \
    --counts_dir ${projectDir}/results/counts

    """


}


process PCA_QC {
    tag "pca_qc"
    publishDir "${params.outdir}/pca", mode: 'copy'

    input:
    path tpm_matrix

    output:
    path "pca.tsv"
    path "pca.png"


    conda "/Users/pei-chen_wu/miniconda3/envs/bioinfo_data"

    script:
    """
    echo ">>> Running PCA QC"
  
    python3 ${projectDir}/scripts/pca_qc.py \
        --input ${tpm_matrix} \
        --output_prefix .
    """
}


//
// Workflow entry point
//

workflow {

    Channel
        .fromPath(params.samplesheet)
        .splitCsv(header:true)
        .map { row -> 
            tuple(
                row.sample_id,
                file("${projectDir}/${row.path}")
            )
        }
        .set { samples_ch }

    cleaned_ch = CLEAN_COUNTS(samples_ch)

    matrices_ch = BUILD_MATRICES(cleaned_ch)

    PCA_QC(BUILD_MATRICES.out.tpm_matrix)

}

/*
workflow clean_only {
    //cleaned_files_ch = Channel
      //  .fromPath("${projectDir}/results/counts/*.clean.tsv")
      //  .collect()

    samples_ch = Channel
        .fromPath(params.samplesheet)
        .splitCsv(header:true)
        .map { row -> tuple(row.sample_id, file(row.path)) }

    cleaned_ch = CLEAN_COUNTS(samples_ch)

    cleaned_dir = params.outdir + "/counts"


}

*/
/*
workflow pca_only {
    /*
     * Load already built TPM matrix
     */
    Channel
        .fromPath("${params.outdir}/matrix/tpm_matrix.parquet")
        .set { tpm_matrix_ch }

    PCA_QC(tpm_matrix_ch)
}

*/