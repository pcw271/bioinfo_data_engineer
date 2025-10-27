# 🧬 Bioinformatics Data Engineer Portfolio

### Developer: Pei-Chen Wu  
**Goal:** Build a production-grade bioinformatics data platform integrating genomic data pipelines, databases, and cloud deployment.

---

## 🚀 Project Overview
This portfolio demonstrates how bioinformatics and data engineering intersect to create scalable and queryable genomic data systems.  
It includes:
- **Data ingestion** from public NGS datasets (GEO, TCGA, cBioPortal)
- **Database design** for variant, transcriptomic, and metadata
- **ETL pipeline** transforming raw FASTQ metadata into structured tables
- **Cloud deployment** using Docker, MySQL, and AWS RDS
- **Query optimization and dashboarding** using Python/SQL analytics

---

## 🧩 Folder Structure
| Folder | Description |
|---------|-------------|
| `data_ingest/` | Scripts to download and parse genomics datasets |
| `database_pipeline/` | ETL scripts to load and transform data into databases |
| `database_deploy/` | Docker, CI/CD, and cloud deployment setup |
| `notebooks/` | Exploratory and QC analysis |
| `docs/` | Database schema, diagrams, and metadata model |

---

## 🧠 Technologies
- **Languages:** Python (pandas, sqlalchemy, psycopg2), SQL  
- **Databases:** PostgreSQL, SQLite  
- **Cloud & Tools:** AWS RDS, Docker, GitHub Actions, pgAdmin  
- **Bioinformatics:** GEOparse, Biopython, pyBigWig, pysam  

---

## 📈 Example Pipeline (Simplified)
```bash
python data_ingest/fetch_geo_data.py
python database_pipeline/load_to_postgres.py
docker-compose up -d

