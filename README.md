# Analitik Founder Grit Berbasis Data Lakehouse

Proyek ini membangun data lakehouse lokal dengan MinIO sebagai object storage dan DuckDB sebagai compute engine. Pipeline mengintegrasikan data startup unicorn, profil eksekutif dari Wikidata, dan QS World University Rankings untuk menganalisis hubungan latar belakang pendidikan founder dengan valuasi perusahaan.

## Arsitektur

Pipeline menggunakan medallion architecture:

1. Bronze layer: data mentah dari file lokal dan Wikidata SPARQL disimpan ke bucket `bronze`.
2. Silver layer: data dibersihkan, dinormalisasi, diperkaya dengan QS ranking, lalu ditulis sebagai Delta Lake table ke bucket `silver`.
3. Gold layer: data Silver dimodelkan menjadi star schema di bucket `gold`.

Skema Gold:

- `dim_company.parquet`
- `dim_executive.parquet`
- `fact_valuation_grit.parquet`

## Sumber Data

- Wikidata SPARQL API: profil perusahaan, eksekutif, universitas, dan gelar.
- Kaggle unicorn startups CSV: valuasi dan metadata perusahaan startup.
- QS World University Rankings Excel: ranking institusi pendidikan.

## Panduan Menjalankan

Jalankan MinIO:

```powershell
docker compose up -d
```

Instal dependency:

```powershell
pip install -r requirements.txt
```

Jalankan pipeline lakehouse:

```powershell
python bronze_ingestion.py
python silver_transformation.py
python gold_star_schema.py
```

Jalankan query analitik dari folder `queries/` melalui DBeaver atau DuckDB. Sebelum menjalankan query, konfigurasi akses DuckDB ke MinIO:

```sql
INSTALL httpfs;
LOAD httpfs;

SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_region='us-east-1';
SET s3_url_style='path';
```

Untuk Query Explanation Analysis, tambahkan `EXPLAIN ANALYZE` sebelum query presentasi di DBeaver:

```sql
EXPLAIN ANALYZE
SELECT ...
```