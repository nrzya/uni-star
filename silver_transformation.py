import pandas as pd
import boto3
from botocore.exceptions import ClientError
from deltalake.writer import write_deltalake

import os
import re
import unicodedata
from dotenv import load_dotenv

load_dotenv()

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
REPORT_DIR = "reports"
MISSING_VALUE_MARKERS = ["NA", "N/A", "NULL", "null", ""]
QS_FILE_KEYWORD = "QS World University Rankings"
UNIVERSITY_ALIAS = {
    "royal institute of technology": "kth royal institute of technology",
}

# S3 Storage Options for Pandas
PANDAS_STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
}

DELTA_STORAGE_OPTIONS = {
    'AWS_ACCESS_KEY_ID': MINIO_ACCESS_KEY,
    'AWS_SECRET_ACCESS_KEY': MINIO_SECRET_KEY,
    'AWS_ENDPOINT_URL': MINIO_ENDPOINT,
    'AWS_REGION': 'us-east-1',
    'AWS_ALLOW_HTTP': 'true',
    'AWS_S3_ALLOW_UNSAFE_RENAME': 'true'
}

def get_s3_client():
    """
    Initializes and returns a boto3 S3 client configured for the MinIO Data Lake.
    """
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1' # Default for MinIO
    )

def ensure_bucket_exists(s3_client, bucket_name):
    """
    Verifies the existence of an S3 bucket and creates it if it does not exist.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            raise

def normalize_missing_values(df):
    """
    Converts common string missing-value markers to pandas NA and trims text columns.
    """
    cleaned = df.copy()
    text_columns = cleaned.select_dtypes(include=["object", "string"]).columns
    for col in text_columns:
        cleaned[col] = cleaned[col].astype("string").str.strip()
    cleaned.replace(MISSING_VALUE_MARKERS, pd.NA, inplace=True)
    return cleaned

def normalize_university_name(value):
    """
    Normalizes institution names from Wikidata and QS before matching.
    """
    if pd.isna(value):
        return pd.NA
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"\([^)]*\)", "", text)
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return UNIVERSITY_ALIAS.get(text, text)

def find_latest_qs_object_key(s3_client):
    """
    Finds the newest QS ranking workbook in the Bronze bucket, preferring the highest year.
    """
    response = s3_client.list_objects_v2(Bucket=BRONZE_BUCKET)
    candidates = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if QS_FILE_KEYWORD in key and key.endswith(".xlsx"):
            match = re.search(r"(20\d{2})", key)
            year = int(match.group(1)) if match else 0
            candidates.append((year, obj["LastModified"], key))
    if not candidates:
        raise FileNotFoundError(f"No '{QS_FILE_KEYWORD}' workbook found in s3://{BRONZE_BUCKET}")
    return sorted(candidates, reverse=True)[0][2]

def calculate_s3_folder_size(client, bucket, prefix):
    """
    Calculates the total storage size of an S3 folder (prefix) in bytes.
    
    Args:
        client (boto3.client): The active S3 client instance.
        bucket (str): The target bucket name.
        prefix (str): The folder prefix to calculate size for.
        
    Returns:
        int: Total size in bytes.
    """
    total_size = 0
    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                total_size += obj['Size']
    return total_size

def write_storage_comparison_report(rows):
    """
    Persists the storage comparison as a CSV artifact for presentation/audit evidence.
    """
    os.makedirs(REPORT_DIR, exist_ok=True)
    report_path = os.path.join(REPORT_DIR, "storage_comparison.csv")
    report_df = pd.DataFrame(rows)
    report_df.to_csv(report_path, index=False)
    print(f"[ OK ] Storage comparison report written to {report_path}")

def process_silver():
    """
    Executes the Silver layer transformation pipeline.
    
    This pipeline extracts raw datasets from the Bronze layer, standardizes them,
    integrates Startups, Executives, and QS Rankings into a unified schema, 
    applies tier categorization logic, and writes the output as optimized Parquet files.
    """
    print("[INFO] Reading data from Bronze Layer...")
    s3_client = get_s3_client()
    qs_object_key = find_latest_qs_object_key(s3_client)
    print(f"[INFO] Using QS ranking workbook: s3://{BRONZE_BUCKET}/{qs_object_key}")
    
    # Read Kaggle CSV
    df_kaggle = pd.read_csv(
        f"s3://{BRONZE_BUCKET}/kaggle_unicorn_startups.csv",
        storage_options=PANDAS_STORAGE_OPTIONS
    )
    
    # Read Wikidata CSV
    df_wiki = pd.read_csv(
        f"s3://{BRONZE_BUCKET}/wikidata_executive_profile.csv",
        storage_options=PANDAS_STORAGE_OPTIONS
    )
    
    # Read QS Rankings Excel
    df_qs = pd.read_excel(
        f"s3://{BRONZE_BUCKET}/{qs_object_key}",
        storage_options=PANDAS_STORAGE_OPTIONS,
        header=2
    )

    # Data Cleansing for all Bronze inputs
    print("[INFO] Data Cleaning & Transformation...")
    df_kaggle = normalize_missing_values(df_kaggle)
    df_wiki = normalize_missing_values(df_wiki)
    df_qs = normalize_missing_values(df_qs)

    df_wiki.drop_duplicates(inplace=True)
    df_wiki.dropna(subset=['companyLabel', 'executiveLabel', 'universityLabel'], inplace=True)
    df_wiki['degreeLabel'] = df_wiki['degreeLabel'].fillna('Unknown').astype("string")
    df_wiki['companyLabel'] = df_wiki['companyLabel'].astype("string")
    df_wiki['executiveLabel'] = df_wiki['executiveLabel'].astype("string")
    df_wiki['universityLabel'] = df_wiki['universityLabel'].astype("string")
    df_wiki['university_match_key'] = df_wiki['universityLabel'].apply(normalize_university_name).astype("string")

    if 'Valuation_Formatted' in df_kaggle.columns:
        df_kaggle['Valuation_Formatted'] = df_kaggle['Valuation_Formatted'].astype("string")
    for numeric_col in ['Rank', 'Year_Founded', 'Company_Age_Years']:
        if numeric_col in df_kaggle.columns:
            df_kaggle[numeric_col] = pd.to_numeric(df_kaggle[numeric_col], errors='coerce')
    
    # Process QS Rankings
    inst_col = [c for c in df_qs.columns if 'institution' in str(c).lower() or 'name' in str(c).lower()][0]
    rank_col = [c for c in df_qs.columns if 'rank' in str(c).lower()][0]
    
    df_qs_clean = df_qs[[inst_col, rank_col]].copy()
    df_qs_clean.columns = ['universityLabel', 'QS_Rank']
    df_qs_clean = normalize_missing_values(df_qs_clean)
    df_qs_clean.dropna(subset=['universityLabel'], inplace=True)
    df_qs_clean['universityLabel'] = df_qs_clean['universityLabel'].astype("string")
    df_qs_clean['university_match_key'] = df_qs_clean['universityLabel'].apply(normalize_university_name).astype("string")
    
    df_qs_clean['QS_Rank_Num'] = df_qs_clean['QS_Rank'].astype(str).str.extract(r'(\d+)').astype(float)
    df_qs_clean = df_qs_clean.sort_values('QS_Rank_Num').drop_duplicates('university_match_key', keep='first')
    
    # Merge Wikidata with QS to Flag Top Tier
    df_wiki_enriched = df_wiki.merge(
        df_qs_clean[['university_match_key', 'QS_Rank', 'QS_Rank_Num']],
        on='university_match_key',
        how='left'
    )
    
    def flag_tier(row):
        rank = row['QS_Rank_Num']
        
        if pd.isna(rank):
            return "Unranked Higher Ed"
        elif rank <= 100:
            return "Top Tier Elite (Top 100)"
        elif rank <= 500:
            return "Mid Tier (101-500)"
        else:
            return "Non-Top Tier (>500)"
            
    df_wiki_enriched['University_Tier_Flag'] = df_wiki_enriched.apply(flag_tier, axis=1)
    
    # Drop QS_Rank string column to avoid Delta Lake "Null" type error if all are missing
    df_wiki_enriched.drop(columns=['QS_Rank', 'university_match_key'], inplace=True, errors='ignore')
    df_wiki_enriched['University_Tier_Flag'] = df_wiki_enriched['University_Tier_Flag'].astype(str)
    
    print("[INFO] Writing Cleansed Data to Silver Layer (Delta Format)...")
    ensure_bucket_exists(s3_client, SILVER_BUCKET)

    # Write Delta Tables to S3
    write_deltalake(
        f"s3://{SILVER_BUCKET}/unicorn_startups",
        df_kaggle,
        storage_options=DELTA_STORAGE_OPTIONS,
        mode="overwrite"
    )
    
    write_deltalake(
        f"s3://{SILVER_BUCKET}/executive_profiles",
        df_wiki_enriched,
        storage_options=DELTA_STORAGE_OPTIONS,
        mode="overwrite"
    )

    print("------ File Format Storage Comparison ------")
    s3 = get_s3_client()
    
    # Bronze Size
    bronze_kaggle = s3.head_object(Bucket=BRONZE_BUCKET, Key='kaggle_unicorn_startups.csv')['ContentLength']
    bronze_wiki = s3.head_object(Bucket=BRONZE_BUCKET, Key='wikidata_executive_profile.csv')['ContentLength']
    bronze_qs = s3.head_object(Bucket=BRONZE_BUCKET, Key=qs_object_key)['ContentLength']
    total_bronze = bronze_kaggle + bronze_wiki + bronze_qs
    
    # Silver Size (Delta/Parquet)
    silver_kaggle = calculate_s3_folder_size(s3, SILVER_BUCKET, 'unicorn_startups/')
    silver_wiki = calculate_s3_folder_size(s3, SILVER_BUCKET, 'executive_profiles/')
    total_silver = silver_kaggle + silver_wiki
    reduction_pct = ((total_bronze - total_silver) / total_bronze) * 100 if total_bronze else 0
    comparison_rows = [
        {"layer": "bronze", "dataset": "kaggle_unicorn_startups", "format": "csv", "size_bytes": bronze_kaggle},
        {"layer": "bronze", "dataset": "wikidata_executive_profile", "format": "csv", "size_bytes": bronze_wiki},
        {"layer": "bronze", "dataset": qs_object_key, "format": "xlsx", "size_bytes": bronze_qs},
        {"layer": "silver", "dataset": "unicorn_startups", "format": "delta/parquet", "size_bytes": silver_kaggle},
        {"layer": "silver", "dataset": "executive_profiles", "format": "delta/parquet", "size_bytes": silver_wiki},
        {"layer": "summary", "dataset": "bronze_total", "format": "raw", "size_bytes": total_bronze},
        {"layer": "summary", "dataset": "silver_total", "format": "delta/parquet", "size_bytes": total_silver},
        {"layer": "summary", "dataset": "storage_reduction_pct", "format": "metric", "size_bytes": round(reduction_pct, 2)},
    ]
    write_storage_comparison_report(comparison_rows)
    
    print(f"Bronze Layer (Raw CSV/Excel): {total_bronze / 1024:.2f} KB")
    print(f"Silver Layer (Compressed Delta/Parquet): {total_silver / 1024:.2f} KB")
    print(f"Storage Reduction: {reduction_pct:.2f}%")
    print("--------------------------------------------")

    print("[INFO] Silver Transformation Process Completed.")

if __name__ == "__main__":
    process_silver()
