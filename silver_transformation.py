import pandas as pd
import boto3
from deltalake.writer import write_deltalake

import os
from dotenv import load_dotenv

load_dotenv()

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"

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

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"

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

def process_silver():
    """
    Executes the Silver layer transformation pipeline.
    
    This pipeline extracts raw datasets from the Bronze layer, standardizes them,
    integrates Startups, Executives, and QS Rankings into a unified schema, 
    applies tier categorization logic, and writes the output as optimized Parquet files.
    """
    print("[INFO] Reading data from Bronze Layer...")
    
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
        f"s3://{BRONZE_BUCKET}/2026 QS World University Rankings 1.3 (For qs.com).xlsx",
        storage_options=PANDAS_STORAGE_OPTIONS
    )

    print("[INFO] Data Cleaning & Transformation...")
    
    # Clean Kaggle Data
    df_kaggle = df_kaggle.fillna('N/A')
    
    # Clean Wikidata
    df_wiki['universityLabel'] = df_wiki['universityLabel'].fillna('Unknown')
    df_wiki['universityLabel'] = df_wiki['universityLabel'].str.strip()
    
    # Process QS Rankings
    inst_col = [c for c in df_qs.columns if 'institution' in str(c).lower() or 'name' in str(c).lower()][0]
    rank_col = [c for c in df_qs.columns if 'rank' in str(c).lower()][0]
    
    df_qs_clean = df_qs[[inst_col, rank_col]].copy()
    df_qs_clean.columns = ['universityLabel', 'QS_Rank']
    df_qs_clean['universityLabel'] = df_qs_clean['universityLabel'].astype(str).str.strip()
    
    # Handling non-numeric ranks like "501-510" by taking the first number
    df_qs_clean['QS_Rank_Num'] = df_qs_clean['QS_Rank'].astype(str).str.extract(r'(\d+)').astype(float)
    
    # Merge Wikidata with QS to Flag Top Tier
    df_wiki_enriched = df_wiki.merge(df_qs_clean, on='universityLabel', how='left')
    
    def flag_tier(rank):
        if pd.isna(rank):
            return "Non-Formal / Experience"
        elif rank <= 100:
            return "Top Tier Elite (Top 100)"
        elif rank <= 500:
            return "Mid Tier (101-500)"
        else:
            return "Non-Top Tier (>500)"
            
    df_wiki_enriched['University_Tier_Flag'] = df_wiki_enriched['QS_Rank_Num'].apply(flag_tier)
    
    print("[INFO] Writing Cleansed Data to Silver Layer (Delta Format)...")
    s3_client = get_s3_client()
    try:
        s3_client.create_bucket(Bucket=SILVER_BUCKET)
    except:
        pass # Bucket might already exist

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

    print("--- File Format Storage Comparison ---")
    s3 = get_s3_client()
    
    # Bronze Size
    bronze_kaggle = s3.head_object(Bucket=BRONZE_BUCKET, Key='kaggle_unicorn_startups.csv')['ContentLength']
    bronze_wiki = s3.head_object(Bucket=BRONZE_BUCKET, Key='wikidata_executive_profile.csv')['ContentLength']
    bronze_qs = s3.head_object(Bucket=BRONZE_BUCKET, Key='2026 QS World University Rankings 1.3 (For qs.com).xlsx')['ContentLength']
    total_bronze = bronze_kaggle + bronze_wiki + bronze_qs
    
    # Silver Size (Delta/Parquet)
    silver_kaggle = calculate_s3_folder_size(s3, SILVER_BUCKET, 'unicorn_startups/')
    silver_wiki = calculate_s3_folder_size(s3, SILVER_BUCKET, 'executive_profiles/')
    total_silver = silver_kaggle + silver_wiki
    
    print(f"[INFO] Bronze Layer (Raw CSV/Excel): {total_bronze / 1024:.2f} KB")
    print(f"[INFO] Silver Layer (Compressed Delta/Parquet): {total_silver / 1024:.2f} KB")
    print(f"[INFO] Storage Reduction: {((total_bronze - total_silver) / total_bronze) * 100:.2f}%")
    print("[SUCCESS] Silver Transformation Process Completed.")

if __name__ == "__main__":
    process_silver()
