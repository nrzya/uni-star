import os
import hashlib
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import socket
import getpass
import requests

import os
from dotenv import load_dotenv

load_dotenv()

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = "bronze"

def get_md5(file_path):
    """
    Calculates the MD5 checksum of a local file in chunks to support large files.
    
    Args:
        file_path (str): The absolute or relative path to the local file.
        
    Returns:
        str: The hexadecimal MD5 checksum string.
    """
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def get_s3_client():
    """
    Initializes and returns a boto3 S3 client configured for the MinIO Data Lake.
    
    Returns:
        boto3.client: Configured S3 client instance.
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
    
    Args:
        s3_client (boto3.client): The active S3 client instance.
        bucket_name (str): The target bucket name to verify or create.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"[INFO] Creating bucket: {bucket_name}")
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            raise

def upload_to_bronze(s3_client, file_path, object_name, source_system="local_file_system"):
    """
    Uploads a file to the Bronze layer with idempotency checks and custom metadata.
    
    This function compares the local file's MD5 hash against the S3 object's E-Tag.
    If the hashes match, the upload is skipped to save bandwidth and compute resources.
    
    Args:
        s3_client (boto3.client): The active S3 client instance.
        file_path (str): Path to the local file.
        object_name (str): The destination object key in S3.
        source_system (str): Origin label stored as Bronze object metadata.
    """
    local_md5 = get_md5(file_path)
    
    # Check if object exists and compare e-Tag
    try:
        response = s3_client.head_object(Bucket=BRONZE_BUCKET, Key=object_name)
        s3_etag = response['ETag'].strip('"')
        
        if s3_etag == local_md5:
            print(f"[SKIP] [{object_name}] File has not changed (MD5 matches E-Tag: {local_md5})")
            return
        else:
            print(f"[INFO] [{object_name}] File changed. Local MD5: {local_md5}, S3 E-Tag: {s3_etag}")
            
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"[INFO] [{object_name}] File not found in MinIO. Uploading...")
        else:
            raise

    # Upload with Custom Metadata
    try:
        operator_val = f"{getpass.getuser()}@{socket.gethostname()}"
    except Exception:
        operator_val = "data_engineer"

    metadata = {
        'ingestion_timestamp': datetime.now(timezone.utc).isoformat(),
        'source_system': source_system,
        'operator_id': operator_val,
        'original_md5': local_md5
    }
    
    print(f"[INFO] [{object_name}] Uploading to {BRONZE_BUCKET}...")
    s3_client.upload_file(
        file_path, 
        BRONZE_BUCKET, 
        object_name,
        ExtraArgs={'Metadata': metadata}
    )
    print(f"[ OK ] [{object_name}] Uploaded with metadata: {metadata}")

import glob

def fetch_wikidata_sparql(query, output_path):
    """
    Fetches data directly from Wikidata SPARQL API and saves it as CSV.
    
    Args:
        query (str): The SPARQL query string to execute.
        output_path (str): The local path where the CSV result will be saved.
    """
    print("[INFO] Fetching live data from Wikidata SPARQL API...")
    url = 'https://query.wikidata.org/sparql'
    try:
        response = requests.get(
            url, 
            params={'query': query}, 
            headers={'Accept': 'text/csv', 'User-Agent': 'Mozilla/5.0 DataLakehouseProject/1.0'}
        )
        response.raise_for_status()
        with open(output_path, 'wb') as f:
            f.write(response.content)
        print(f"[ OK ] Wikidata data downloaded to {output_path}")
    except Exception as e:
        print(f"[ERR ] Failed to fetch Wikidata: {e}")

def main():
    """
    Main execution pipeline for the Bronze Ingestion layer.
    
    Scans the designated raw data directory and processes all supported files
    into the Data Lakehouse, ensuring idempotency and capturing metadata.
    """
    raw_data_dir = os.path.join("data")
    if not os.path.exists(raw_data_dir):
        os.makedirs(raw_data_dir)
        print(f"[INFO] Created directory {raw_data_dir}. Place raw data files here.")
    
    # Fetch live data from Wikidata
    temp_dir = os.path.join(os.path.dirname(__file__), ".temp")
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
        
    wikidata_query = '''SELECT DISTINCT ?companyLabel ?executiveLabel ?universityLabel ?degreeLabel
WHERE {
  ?company wdt:P31/wdt:P279* wd:Q4830453. 
  ?company wdt:P169 ?executive. 
  ?executive wdt:P69 ?university. 
  
  # Hanya memprioritaskan Institusi Pendidikan Tinggi (Universitas, Perguruan Tinggi, Sekolah Bisnis, dll)
  VALUES ?uniType { wd:Q3918 wd:Q189004 wd:Q13220391 wd:Q38723 }
  ?university wdt:P31 ?uniType.
  
  OPTIONAL { ?executive wdt:P512 ?degree. }
  SERVICE wikibase:label { 
    bd:serviceParam wikibase:language "en". 
    ?company rdfs:label ?companyLabel.
    ?executive rdfs:label ?executiveLabel.
    ?university rdfs:label ?universityLabel.
    ?degree rdfs:label ?degreeLabel.
  }
}'''
    wikidata_file = os.path.join(temp_dir, "wikidata_executive_profile.csv")
    fetch_wikidata_sparql(wikidata_query, wikidata_file)
    
    # Identify all supported flat files in the raw dropzone (Manual Files)
    files_to_ingest = []
    for file_path in glob.glob(os.path.join(raw_data_dir, "*")):
        if file_path.endswith((".csv", ".json", ".xml", ".xlsx", ".txt")):
            files_to_ingest.append((file_path, "local_file_system"))
            
    # Explicitly add the API file from .temp to the ingestion list
    files_to_ingest.append((wikidata_file, "wikidata_sparql_api"))
        
    s3_client = get_s3_client()
    ensure_bucket_exists(s3_client, BRONZE_BUCKET)
    
    print(f"[INFO] Starting Advanced Ingestion to Bronze Layer from {raw_data_dir}...")
    if not files_to_ingest:
        print(f"[WARN] No data files found in {raw_data_dir}. Skipping ingestion.")
        return

    for file_path, source_system in files_to_ingest:
        file_name = os.path.basename(file_path)
        upload_to_bronze(s3_client, file_path, file_name, source_system)
            
    print("[ OK ] Ingestion Process Completed.")

if __name__ == "__main__":
    main()
