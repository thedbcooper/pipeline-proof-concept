import duckdb
import os
import shutil
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

print("🚀 RUNNING SCRIPT VERSION: 12.0 (Python Partition Pruning)")

# Load credentials
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# --- CONFIGURATION: PRUNING FILTERS ---
# This is where you control what data gets pulled.
# Example 1: Download EVERYTHING (Default)
TARGET_PREFIXES = [""] 

# Example 2: Download only Year 2025
# TARGET_PREFIXES = ["year=2025"]

# Example 3: Download only specific weeks
# TARGET_PREFIXES = ["year=2025/week=40", "year=2025/week=41"]

LOCAL_DOWNLOAD_DIR = "temp_lakehouse"

# --- 1. CONNECT & LIST FILES ---
print("🔌 Connecting to Azure via Python...")
credential = DefaultAzureCredential()
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)
data_client = blob_service.get_container_client("data")

print("🔍 Listing files in Data Lake...")
blobs = list(data_client.list_blobs())

# Clean up any previous run
if os.path.exists(LOCAL_DOWNLOAD_DIR):
    shutil.rmtree(LOCAL_DOWNLOAD_DIR)
os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)

# --- 2. THE PRUNING LOGIC ---
download_count = 0
skipped_count = 0

print("⬇️  Starting Smart Download...")

for blob in blobs:
    # Filter A: Only look at actual data files (ignore root exports)
    if "data.csv" not in blob.name:
        continue

    # Filter B: PARTITION PRUNING
    # We check if the file starts with any of our target prefixes.
    # If TARGET_PREFIXES is ["year=2025"], this ignores "year=2024" folders.
    match = False
    for prefix in TARGET_PREFIXES:
        if blob.name.startswith(prefix):
            match = True
            break
    
    if not match:
        skipped_count += 1
        continue

    # Logic: Replicate the folder structure locally
    # Blob Name: year=2025/week=40/data.csv
    # Local Path: temp_lakehouse/year=2025/week=40/data.csv
    local_path = os.path.join(LOCAL_DOWNLOAD_DIR, blob.name)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    with open(local_path, "wb") as download_file:
        download_file.write(data_client.download_blob(blob.name).readall())
    
    download_count += 1

print(f"✅ Download Summary: Fetched {download_count} files. Pruned (Skipped) {skipped_count} files.")

if download_count == 0:
    print("⚠️  No files matched your filter! Exiting.")
    exit(0)

# --- 3. DUCKDB QUERY (LOCAL) ---
local_filename = "final_cdc_export.csv"
print(f"📦 Generating report locally: {local_filename}...")

con = duckdb.connect()

# DuckDB's 'hive_partitioning' feature works on local folders too!
# It will see the folders 'year=...' and 'week=...' and treat them as columns.
con.sql(f"""
    COPY (
        SELECT 
            sample_id,
            test_date,
            result,
            viral_load,
            year,
            week
        FROM read_csv_auto('{LOCAL_DOWNLOAD_DIR}/*/*/data.csv', hive_partitioning=1)
        ORDER BY test_date DESC
    ) TO '{local_filename}' (FORMAT CSV, HEADER)
""")

# --- 4. UPLOAD EXPORT ---
print(f"☁️  Uploading final report to Azure...")
target_blob_name = "final_cdc_export.csv" 

with open(local_filename, "rb") as data:
    data_client.upload_blob(name=target_blob_name, data=data, overwrite=True)

print("✅ Pipeline Success! Report uploaded.")