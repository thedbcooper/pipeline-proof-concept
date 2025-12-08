import polars as pl
import os
import shutil
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

print("🚀 RUNNING SCRIPT: Polars Export (Parquet Edition)")

# --- CONFIGURATION ---
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# Config settings
LOCAL_DOWNLOAD_DIR = "temp_lakehouse"
TARGET_PREFIXES = [""] # Download everything

# --- 1. DOWNLOAD (Client-Side Pruning) ---
print("🔌 Connecting to Azure...")
credential = DefaultAzureCredential()
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)
data_client = blob_service.get_container_client("data")

print("⬇️  Downloading files...")
blobs = list(data_client.list_blobs())

if os.path.exists(LOCAL_DOWNLOAD_DIR):
    shutil.rmtree(LOCAL_DOWNLOAD_DIR)
os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)

downloaded_files = []

for blob in blobs:
    # CHANGE: We now look for .parquet files
    if "data.parquet" not in blob.name: continue
    
    # Pruning Check
    match = False
    for prefix in TARGET_PREFIXES:
        if blob.name.startswith(prefix):
            match = True
            break
    if not match: continue

    local_path = os.path.join(LOCAL_DOWNLOAD_DIR, blob.name)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    with open(local_path, "wb") as f:
        f.write(data_client.download_blob(blob.name).readall())
    
    downloaded_files.append(local_path)

if not downloaded_files:
    print("No files found!")
    exit(0)

# --- 2. GENERATE REPORT ---
print("📦 Generating report with Polars...")

# CHANGE: Read Parquet instead of CSV
# Parquet already knows 'viral_load' is Int and 'test_date' is Date.
df = pl.read_parquet(downloaded_files)

# DELETED: The 'df.with_columns(...str.to_date...)' block.
# We don't need it anymore because Parquet preserved the Date type!

# Sort descending by date
df = df.sort("test_date", descending=True)

# Save to CSV (Final report usually needs to be CSV for compatibility)
local_filename = "final_cdc_export.csv"
df.write_csv(local_filename)
print(f"✅ Created {local_filename} with {len(df)} rows.")

# --- 3. UPLOAD ---
print(f"☁️  Uploading to Azure...")
with open(local_filename, "rb") as data:
    data_client.upload_blob(name="final_cdc_export.csv", data=data, overwrite=True)

print("✅ Success!")