import pandas as pd
import os
import shutil
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

print("🚀 RUNNING SCRIPT: Pure Pandas Export")

# Load credentials
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# CONFIG
LOCAL_DOWNLOAD_DIR = "temp_lakehouse"
TARGET_PREFIXES = [""] # Download everything

# --- 1. DOWNLOAD (Client-Side Pruning) ---
# (This part is identical to the previous Python script)
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
    if "data.csv" not in blob.name: continue
    
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

print(f"✅ Downloaded {len(downloaded_files)} files.")

if not downloaded_files:
    print("No files found!")
    exit(0)

# --- 2. GENERATE REPORT (PANDAS) ---
print("📦 Generating report with Pandas...")

# Read all CSVs into a list of DataFrames
# dtype=str ensures we don't lose leading zeros during the merge
dfs = [pd.read_csv(f, dtype=str) for f in downloaded_files]

# Stack them all together
full_df = pd.concat(dfs, ignore_index=True)

# Convert types if needed for sorting (e.g. date)
full_df['test_date'] = pd.to_datetime(full_df['test_date'])
full_df = full_df.sort_values(by='test_date', ascending=False)

# Save to CSV
local_filename = "final_cdc_export.csv"
full_df.to_csv(local_filename, index=False)
print(f"✅ Created {local_filename} with {len(full_df)} rows.")

# --- 3. UPLOAD ---
print(f"☁️  Uploading to Azure...")
with open(local_filename, "rb") as data:
    data_client.upload_blob(name="final_cdc_export.csv", data=data, overwrite=True)

print("✅ Success!")