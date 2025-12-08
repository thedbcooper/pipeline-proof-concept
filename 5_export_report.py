import duckdb
from dotenv import load_dotenv
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Load credentials
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# Connect to the Cloud
print(f"🔌 Connecting to {ACCOUNT_NAME}...")
credential = DefaultAzureCredential()

# --- 1. SETUP DUCKDB & AZURE CONNECTION ---
con = duckdb.connect()
con.sql("INSTALL azure; LOAD azure;")
con.sql(f"""
    CREATE SECRET (
        TYPE AZURE,
        PROVIDER CREDENTIAL_CHAIN,
        ACCOUNT_NAME '{ACCOUNT_NAME}'
    );
""")

# --- 2. GENERATE CSV LOCALLY ---
# We write to a temporary file on the runner first.
local_filename = "temp_cdc_export.csv"
print(f"📦 Generating report locally: {local_filename}...")

con.sql(f"""
    COPY (
        SELECT 
            sample_id,
            test_date,
            result,
            viral_load
        FROM read_csv_auto('azure://{ACCOUNT_NAME}.blob.core.windows.net/data/*/*/data.csv', hive_partitioning=1)
        ORDER BY test_date DESC
    ) TO '{local_filename}' (FORMAT CSV, HEADER)
""")

# --- 3. UPLOAD TO AZURE ROOT ---
# Now we push that local file to the root of the 'data' container
target_blob_name = "final_cdc_export.csv" 
print(f"☁️  Uploading to Azure container 'data' as: {target_blob_name}...")

# Connect to Azure Blob Storage
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)
data_client = blob_service.get_container_client("data")

# Upload (Overwrite if it exists)
with open(local_filename, "rb") as data:
    data_client.upload_blob(name=target_blob_name, data=data, overwrite=True)

print("✅ Upload Complete! The file is ready in the root of your data container.")