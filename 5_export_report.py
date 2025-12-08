import duckdb
from dotenv import load_dotenv
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Load credentials
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# Get the specific Service Principal secrets for DuckDB
SP_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
SP_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SP_TENANT_ID = os.getenv("AZURE_TENANT_ID")

# --- 1. SETUP DUCKDB & AZURE CONNECTION ---
con = duckdb.connect()
con.sql("INSTALL azure; LOAD azure;")

# FIX: Use SERVICE_PRINCIPAL_SECRET instead of CREDENTIAL_CHAIN
# We explicitly pass the secrets we already have in our environment.
print("🔌 Configuring DuckDB with Service Principal...")
con.sql(f"""
    CREATE SECRET (
        TYPE AZURE,
        PROVIDER SERVICE_PRINCIPAL_SECRET,
        TENANT_ID '{SP_TENANT_ID}',
        CLIENT_ID '{SP_CLIENT_ID}',
        CLIENT_SECRET '{SP_CLIENT_SECRET}',
        ACCOUNT_NAME '{ACCOUNT_NAME}'
    );
""")

# --- 2. GENERATE CSV LOCALLY ---
local_filename = "temp_cdc_export.csv"
print(f"📦 Generating report locally: {local_filename}...")

# Note: We added 'test_date DESC' to ensure the newest data is at the top
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
target_blob_name = "final_cdc_export.csv" 
print(f"☁️  Uploading to Azure container 'data' as: {target_blob_name}...")

credential = DefaultAzureCredential()
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)
data_client = blob_service.get_container_client("data")

with open(local_filename, "rb") as data:
    data_client.upload_blob(name=target_blob_name, data=data, overwrite=True)

print("✅ Upload Complete! The file is ready in the root of your data container.")