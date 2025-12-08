import duckdb
from dotenv import load_dotenv
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

print("🚀 RUNNING SCRIPT VERSION: 4.0 (Legacy SET Method)")

# Load credentials
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

SP_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
SP_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SP_TENANT_ID = os.getenv("AZURE_TENANT_ID")

# --- 1. SETUP DUCKDB (Legacy Mode) ---
con = duckdb.connect()
con.sql("INSTALL azure; LOAD azure;")

print("🔌 Configuring DuckDB variables directly...")

# Instead of CREATE SECRET, we set global configuration variables.
# This works on almost all versions of the Azure extension.
con.sql(f"SET azure_tenant_id = '{SP_TENANT_ID}';")
con.sql(f"SET azure_client_id = '{SP_CLIENT_ID}';")
con.sql(f"SET azure_client_secret = '{SP_CLIENT_SECRET}';")

# FORCE DuckDB to use the Service Principal method (ignore CLI/Managed Identity)
con.sql("SET azure_credential_chain = 'service_principal';")

# --- 2. GENERATE CSV LOCALLY ---
local_filename = "temp_cdc_export.csv"
print(f"📦 Generating report locally: {local_filename}...")

# Run the query
# Note: We don't need a secret name anymore; the global variables handle it.
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

print("✅ Upload Complete!")