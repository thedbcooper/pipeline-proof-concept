import duckdb
import os
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

print("🚀 RUNNING SCRIPT VERSION: 11.0 (System Certs Override)")

# Load credentials
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# --- 1. SET ENV VARS TO SYSTEM PATH ---
# Instead of using certifi (inside .venv), we point to the Ubuntu system default.
# This file is guaranteed to exist and be readable on GitHub Actions runners.
system_cert_path = "/etc/ssl/certs/ca-certificates.crt"

if os.path.exists(system_cert_path):
    print(f"🔌 Found System Certs at: {system_cert_path}")
    os.environ['CURL_CA_BUNDLE'] = system_cert_path
    os.environ['SSL_CERT_FILE'] = system_cert_path
else:
    print("⚠️  System certs not found! Falling back to default...")

# --- 2. GET TOKEN ---
print("🔑 Requesting Azure Access Token via Python...")
credential = DefaultAzureCredential()
token_object = credential.get_token("https://storage.azure.com/")
access_token = token_object.token

# --- 3. CONFIGURE DUCKDB ---
con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;") # Load networking first
con.sql("INSTALL azure; LOAD azure;")   # Then load azure

print("🔌 Passing Access Token to DuckDB...")
con.sql(f"""
    CREATE SECRET my_azure_secret (
        TYPE AZURE,
        PROVIDER ACCESS_TOKEN,
        ACCESS_TOKEN '{access_token}',
        ACCOUNT_NAME '{ACCOUNT_NAME}'
    );
""")

# --- 4. EXECUTE QUERY ---
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

# --- 5. UPLOAD ---
print(f"☁️  Uploading final report to Azure...")
target_blob_name = "final_cdc_export.csv" 
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)
data_client = blob_service.get_container_client("data")

with open(local_filename, "rb") as data:
    data_client.upload_blob(name=target_blob_name, data=data, overwrite=True)

print("✅ Pipeline Success!")