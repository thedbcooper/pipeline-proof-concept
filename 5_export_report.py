import duckdb
import os
import certifi  # <--- NEW IMPORT
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

print("🚀 RUNNING SCRIPT VERSION: 8.0 (SSL Certificate Fix)")

# Load credentials
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# --- 1. GET TOKEN VIA PYTHON ---
print("🔑 Requesting Azure Access Token via Python...")
credential = DefaultAzureCredential()
token_object = credential.get_token("https://storage.azure.com/.default")
access_token = token_object.token

# --- 2. CONFIGURE DUCKDB ---
con = duckdb.connect()
con.sql("INSTALL azure; LOAD azure;")
con.sql("INSTALL httpfs; LOAD httpfs;") # Explicitly load httpfs just in case

print(f"🔌 Pointing DuckDB to SSL Bundle: {certifi.where()}")

# FIX: Tell DuckDB exactly where the SSL certificates are
# This solves the "Problem with the SSL CA cert" error
con.sql(f"SET http_ca_file = '{certifi.where()}';")

print("🔌 Passing Access Token to DuckDB...")
con.sql(f"""
    CREATE SECRET my_azure_secret (
        TYPE AZURE,
        PROVIDER ACCESS_TOKEN,
        ACCESS_TOKEN '{access_token}',
        ACCOUNT_NAME '{ACCOUNT_NAME}'
    );
""")

# --- 3. EXECUTE CLOUD QUERY ---
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

# --- 4. UPLOAD REPORT ---
print(f"☁️  Uploading final report to Azure...")
target_blob_name = "final_cdc_export.csv" 
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)
data_client = blob_service.get_container_client("data")

with open(local_filename, "rb") as data:
    data_client.upload_blob(name=target_blob_name, data=data, overwrite=True)

print("✅ Pipeline Success! Cloud Query Complete.")