import duckdb
import os
import certifi
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

print("🚀 RUNNING SCRIPT VERSION: 10.0 (Global SSL Env Fix)")

# Load credentials
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# --- 1. SET GLOBAL SSL VARIABLE ---
# This is the magic fix. We tell the underlying C++ libraries where to look.
# We do this in Python's OS environment, which DuckDB inherits.
os.environ['CURL_CA_BUNDLE'] = certifi.where()
os.environ['SSL_CERT_FILE'] = certifi.where() # Set both just to be safe
print(f"🔌 Global SSL Configured: {certifi.where()}")

# --- 2. GET TOKEN VIA PYTHON ---
print("🔑 Requesting Azure Access Token via Python...")
credential = DefaultAzureCredential()
token_object = credential.get_token("https://storage.azure.com/.default")
access_token = token_object.token

# --- 3. CONFIGURE DUCKDB ---
con = duckdb.connect()
con.sql("INSTALL azure; LOAD azure;")
# We don't need to load httpfs explicitly, 'azure' does it, but no harm ensuring it.
con.sql("INSTALL httpfs; LOAD httpfs;") 

print("🔌 Passing Access Token to DuckDB...")
con.sql(f"""
    CREATE SECRET my_azure_secret (
        TYPE AZURE,
        PROVIDER ACCESS_TOKEN,
        ACCESS_TOKEN '{access_token}',
        ACCOUNT_NAME '{ACCOUNT_NAME}'
    );
""")

# --- 4. EXECUTE CLOUD QUERY ---
local_filename = "temp_cdc_export.csv"
print(f"📦 Generating report locally: {local_filename}...")

# This query runs via the native Azure extension
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

# --- 5. UPLOAD REPORT ---
print(f"☁️  Uploading final report to Azure...")
target_blob_name = "final_cdc_export.csv" 
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)
data_client = blob_service.get_container_client("data")

with open(local_filename, "rb") as data:
    data_client.upload_blob(name=target_blob_name, data=data, overwrite=True)

print("✅ Pipeline Success! Cloud Query Complete.")