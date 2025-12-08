import duckdb
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, generate_container_sas, ContainerSasPermissions

print("🚀 RUNNING SCRIPT VERSION: 5.0 (Python SAS Generation)")

# Load credentials
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
if not ACCOUNT_NAME:
    raise ValueError("AZURE_STORAGE_ACCOUNT environment variable is not set")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# Service Principal Credentials (needed to sign the SAS token)
SP_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
SP_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SP_TENANT_ID = os.getenv("AZURE_TENANT_ID")

# --- 1. GENERATE SAS TOKEN USING PYTHON ---
# We use the azure library to create a temporary "Guest Pass" for the 'data' container
print("🔑 Generating SAS Token via Python...")

# We need the User Delegation Key (The "Master Key" for the session)
credential = DefaultAzureCredential()
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)
user_delegation_key = blob_service.get_user_delegation_key(
    key_start_time=datetime.now(timezone.utc) - timedelta(minutes=1),
    key_expiry_time=datetime.now(timezone.utc) + timedelta(hours=1)
)

# Create the SAS Token (Valid for 1 hour, Read/List permissions)
sas_token = generate_container_sas(
    account_name=ACCOUNT_NAME,
    container_name="data",
    user_delegation_key=user_delegation_key,
    permission=ContainerSasPermissions(read=True, list=True),
    expiry=datetime.now(timezone.utc) + timedelta(hours=1)
)

# Build the Connection String
# This is the "Universal Language" DuckDB understands perfectly
connection_string = f"BlobEndpoint={ACCOUNT_URL};SharedAccessSignature={sas_token}"

# --- 2. SETUP DUCKDB ---
con = duckdb.connect()
con.sql("INSTALL azure; LOAD azure;")

print("🔌 Configuring DuckDB with SAS Connection String...")
# We use the 'azure_storage_connection_string' variable which exists in ALL versions
con.sql(f"SET azure_storage_connection_string = '{connection_string}';")

# --- 3. GENERATE CSV LOCALLY ---
local_filename = "temp_cdc_export.csv"
print(f"📦 Generating report locally: {local_filename}...")

# Run the query
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

# --- 4. UPLOAD TO AZURE ROOT ---
target_blob_name = "final_cdc_export.csv" 
print(f"☁️  Uploading to Azure container 'data' as: {target_blob_name}...")

data_client = blob_service.get_container_client("data")
with open(local_filename, "rb") as data:
    data_client.upload_blob(name=target_blob_name, data=data, overwrite=True)

print("✅ Upload Complete!")