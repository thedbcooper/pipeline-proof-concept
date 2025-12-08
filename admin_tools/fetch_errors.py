import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"
LOCAL_ERROR_DIR = "fix_me_please"

os.makedirs(LOCAL_ERROR_DIR, exist_ok=True)

print("🕵️‍♀️ Checking Quarantine...")
credential = DefaultAzureCredential()
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)
container = blob_service.get_container_client("quarantine")

count = 0
for blob in container.list_blobs():
    print(f"   Downloading {blob.name}...")
    with open(f"{LOCAL_ERROR_DIR}/{blob.name}", "wb") as f:
        f.write(container.download_blob(blob.name).readall())
    
    # Optional: Delete from cloud after download so you don't fix it twice?
    # container.delete_blob(blob.name) 
    count += 1

print(f"✅ Downloaded {count} files to folder '{LOCAL_ERROR_DIR}'. Time to fix them!")