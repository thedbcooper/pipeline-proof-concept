from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# CONFIGURATION
ACCOUNT_NAME = "your_storage_account_name_here" # <--- PUT YOUR NAME HERE
CONTAINER = "landing-zone"

def test_cloud():
    print(f"Connecting to {ACCOUNT_NAME}...")
    
    # 1. The Magic Login (Uses your VS Code login or 'az login')
    credential = DefaultAzureCredential()
    account_url = f"https://{ACCOUNT_NAME}.blob.core.windows.net"
    
    # 2. Connect
    blob_service = BlobServiceClient(account_url, credential=credential)
    container_client = blob_service.get_container_client(CONTAINER)
    
    # 3. List files
    print(f"Checking container '{CONTAINER}'...")
    blobs = list(container_client.list_blobs())
    
    if not blobs:
        print("✅ Connection successful! (Container is empty)")
    else:
        print(f"✅ Connection successful! Found {len(blobs)} files:")
        for blob in blobs:
            print(f" - {blob.name}")

if __name__ == "__main__":
    test_cloud()