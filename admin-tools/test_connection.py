from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

# 1. Load the fake environment variables from .env file
load_dotenv()

def test_cloud():
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
    print(f"Connecting to {account_name} as Service Principal...")
    
    # 2. This now ignores 'az login' and uses the ENV vars you set
    credential = DefaultAzureCredential()
    
    account_url = f"https://{account_name}.blob.core.windows.net"
    blob_service = BlobServiceClient(account_url, credential=credential)
    
    # Try to list containers
    containers = blob_service.list_containers()
    print("Success! I found these containers:")
    for c in containers:
        print(f" - {c.name}")

if __name__ == "__main__":
    test_cloud()