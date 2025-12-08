from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime

load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
blob_service = BlobServiceClient(f"https://{ACCOUNT_NAME}.blob.core.windows.net", credential=DefaultAzureCredential())

# Generate fake data
df = pd.DataFrame({
    'sample_id': ['CLOUD-001', 'CLOUD-002'],
    'test_date': ['2025-10-01', '2025-10-01'],
    'result': ['POS', 'Negative'], # One Valid, One Error (Negative != NEG)
    'viral_load': [500, 0]
})

# Upload
csv_data = df.to_csv(index=False)
filename = f"manual_upload_{datetime.now().strftime('%M%S')}.csv"
blob_service.get_blob_client("landing-zone", filename).upload_blob(csv_data)

print(f"🚀 Uploaded {filename} to landing-zone")