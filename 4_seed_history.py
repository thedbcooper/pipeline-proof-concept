import pandas as pd
import random
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import io

# Load Robot Credentials
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

print(f"🌱 Seeding history to {ACCOUNT_NAME}...")

# Connect to Azure
credential = DefaultAzureCredential()
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)
data_client = blob_service.get_container_client("data")

# START DATE: 6 months ago
start_date = datetime.now() - timedelta(weeks=26)

# Generate 26 weeks of data
for i in range(26):
    # Calculate the date for this specific week
    current_week_date = start_date + timedelta(weeks=i)
    year = current_week_date.year
    week = current_week_date.isocalendar()[1]
    
    # 1. Create Fake Data for this week
    num_samples = random.randint(5, 20) # 5-20 samples per week
    data = {
        'sample_id': [f"HIST-{year}-{week}-{x}" for x in range(num_samples)],
        'test_date': [current_week_date.strftime('%Y-%m-%d') for _ in range(num_samples)],
        'result': [random.choice(['POS', 'NEG']) for _ in range(num_samples)],
        'viral_load': [random.randint(0, 10000) for _ in range(num_samples)]
    }
    df = pd.DataFrame(data)

    # 2. Define the Partition Path (The "Folder")
    blob_path = f"data/year={year}/week={week}/data.csv"
    
    # 3. Upload directly to 'data' container (Bypassing the pipeline for speed)
    print(f"   Uploading Week {week} ({year}) -> {blob_path}")
    csv_buffer = df.to_csv(index=False)
    data_client.upload_blob(blob_path, csv_buffer, overwrite=True)

print("✅ History seeding complete!")