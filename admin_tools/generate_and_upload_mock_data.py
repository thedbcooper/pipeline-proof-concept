import polars as pl
import random
import os
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

print(f"🔌 Connecting to {ACCOUNT_NAME}...")
credential = DefaultAzureCredential()
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)
landing_client = blob_service.get_container_client("landing-zone")

# --- GENERATOR SETTINGS ---
WEEKS_TO_GENERATE = 15
SAMPLES_PER_WEEK = 25

print(f"🚀 Generating data for the past {WEEKS_TO_GENERATE} weeks...")

current_date = datetime.now()

for i in range(WEEKS_TO_GENERATE):
    # Calculate a date in the past (going back 1 week at a time)
    week_date = current_date - timedelta(weeks=i)
    
    # Create a filename that looks like an email attachment
    # e.g., "Lab_Results_2025-11-20.csv"
    date_str = week_date.strftime('%Y-%m-%d')
    filename = f"Lab_Results_{date_str}.csv"
    
    # Generate Mock Data
    data = {
        # ID format: TEST-WeekNum-SampleNum
        'sample_id': [f"TEST-{week_date.isocalendar()[1]}-{x}" for x in range(SAMPLES_PER_WEEK)],
        'test_date': [date_str for _ in range(SAMPLES_PER_WEEK)],
        # Sprinkle in some "POS" and maybe a typo ("Positive") to test validation
        'result': [random.choice(['POS', 'NEG', 'NEG', 'N/A', 'Positive']) for _ in range(SAMPLES_PER_WEEK)],
        'viral_load': [random.randint(0, 5000) for _ in range(SAMPLES_PER_WEEK)]
    }
    
    # Create Polars DataFrame
    df = pl.DataFrame(data)
    
    # Upload to Azure
    print(f"   📤 Uploading {filename} to landing-zone ({len(df)} rows)...")
    landing_client.upload_blob(name=filename, data=df.write_csv(), overwrite=True)

print("\n✅ Success! 5 weeks of data are now waiting in the Landing Zone.")
print("   👉 Run the pipeline to process them.")