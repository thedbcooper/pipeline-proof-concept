import polars as pl
import os
import shutil
from datetime import date
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# --- MODULE IMPORTS ---
from models import LabResult
from pydantic import ValidationError

# --- CONFIGURATION ---
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

LOCAL_FIX_DIR = "fix_me_please"
LOCAL_DONE_DIR = "fix_me_please/completed"
os.makedirs(LOCAL_DONE_DIR, exist_ok=True)

def process_reingest():
    print("🕵️‍♀️ Scanning local folder for fixed files...")
    
    # Connect to Azure
    credential = DefaultAzureCredential()
    blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)
    landing_client = blob_service.get_container_client("landing-zone")
    quarantine_client = blob_service.get_container_client("quarantine")

    files = [f for f in os.listdir(LOCAL_FIX_DIR) if f.endswith(".csv")]
    
    if not files:
        print("No CSV files found in 'fix_me_please'.")
        return

    for filename in files:
        filepath = os.path.join(LOCAL_FIX_DIR, filename)
        print(f"\nChecking {filename}...")
        
        try:
            # 1. Read and Clean (Polars)
            # We enforce string types to preserve leading zeros
            df = pl.read_csv(filepath, infer_schema_length=0)
            
            # Remove the old error columns if they exist
            # strict=False prevents crash if column doesn't exist
            df = df.drop(["pipeline_error", "source_file"], strict=False)

            # 2. Validate Rows
            validation_passed = True
            for row in df.iter_rows(named=True):
                try:
                    LabResult(**row)
                except ValidationError as e:
                    print(f"   ❌ Validation FAILED: {e}")
                    validation_passed = False
                    # We stop checking this file after the first error to save time
                    break 
            
            # 3. Decision Time
            if validation_passed:
                print("   ✅ Validation Passed!")
                
                # Upload to Landing Zone
                print("   🚀 Uploading to landing-zone...")
                landing_client.upload_blob(filename, df.write_csv(), overwrite=True)
                
                # Delete from Quarantine (Cloud)
                print("   🗑️  Deleting from Azure quarantine...")
                quarantine_blob = quarantine_client.get_blob_client(filename)
                if quarantine_blob.exists():
                    quarantine_blob.delete_blob()
                else:
                    print("      (Note: File wasn't found in Azure quarantine, skipping delete)")

                # Move local file to 'completed' folder
                shutil.move(filepath, os.path.join(LOCAL_DONE_DIR, filename))
                print("   ✨ Done.")
                
            else:
                print("   🛑 File rejected. Please fix the errors in Excel and try again.")

        except Exception as e:
            print(f"   ⚠️  Script Error processing file: {e}")

if __name__ == "__main__":
    process_reingest()