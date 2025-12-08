import os
import io
import pandas as pd
from datetime import date, datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from models import LabResult
from pydantic import ValidationError

# --- CONFIGURATION ---
load_dotenv() # Loads .env or GitHub Secrets

# Get Account info
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# Connect to the Cloud
print(f"🔌 Connecting to {ACCOUNT_NAME}...")
credential = DefaultAzureCredential()
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)

# Get clients for our specific containers
landing_client = blob_service.get_container_client("landing-zone")
quarantine_client = blob_service.get_container_client("quarantine")
data_client = blob_service.get_container_client("data")

# --- HELPER: GET WEEK ---
def get_partition_path(date_obj):
    y = date_obj.year
    w = date_obj.isocalendar()[1]
    return f"year={y}/week={w}"

def process_pipeline():
    # 1. LIST FILES
    blobs = list(landing_client.list_blobs())
    
    if not blobs:
        print("📭 No new files in landing-zone.")
        return

    print(f"found {len(blobs)} files to process...")

    all_valid_rows = []
    all_error_rows = []

    for blob in blobs:
        print(f"Downloading {blob.name}...")
        
        # DOWNLOAD content into memory
        blob_client = landing_client.get_blob_client(blob.name)
        downloaded_bytes = blob_client.download_blob().readall()
        
        # Read into Pandas (Force dtype=str to protect leading zeros)
        df = pd.read_csv(io.BytesIO(downloaded_bytes), dtype=str)

        # VALIDATE LOOP
        for index, row in df.iterrows():
            try:
                # Pydantic validation
                valid_sample = LabResult(**row.to_dict())
                all_valid_rows.append(valid_sample.model_dump())
            except ValidationError as e:
                bad_row = row.to_dict()
                bad_row['pipeline_error'] = str(e)
                bad_row['source_file'] = blob.name
                all_error_rows.append(bad_row)
        
        # DELETE processed file
        print(f"Deleting {blob.name} from landing-zone...")
        blob_client.delete_blob()

    # --- 2. HANDLE BAD DATA (Upload to Quarantine) ---
    if all_error_rows:
        error_df = pd.DataFrame(all_error_rows)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"quarantine_{timestamp}.csv"
        
        csv_buffer = error_df.to_csv(index=False)
        
        print(f"⚠️ Uploading errors to {filename}...")
        quarantine_client.upload_blob(filename, csv_buffer, overwrite=True)

    if not all_valid_rows:
        print("No valid data to upsert.")
        return

    # --- 3. HANDLE GOOD DATA (Upsert with Pandas) ---
    full_df = pd.DataFrame(all_valid_rows)
    # Convert test_date to actual datetime objects for the helper function
    full_df['test_date_obj'] = pd.to_datetime(full_df['test_date'])
    full_df['partition_path'] = full_df['test_date_obj'].apply(get_partition_path)

    unique_partitions = full_df['partition_path'].unique()

    for part_path in unique_partitions:
        print(f"Processing partition: {part_path}")
        
        # Filter new data for this week
        new_batch_df = full_df[full_df['partition_path'] == part_path].copy()
        # Drop the helper columns before saving
        new_batch_df = new_batch_df.drop(columns=['partition_path', 'test_date_obj'])
        
        # Define cloud filename
        blob_name = f"{part_path}/data.csv"
        blob_client = data_client.get_blob_client(blob_name)
        
        # CHECK if history exists
        if blob_client.exists():
            print(f"   Downloading history from Azure...")
            download_stream = blob_client.download_blob()
            
            # Read history (Protect types!)
            history_df = pd.read_csv(io.BytesIO(download_stream.readall()), dtype=str)
            
            # --- THE UPSERT LOGIC ---
            print("   Merging and Upserting...")
            
            # 1. Stack: History on top, New Batch on bottom
            combined_df = pd.concat([history_df, new_batch_df], ignore_index=True)
            
            # 2. Deduplicate: Based on 'sample_id'
            # keep='last' ensures the row from 'new_batch_df' (bottom) wins
            final_df = combined_df.drop_duplicates(subset=['sample_id'], keep='last')
            
            # Upload back
            output_csv = final_df.to_csv(index=False)
            print(f"   Uploading merged data ({len(final_df)} rows)...")
            blob_client.upload_blob(output_csv, overwrite=True)
            
        else:
            print(f"   No history found. Creating new file {blob_name}...")
            output_csv = new_batch_df.to_csv(index=False)
            blob_client.upload_blob(output_csv, overwrite=True)

    print("✅ Cloud Pipeline Complete!")

if __name__ == "__main__":
    process_pipeline()