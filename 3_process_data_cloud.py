import os
import io
import duckdb
import pandas as pd
from pydantic import BaseModel, field_validator, ValidationError
from datetime import date, datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv() # Loads the .env file with your Robot credentials

ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# Connect to the Cloud
print(f"🔌 Connecting to {ACCOUNT_NAME}...")
credential = DefaultAzureCredential()
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)

# Get clients for our specific "Drawers"
landing_client = blob_service.get_container_client("landing-zone")
quarantine_client = blob_service.get_container_client("quarantine")
data_client = blob_service.get_container_client("data")

# --- PYDANTIC MODEL (Same as before) ---
class LabResult(BaseModel):
    sample_id: str
    test_date: date
    result: str
    viral_load: int

    @field_validator('result')
    def check_result_code(cls, v):
        allowed = ['POS', 'NEG', 'N/A']
        if v not in allowed:
            raise ValueError(f"Invalid result code: '{v}'. Must be POS, NEG, or N/A")
        return v

# --- HELPER: GET WEEK ---
def get_partition_path(date_obj):
    y = date_obj.year
    w = date_obj.isocalendar()[1]
    return f"year={y}/week={w}"

def process_pipeline():
    # 1. LIST FILES (Cloud Version)
    # We ask Azure: "What blobs are in the landing-zone?"
    blobs = list(landing_client.list_blobs())
    
    if not blobs:
        print("📭 No new files in landing-zone.")
        return

    print(f"found {len(blobs)} files to process...")

    all_valid_rows = []
    all_error_rows = []

    for blob in blobs:
        print(f"Downloading {blob.name}...")
        
        # DOWNLOAD the content into memory (no need to save to disk yet)
        blob_client = landing_client.get_blob_client(blob.name)
        downloaded_bytes = blob_client.download_blob().readall()
        
        # Read into Pandas using io.BytesIO (tricks Pandas into thinking it's a file)
        # dtype=str preserves leading zeros
        df = pd.read_csv(io.BytesIO(downloaded_bytes), dtype=str)

        # VALIDATE LOOP
        for index, row in df.iterrows():
            try:
                valid_sample = LabResult(**row.to_dict())
                all_valid_rows.append(valid_sample.model_dump())
            except ValidationError as e:
                bad_row = row.to_dict()
                bad_row['pipeline_error'] = str(e)
                bad_row['source_file'] = blob.name
                all_error_rows.append(bad_row)
        
        # DELETE processed file from landing-zone
        print(f"Deleting {blob.name} from landing-zone...")
        blob_client.delete_blob()

    # --- 2. HANDLE BAD DATA (Upload to Quarantine) ---
    if all_error_rows:
        error_df = pd.DataFrame(all_error_rows)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"quarantine_{timestamp}.csv"
        
        # Convert DF to CSV string in memory
        csv_buffer = error_df.to_csv(index=False)
        
        # UPLOAD to Azure
        print(f"⚠️ Uploading errors to {filename}...")
        quarantine_client.upload_blob(filename, csv_buffer, overwrite=True)

    if not all_valid_rows:
        print("No valid data to upsert.")
        return

    # --- 3. HANDLE GOOD DATA (Upsert) ---
    full_df = pd.DataFrame(all_valid_rows)
    full_df['partition_path'] = full_df['test_date'].apply(get_partition_path)

    con = duckdb.connect()
    unique_partitions = full_df['partition_path'].unique()

    for part_path in unique_partitions:
        print(f"Processing partition: {part_path}")
        
        # Filter new data for this week
        new_batch_df = full_df[full_df['partition_path'] == part_path].copy()
        
        # Define the cloud filename
        blob_name = f"{part_path}/data.csv"
        blob_client = data_client.get_blob_client(blob_name)
        
        # CHECK if history exists in Cloud
        if blob_client.exists():
            print(f"   Downloading history from Azure...")
            
            # Download to a temp file on disk so DuckDB can read it safely
            with open("temp_history.csv", "wb") as my_blob:
                download_stream = blob_client.download_blob()
                my_blob.write(download_stream.readall())
            
            # DuckDB Magic (Same as before)
            history_df = con.query(f"SELECT * FROM read_csv_auto('temp_history.csv', all_varchar=True)").to_df()
            
            con.register('history_table', history_df)
            con.register('new_batch_table', new_batch_df)
            
            merged_df = con.query("""
                SELECT sample_id, CAST(test_date AS DATE) as test_date, result, CAST(viral_load AS INTEGER) as viral_load 
                FROM history_table 
                WHERE sample_id NOT IN (SELECT sample_id FROM new_batch_table)
                UNION ALL
                SELECT sample_id, test_date, result, viral_load FROM new_batch_table
            """).to_df()
            
            # Convert result to CSV string
            output_csv = merged_df.to_csv(index=False)
            
            # UPLOAD back to Azure
            print(f"   Uploading merged data back to {blob_name}...")
            blob_client.upload_blob(output_csv, overwrite=True)
            
            # Clean up temp file
            os.remove("temp_history.csv")
            
        else:
            print(f"   No history found. Creating new file {blob_name}...")
            new_batch_df = new_batch_df.drop(columns=['partition_path'])
            output_csv = new_batch_df.to_csv(index=False)
            blob_client.upload_blob(output_csv, overwrite=True)

    print("✅ Cloud Pipeline Complete!")

if __name__ == "__main__":
    process_pipeline()