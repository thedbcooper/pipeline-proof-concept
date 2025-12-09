"""
Delete records from partitioned data storage.
Reads a CSV with sample_id and test_date, finds matching records across partitions,
removes them, and re-uploads the updated parquet files.
"""
import os
import io
import polars as pl
from datetime import datetime
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

deletion_client = blob_service.get_container_client("deletion-requests")
data_client = blob_service.get_container_client("data")
logs_client = blob_service.get_container_client("logs")

def process_deletions():
    """Process deletion requests from deletion-requests container."""
    
    # Initialize deletion log
    execution_start = datetime.now()
    log_entry = {
        'execution_timestamp': execution_start.isoformat(),
        'files_processed': 0,
        'rows_deleted': 0,
        'partitions_updated': 0
    }
    
    # 1. LIST DELETION REQUEST FILES
    deletion_blobs = list(deletion_client.list_blobs())
    
    if not deletion_blobs:
        print("📭 No deletion requests found.")
        _save_deletion_log(log_entry)
        return
    
    print(f"Found {len(deletion_blobs)} deletion request file(s)...")
    log_entry['files_processed'] = len(deletion_blobs)
    
    # Collect all deletion requests
    all_deletions = []
    
    for blob in deletion_blobs:
        print(f"Reading {blob.name}...")
        
        blob_client = deletion_client.get_blob_client(blob.name)
        downloaded_bytes = blob_client.download_blob().readall()
        
        try:
            # Read deletion CSV (must have sample_id and test_date columns)
            df = pl.read_csv(io.BytesIO(downloaded_bytes))
            
            # Validate required columns
            if 'sample_id' not in df.columns or 'test_date' not in df.columns:
                print(f"⚠️ Skipping {blob.name}: Missing required columns (sample_id, test_date)")
                continue
            
            # Select only needed columns and parse test_date
            deletion_df = df.select(['sample_id', 'test_date'])
            deletion_df = deletion_df.with_columns(
                pl.col('test_date').str.strptime(pl.Date, "%Y-%m-%d")
            )
            
            all_deletions.append(deletion_df)
            
            # Delete processed request file
            print(f"Deleting {blob.name} from deletion-requests...")
            blob_client.delete_blob()
            
        except Exception as e:
            print(f"❌ Error processing {blob.name}: {e}")
            continue
    
    if not all_deletions:
        print("No valid deletion requests to process.")
        _save_deletion_log(log_entry)
        return
    
    # Combine all deletion requests
    deletions_df = pl.concat(all_deletions)
    print(f"Total deletion requests: {len(deletions_df)}")
    
    # 2. CALCULATE PARTITIONS TO CHECK
    # Add partition path to deletion requests
    deletions_df = deletions_df.with_columns(
        partition_path = pl.format("year={}/week={}", 
                                   pl.col("test_date").dt.year(), 
                                   pl.col("test_date").dt.week())
    )
    
    unique_partitions = deletions_df["partition_path"].unique().to_list()
    print(f"Checking {len(unique_partitions)} partition(s)...")
    
    # 3. PROCESS EACH PARTITION
    total_deleted = 0
    partitions_updated = 0
    
    for part_path in unique_partitions:
        print(f"\nProcessing partition: {part_path}")
        
        # Get deletion IDs for this partition
        partition_deletions = deletions_df.filter(pl.col("partition_path") == part_path)
        ids_to_delete = set(partition_deletions["sample_id"].to_list())
        
        # Check if partition file exists
        blob_name = f"{part_path}/data.parquet"
        blob_client = data_client.get_blob_client(blob_name)
        
        if not blob_client.exists():
            print(f"  ⚠️ Partition file not found: {blob_name}")
            continue
        
        # Download existing data
        print(f"  Downloading {blob_name}...")
        download_stream = blob_client.download_blob()
        history_df = pl.read_parquet(io.BytesIO(download_stream.readall()))
        
        rows_before = len(history_df)
        
        # Filter out records to delete
        filtered_df = history_df.filter(~pl.col("sample_id").is_in(ids_to_delete))
        
        rows_after = len(filtered_df)
        rows_deleted = rows_before - rows_after
        
        if rows_deleted > 0:
            print(f"  🗑️ Removing {rows_deleted} record(s)...")
            total_deleted += rows_deleted
            partitions_updated += 1
            
            # Upload updated parquet
            output_stream = io.BytesIO()
            filtered_df.write_parquet(output_stream)
            blob_client.upload_blob(output_stream.getvalue(), overwrite=True)
            print(f"  ✅ Updated {blob_name} ({rows_after} rows remaining)")
        else:
            print(f"  ℹ️ No matching records found in this partition")
    
    log_entry['rows_deleted'] = total_deleted
    log_entry['partitions_updated'] = partitions_updated
    
    print(f"\n✅ Deletion Complete!")
    print(f"   Total rows deleted: {total_deleted}")
    print(f"   Partitions updated: {partitions_updated}")
    
    # Save deletion log
    _save_deletion_log(log_entry)

def _save_deletion_log(log_entry):
    """Save deletion log to logs container as CSV."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"deletion_{timestamp}.csv"
        
        # Convert log to DataFrame and CSV
        log_df = pl.DataFrame([log_entry])
        log_csv = log_df.write_csv()
        
        # Upload to logs container
        logs_client.upload_blob(log_filename, log_csv, overwrite=True)
        print(f"📊 Deletion log saved: {log_filename}")
    except Exception as e:
        print(f"⚠️ Failed to save deletion log: {e}")

if __name__ == "__main__":
    process_deletions()
