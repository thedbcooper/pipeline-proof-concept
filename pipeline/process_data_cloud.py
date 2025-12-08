import os
import io
import polars as pl
from datetime import datetime
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

print(f"🔌 Connecting to {ACCOUNT_NAME}...")
credential = DefaultAzureCredential()
blob_service = BlobServiceClient(ACCOUNT_URL, credential=credential)

landing_client = blob_service.get_container_client("landing-zone")
quarantine_client = blob_service.get_container_client("quarantine")
data_client = blob_service.get_container_client("data")
logs_client = blob_service.get_container_client("logs")

def process_pipeline():
    # Initialize execution log
    execution_start = datetime.now()
    log_entry = {
        'execution_timestamp': execution_start.isoformat(),
        'files_processed': 0,
        'rows_quarantined': 0,
        'rows_inserted': 0,
        'rows_updated': 0,
        'rows_deleted': 0
    }
    
    # 1. LIST FILES
    blobs = list(landing_client.list_blobs())
    
    if not blobs:
        print("📭 No new files in landing-zone.")
        # Log even empty runs
        _save_execution_log(log_entry)
        return

    print(f"found {len(blobs)} files to process...")
    log_entry['files_processed'] = len(blobs)

    all_valid_rows = []
    all_error_rows = []

    for blob in blobs:
        print(f"Downloading {blob.name}...")
        
        blob_client = landing_client.get_blob_client(blob.name)
        downloaded_bytes = blob_client.download_blob().readall()
        
        # Read CSV (Safely as Strings)
        try:
            df = pl.read_csv(io.BytesIO(downloaded_bytes), infer_schema_length=0)
        except Exception as e:
            print(f"Failed to read CSV {blob.name}: {e}")
            continue

        # VALIDATE LOOP
        for row in df.iter_rows(named=True):
            # Check if this is a tombstone record (skip full validation)
            if row.get('sample_status') == 'remove':
                # For tombstone records, need sample_id, sample_status, and test_date
                if 'sample_id' in row and 'test_date' in row:
                    # Parse the test_date from string to date object
                    try:
                        if isinstance(row['test_date'], str):
                            parsed_date = datetime.strptime(row['test_date'], '%Y-%m-%d').date()
                        else:
                            parsed_date = row['test_date']
                        
                        tombstone_record = {
                            'sample_id': row['sample_id'],
                            'sample_status': 'remove',
                            'test_date': parsed_date,
                            # Add dummy values for other required fields
                            'result': 'N/A',
                            'viral_load': 0
                        }
                        all_valid_rows.append(tombstone_record)
                    except (ValueError, KeyError) as e:
                        bad_row = row
                        bad_row['pipeline_error'] = f"Tombstone record has invalid test_date: {e}"
                        bad_row['source_file'] = blob.name
                        all_error_rows.append(bad_row)
                else:
                    bad_row = row
                    bad_row['pipeline_error'] = "Tombstone record missing required fields: 'sample_id' and 'test_date'"
                    bad_row['source_file'] = blob.name
                    all_error_rows.append(bad_row)
                continue
            
            # Normal validation for non-tombstone records
            try:
                valid_sample = LabResult(**row)
                all_valid_rows.append(valid_sample.model_dump())
            except ValidationError as e:
                bad_row = row
                bad_row['pipeline_error'] = str(e)
                bad_row['source_file'] = blob.name
                all_error_rows.append(bad_row)
        
        print(f"Deleting {blob.name} from landing-zone...")
        blob_client.delete_blob()

    # --- 2. HANDLE BAD DATA ---
    if all_error_rows:
        error_df = pl.DataFrame(all_error_rows)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"quarantine_{timestamp}.csv"
        
        log_entry['rows_quarantined'] = len(all_error_rows)
        print(f"⚠️ Uploading errors to {filename}...")
        quarantine_client.upload_blob(filename, error_df.write_csv(), overwrite=True)

    if not all_valid_rows:
        print("No valid data to upsert.")
        _save_execution_log(log_entry)
        return

    # --- 3. HANDLE GOOD DATA (Upsert to Parquet) ---
    full_df = pl.DataFrame(all_valid_rows)

    # Create Partition Path
    full_df = full_df.with_columns(
        partition_path = pl.format("year={}/week={}", 
                                   pl.col("test_date").dt.year(), 
                                   pl.col("test_date").dt.week())
    )

    unique_partitions = full_df["partition_path"].unique().to_list()

    for part_path in unique_partitions:
        print(f"Processing partition: {part_path}")
        
        new_batch_df = full_df.filter(pl.col("partition_path") == part_path)
        new_batch_df = new_batch_df.drop("partition_path")
        
        # Change extension to .parquet
        blob_name = f"{part_path}/data.parquet"
        blob_client = data_client.get_blob_client(blob_name)
        
        if blob_client.exists():
            print(f"   Downloading history (Parquet)...")
            download_stream = blob_client.download_blob()
            
            # Fix 1: Typo 'download_stream' vs 'downloaded_stream'
            history_df = pl.read_parquet(io.BytesIO(download_stream.readall()))
            
            # Align column order - use new_batch_df column order as the standard
            history_df = history_df.select(new_batch_df.columns)
            
            print("   Merging...")
            combined_df = pl.concat([history_df, new_batch_df])
            final_df = combined_df.unique(subset=["sample_id"], keep="last")
            
            # Track changes: Find new vs updated records
            existing_ids = set(history_df["sample_id"].to_list())
            new_batch_ids = set(new_batch_df["sample_id"].to_list())
            new_inserts = len(new_batch_ids - existing_ids)
            updates = len(new_batch_ids & existing_ids)
            log_entry['rows_inserted'] += new_inserts
            log_entry['rows_updated'] += updates
            
            # Handle tombstone deletions: Remove rows with sample_status='remove'
            if 'sample_status' in final_df.columns:
                rows_before = len(final_df)
                final_df = final_df.filter(pl.col("sample_status") != "remove")
                rows_removed = rows_before - len(final_df)
                if rows_removed > 0:
                    log_entry['rows_deleted'] += rows_removed
                    print(f"   Removed {rows_removed} tombstoned row(s)")
            
            print(f"   Uploading Parquet ({len(final_df)} rows)...")
            
            # Fix 2: Write to buffer, then upload bytes
            output_stream = io.BytesIO()
            final_df.write_parquet(output_stream)
            blob_client.upload_blob(output_stream.getvalue(), overwrite=True)
            
        else:
            print(f"   Creating new Parquet file {blob_name}...")
            
            # Filter out tombstones even for new files
            final_batch = new_batch_df.filter(pl.col("sample_status") != "remove") if 'sample_status' in new_batch_df.columns else new_batch_df
            
            # All rows in a new file are inserts
            log_entry['rows_inserted'] += len(final_batch)
            
            # Fix 2: Write to buffer, then upload bytes
            output_stream = io.BytesIO()
            final_batch.write_parquet(output_stream)
            blob_client.upload_blob(output_stream.getvalue(), overwrite=True)

    print("✅ Cloud Pipeline Complete!")
    
    # Save execution log
    _save_execution_log(log_entry)

def _save_execution_log(log_entry):
    """Save execution log to logs container as CSV."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"execution_{timestamp}.csv"
        
        # Convert log to DataFrame and CSV
        log_df = pl.DataFrame([log_entry])
        log_csv = log_df.write_csv()
        
        # Upload to logs container
        logs_client.upload_blob(log_filename, log_csv, overwrite=True)
        print(f"📊 Execution log saved: {log_filename}")
    except Exception as e:
        print(f"⚠️ Failed to save execution log: {e}")

if __name__ == "__main__":
    process_pipeline()