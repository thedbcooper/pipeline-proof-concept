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
        'rows_updated': 0
    }
    
    # Track detailed processing log
    processing_log = []
    
    # 1. LIST FILES
    blobs = list(landing_client.list_blobs())
    
    if not blobs:
        print("📭 No new files in landing-zone.")
        processing_log.append("📭 No new files to process")
        # Log even empty runs
        _save_execution_log(log_entry, processing_log)
        return

    print(f"found {len(blobs)} files to process...")
    processing_log.append(f"📦 Found {len(blobs)} file(s) in landing zone")
    log_entry['files_processed'] = len(blobs)

    all_valid_rows = []
    all_error_rows = []

    for blob in blobs:
        print(f"📥 Downloading {blob.name}...")
        
        blob_client = landing_client.get_blob_client(blob.name)
        downloaded_bytes = blob_client.download_blob().readall()
        
        # Read CSV (Safely as Strings)
        try:
            df = pl.read_csv(io.BytesIO(downloaded_bytes), infer_schema_length=0)
        except Exception as e:
            print(f"❌ Failed to read CSV {blob.name}: {e}")
            continue

        # VALIDATE LOOP
        valid_count = 0
        error_count = 0
        for row in df.iter_rows(named=True):
            try:
                valid_sample = LabResult(**row)
                all_valid_rows.append(valid_sample.model_dump())
                valid_count += 1
            except ValidationError as e:
                bad_row = row
                bad_row['pipeline_error'] = str(e)
                bad_row['source_file'] = blob.name
                all_error_rows.append(bad_row)
                error_count += 1
        
        print(f"✅ Processed {blob.name}: {valid_count} valid, {error_count} errors")
        processing_log.append(f"✅ Processed {blob.name}: {valid_count} valid, {error_count} errors")
        print(f"🗑️ Deleting {blob.name} from landing-zone...")
        blob_client.delete_blob()

    # --- 2. HANDLE BAD DATA ---
    if all_error_rows:
        error_df = pl.DataFrame(all_error_rows)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"quarantine_{timestamp}.csv"
        
        log_entry['rows_quarantined'] = len(all_error_rows)
        print(f"⚠️ Uploading errors to {filename}...")
        processing_log.append(f"⚠️ Quarantined {len(all_error_rows)} row(s) to {filename}")
        quarantine_client.upload_blob(filename, error_df.write_csv(), overwrite=True)

    if not all_valid_rows:
        print("No valid data to upsert.")
        processing_log.append("ℹ️ No valid data to process")
        _save_execution_log(log_entry, processing_log)
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
    print(f"\n📊 Processing {len(all_valid_rows)} valid records across {len(unique_partitions)} partition(s)...")
    processing_log.append(f"📊 Processing {len(all_valid_rows)} valid record(s) across {len(unique_partitions)} partition(s)")

    for part_path in unique_partitions:
        print(f"\n📁 Processing partition: {part_path}")
        
        new_batch_df = full_df.filter(pl.col("partition_path") == part_path)
        new_batch_df = new_batch_df.drop("partition_path")
        print(f"   📦 New batch contains {len(new_batch_df)} record(s)")
        
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
            
            processing_log.append(f"   📁 {part_path}: ➕ {new_inserts} inserted, 🔄 {updates} updated")
            print(f"   ➕ New records: {new_inserts}, 🔄 Updated: {updates}")
            print(f"   💾 Uploading Parquet ({len(final_df)} total rows)...")
            
            # Fix 2: Write to buffer, then upload bytes
            output_stream = io.BytesIO()
            final_df.write_parquet(output_stream)
            blob_client.upload_blob(output_stream.getvalue(), overwrite=True)
            
        else:
            print(f"   Creating new Parquet file {blob_name}...")
            
            # All rows in a new file are inserts
            log_entry['rows_inserted'] += len(new_batch_df)
            
            processing_log.append(f"   📁 {part_path}: ➕ {len(new_batch_df)} inserted (new partition)")
            
            # Write to buffer, then upload bytes
            output_stream = io.BytesIO()
            new_batch_df.write_parquet(output_stream)
            blob_client.upload_blob(output_stream.getvalue(), overwrite=True)

    print("\n" + "="*60)
    print("✅ PIPELINE COMPLETE!")
    print(f"📊 SUMMARY:")
    print(f"   Files Processed: {log_entry['files_processed']}")
    print(f"   Rows Quarantined: {log_entry['rows_quarantined']}")
    print(f"   Rows Inserted: {log_entry['rows_inserted']}")
    print(f"   Rows Updated: {log_entry['rows_updated']}")
    print("="*60 + "\n")
    
    # Save execution log
    _save_execution_log(log_entry, processing_log)

def _save_execution_log(log_entry, processing_log=None):
    """Save execution log to logs container as CSV."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"execution_{timestamp}.csv"
        
        # Add processing log as a single field (pipe-separated for multiple entries)
        log_entry_with_details = log_entry.copy()
        if processing_log:
            log_entry_with_details['processing_details'] = ' | '.join(processing_log)
        else:
            log_entry_with_details['processing_details'] = ''
        
        # Convert log to DataFrame and CSV
        log_df = pl.DataFrame([log_entry_with_details])
        log_csv = log_df.write_csv()
        
        # Upload to logs container
        logs_client.upload_blob(log_filename, log_csv, overwrite=True)
        print(f"📊 Execution log saved: {log_filename}")
    except Exception as e:
        print(f"⚠️ Failed to save execution log: {e}")

if __name__ == "__main__":
    process_pipeline()