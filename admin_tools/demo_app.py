import streamlit as st
import io
import pandas as pd
import time

# --- MOCK IMPORTS ONLY ---
try:
    from mock_azure import MockContainerClient, reset_mock_cloud
except ImportError:
    st.error("Missing 'mock_azure.py'. This file is required for the demo.")
    st.stop()

# Import Pydantic model
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from models import LabResult
    from pydantic import ValidationError
except ImportError:
    st.error("Missing 'models.py'. This file is required for validation.")
    st.stop()

# --- CONFIGURATION ---
st.set_page_config(page_title="Lab Data Admin (Portfolio Demo)", layout="wide")

# ==========================================
# 🧠 SESSION STATE INIT
# ==========================================
if "staged_fixes" not in st.session_state:
    st.session_state.staged_fixes = []
if "upload_counter" not in st.session_state:
    st.session_state.upload_counter = 0
if "upload_success" not in st.session_state:
    st.session_state.upload_success = False

# ==========================================
# ☁️ MOCK CLIENT INITIALIZATION
# ==========================================
@st.cache_resource
def get_mock_clients():
    return (
        MockContainerClient("landing-zone"),
        MockContainerClient("quarantine"),
        MockContainerClient("data"),
        MockContainerClient("logs")
    )

landing_client, quarantine_client, data_client, logs_client = get_mock_clients()

# ==========================================
# 🤖 MINI-PIPELINE
# ==========================================
def run_mock_pipeline():
    from datetime import datetime
    
    # Initialize execution log
    execution_start = datetime.now()
    metrics = {
        'execution_timestamp': execution_start.isoformat(),
        'files_processed': 0,
        'rows_quarantined': 0,
        'rows_inserted': 0,
        'rows_updated': 0,
        'rows_deleted': 0
    }
    
    log = []
    blobs = landing_client.list_blobs()
    if not blobs:
        # Save log even for empty runs
        _save_mock_execution_log(metrics)
        return "📭 No new files in Landing Zone."
    
    metrics['files_processed'] = len(blobs)
    log.append(f"Found {len(blobs)} new files to process.")

    report_blob = data_client.get_blob_client("final_cdc_export.csv")
    if report_blob.exists():
        history_bytes = report_blob.download_blob().readall()
        history_df = pd.read_csv(io.BytesIO(history_bytes), dtype=str)
        existing_ids = set(history_df['sample_id'].tolist()) if 'sample_id' in history_df.columns else set()
    else:
        history_df = pd.DataFrame(columns=["sample_id", "test_date", "result", "viral_load"])
        existing_ids = set()
    
    rows_before = len(history_df)
    log.append(f"History contains {rows_before} rows.")

    all_valid_rows = []
    all_error_rows = []
    
    for blob_prop in blobs:
        b_client = landing_client.get_blob_client(blob_prop.name)
        data = b_client.download_blob().readall()
        try:
            if isinstance(data, str): data = data.encode('utf-8')
            df = pd.read_csv(io.BytesIO(data), dtype=str)
            
            # VALIDATE EACH ROW
            valid_count = 0
            error_count = 0
            
            for index, row in df.iterrows():
                try:
                    # Pydantic validation
                    valid_sample = LabResult(**row.to_dict())
                    all_valid_rows.append(valid_sample.model_dump())
                    valid_count += 1
                except ValidationError as e:
                    bad_row = row.to_dict()
                    bad_row['pipeline_error'] = str(e)
                    bad_row['source_file'] = blob_prop.name
                    all_error_rows.append(bad_row)
                    error_count += 1
            
            # Delete processed file
            b_client.delete_blob()
            log.append(f"✅ Processed & Deleted: {blob_prop.name} ({valid_count} valid, {error_count} errors)")
            
        except Exception as e:
            log.append(f"❌ CRITICAL ERROR reading {blob_prop.name}: {e}")

    # Handle bad data - upload to quarantine
    if all_error_rows:
        from datetime import datetime
        error_df = pd.DataFrame(all_error_rows)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"quarantine_{timestamp}.csv"
        
        metrics['rows_quarantined'] = len(all_error_rows)
        csv_buffer = error_df.to_csv(index=False).encode('utf-8')
        quarantine_client.upload_blob(filename, csv_buffer, overwrite=True)
        log.append(f"⚠️ Quarantined {len(all_error_rows)} rows to {filename}")

    # Handle good data - upsert into report
    if all_valid_rows:
        new_batch = pd.DataFrame(all_valid_rows)
        
        # Track inserts vs updates
        new_batch_ids = set(new_batch['sample_id'].tolist()) if 'sample_id' in new_batch.columns else set()
        inserts = len(new_batch_ids - existing_ids)
        updates = len(new_batch_ids & existing_ids)
        metrics['rows_inserted'] = inserts
        metrics['rows_updated'] = updates
        
        # Convert test_date to string for consistency before merging
        if "test_date" in new_batch.columns:
            new_batch["test_date"] = new_batch["test_date"].astype(str)
        
        full_df = pd.concat([history_df, new_batch])
        full_df = full_df.drop_duplicates(subset=["sample_id"], keep="last")
        
        if "test_date" in full_df.columns:
            full_df = full_df.sort_values("test_date", ascending=False)

        csv_out = full_df.to_csv(index=False).encode('utf-8')
        report_blob.upload_blob(csv_out, overwrite=True)
        
        rows_after = len(full_df)
        log.append(f"📊 Rows Before: {rows_before} -> Rows After: {rows_after}")
        log.append(f"✅ Report successfully updated!")
    elif not all_error_rows:
        log.append("⚠️ No valid data to process.")
    
    # Save execution log
    _save_mock_execution_log(metrics)
    
    # Return log string with metrics appended
    return "\n".join(log) + f"\n\nMETRICS|{metrics['files_processed']}|{metrics['rows_quarantined']}|{metrics['rows_inserted']}|{metrics['rows_updated']}|{metrics['rows_deleted']}"

def _save_mock_execution_log(metrics):
    """Save execution log to logs container."""
    from datetime import datetime
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"execution_{timestamp}.csv"
        
        # Convert to DataFrame and CSV
        log_df = pd.DataFrame([metrics])
        log_csv = log_df.to_csv(index=False).encode('utf-8')
        
        # Upload to logs container
        logs_client.upload_blob(log_filename, log_csv, overwrite=True)
        print(f"✅ Log saved: {log_filename}")  # Debug output
    except Exception as e:
        # Show error in demo for debugging
        print(f"⚠️ Failed to save execution log: {e}")
        import traceback
        traceback.print_exc()

def run_mock_deletions(pending_deletions):
    """Process deletion requests and remove records from mock data."""
    total_deleted = 0
    partitions_updated = 0
    log = []
    
    # Get the final report blob
    report_blob = data_client.get_blob_client("final_cdc_export.csv")
    
    if not report_blob.exists():
        return 0, 0, ["No data found to delete from"]
    
    # Load existing data
    history_bytes = report_blob.download_blob().readall()
    current_df = pd.read_csv(io.BytesIO(history_bytes), dtype=str)
    original_count = len(current_df)
    
    log.append(f"📊 Current data contains {original_count} records")
    
    # Collect all IDs to delete
    all_ids_to_delete = set()
    
    for item in pending_deletions:
        deletion_df = item['dataframe'].copy()
        ids_from_file = set(deletion_df['sample_id'].tolist())
        all_ids_to_delete.update(ids_from_file)
        log.append(f"  {item['filename']}: {len(ids_from_file)} IDs marked for deletion")
    
    log.append(f"🔍 Total unique IDs to delete: {len(all_ids_to_delete)}")
    
    # Filter out records to delete
    filtered_df = current_df[~current_df['sample_id'].isin(all_ids_to_delete)]
    
    rows_deleted = original_count - len(filtered_df)
    total_deleted = rows_deleted
    
    if rows_deleted > 0:
        # Save updated data back
        csv_out = filtered_df.to_csv(index=False).encode('utf-8')
        report_blob.upload_blob(csv_out, overwrite=True)
        partitions_updated = 1  # Mock: treating the whole CSV as one "partition"
        log.append(f"✅ Removed {rows_deleted} record(s)")
        log.append(f"📊 Remaining records: {len(filtered_df)}")
    else:
        log.append("ℹ️ No matching records found to delete")
    
    return total_deleted, partitions_updated, log

# ==========================================
# SIDEBAR: NAVIGATION & CONTROLS
# ==========================================
with st.sidebar:
    st.header("🧬 Lab Data Admin")
    st.info("ℹ️ **DEMO MODE ACTIVE**\n\nThis app is running in an isolated environment. Changes will not affect any real data.")

    if st.button("🔄 Reset Demo Data"):
        reset_mock_cloud()
        # Seed quarantine with unique ID
        q_data = (
            b"sample_id,test_date,result,viral_load,pipeline_error,source_file\n"
            b"TEST-999,2025-12-05,Positive,1000,\"Value error, Invalid result code...\",demo.csv"
        )
        quarantine_client.get_blob_client("quarantine_demo.csv").upload_blob(q_data, overwrite=True)

        st.session_state.staged_fixes = []
        if "preview_df" in st.session_state: del st.session_state.preview_df
        
        # Force navigation back to Home
        st.session_state["nav_selection"] = "🏠 Start Here"
        st.rerun()
    
    # NAVIGATION
    page = st.radio(
        "Go to:", 
        ["🏠 Start Here", "📤 Upload New Data", "🛠️ Fix Quarantine", "🗑️ Delete Records", "⚙️ Process & Monitor", "📊 Final Report"],
        key="nav_selection"
    )

# ==========================================
# PAGE 0: LANDING PAGE
# ==========================================
if page == "🏠 Start Here":
    st.title("🧬 Lab Data Pipeline: Admin Console")
    st.markdown("""
    **Welcome.** This dashboard allows Public Health Epidemiologists to safely manage the flow of sensitive lab data 
    into the Azure Lakehouse without needing to write code.
    """)
    
    st.divider()
    
    # --- WORKFLOW 1: THE HAPPY PATH ---
    st.subheader("🟢 Workflow A: Standard Ingestion")
    st.caption("How data moves from partners to the dashboard.")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 1. Upload")
        st.markdown("Drag & drop CSV files containing new lab results.")
        st.info("📍 *Tab: 'Upload New Data'*")

    with col2:
        st.markdown("### 2. Review & Trigger")
        st.markdown("Review queued files in the landing zone, then trigger the pipeline to validate and process data.")
        st.warning("""
        **How to run it:**
        * **Production:** Auto-runs weekly (Cron Job).
        * **Admin/Demo:** Go to **⚙️ Process & Monitor** tab and click **▶️ Trigger Weekly Pipeline**.
        """)

    with col3:
        st.markdown("### 3. Master Report")
        st.markdown("Valid data is upserted into the CDC Export.")
        st.success("📍 *Tab: 'Final Report'*")

    st.divider()

    # --- WORKFLOW 2: THE EXCEPTION PATH ---
    st.subheader("🔴 Workflow B: Error Resolution (Start here for Demo)")
    st.caption("What happens when the robot rejects a file.")

    q_col1, q_col2, q_col3, q_col4 = st.columns(4)

    with q_col1:
        st.markdown("### 1. Alert")
        st.markdown("Files with errors (e.g. 'Positive' instead of 'POS') are **Quarantined**.")
        st.error("🚨 *Automatic*")

    with q_col2:
        st.markdown("### 2. Review")
        st.markdown("Admin reviews the quarantined file and identifies errors.")
        st.warning("📍 *Tab: 'Fix Quarantine'*")

    with q_col3:
        st.markdown("### 3. Fix & Stage")
        st.markdown("Admin corrects errors using the Excel-like editor and stages the file for upload.")
        st.info("✍️ *Click 'Stage for Upload'*")
    
    with q_col4:
        st.markdown("### 4. Re-Upload & Process")
        st.markdown("Upload fixed files back to landing zone, then go to **⚙️ Process & Monitor** to trigger ingestion.")
        st.success("📍 *Click 'Upload All Fixed Files', then trigger pipeline*")

    st.divider()
    
    # CALL TO ACTION
    st.success("### 🚀 Ready to begin?")
    st.markdown("**For new data:** Go to **📤 Upload New Data**")
    st.markdown("**For error fixes:** Go to **🛠️ Fix Quarantine** (Start here for Demo)")

# ==========================================
# PAGE 1: UPLOAD NEW DATA
# ==========================================
elif page == "📤 Upload New Data":
    st.title("📤 Upload New Data")
    st.caption("Upload new CSV files to the landing zone for processing")
    
    uploaded_files = st.file_uploader(
        "Drag & Drop CSV Files", 
        type="csv", 
        accept_multiple_files=True,
        key=f"file_uploader_{st.session_state.upload_counter}"
    )
    
    st.divider()
    
    # UPLOAD BUTTON
    if uploaded_files:
        if st.button(f"🚀 Upload {len(uploaded_files)} file(s) to Cloud", type="primary"):
            progress_bar = st.progress(0)
            
            for idx, up_file in enumerate(uploaded_files):
                file_bytes = up_file.getvalue()
                landing_client.upload_blob(name=up_file.name, data=file_bytes, overwrite=True)
                st.write(f"✅ Uploaded `{up_file.name}`")
                progress_bar.progress((idx + 1) / len(uploaded_files))
            
            # Increment counter to clear the uploader on rerun
            st.session_state.upload_counter += 1
            st.session_state.upload_success = True
            st.rerun()
    
    # Show success message after rerun
    if st.session_state.upload_success:
        st.success("✨ Done! All files uploaded to Landing Zone. Be sure to trigger the pipeline from the sidebar.")
        st.session_state.upload_success = False
    
    st.divider()
    
    # PREVIEW SECTION
    if uploaded_files:
        st.subheader("📋 File Preview")
        preview_choice = st.selectbox(
            "Select file to preview:", 
            [f.name for f in uploaded_files]
        )
        
        if preview_choice:
            selected_file = next(f for f in uploaded_files if f.name == preview_choice)
            
            try:
                df_preview = pd.read_csv(selected_file, nrows=10)
                st.caption(f"Showing first 10 rows of **{preview_choice}**")
                st.dataframe(df_preview, width="stretch")
            except Exception as e:
                st.error(f"Error reading file: {e}")
    else:
        st.info("📭 No files selected. Drag and drop CSV files above to get started.")

# ==========================================
# PAGE 2: PROCESS & MONITOR
# ==========================================
elif page == "⚙️ Process & Monitor":
    st.title("⚙️ Process & Monitor")
    st.caption("View queued files, trigger pipeline processing, and review execution history")
    
    # Robot Controls Section
    st.subheader("🤖 Pipeline Controls")
    
    if st.button("▶️ Trigger Weekly Pipeline", use_container_width=True):
        with st.status("🤖 Processing Pipeline...", expanded=True) as status:
            st.write("🔍 Scanning landing zone...")
            time.sleep(0.5)
            result_log = run_mock_pipeline()
            time.sleep(0.3)
            
            if "CRITICAL ERROR" in result_log or "Failed" in result_log:
                status.update(label="❌ Pipeline Failed", state="error", expanded=True)
                st.error("⚠️ **Pipeline encountered errors**")
                with st.expander("📜 View Error Details", expanded=True):
                    st.code(result_log, language="text")
            elif "No new files" in result_log:
                status.update(label="⏸️ Pipeline Idle", state="complete", expanded=True)
                st.info("📭 **No files to process**")
                st.caption(result_log)
            else:
                status.update(label="✅ Pipeline Complete!", state="complete", expanded=True)
                
                # Parse the METRICS from the log (new format)
                if "METRICS|" in result_log:
                    metrics_line = [l for l in result_log.split('\n') if 'METRICS|' in l][0]
                    _, files_processed, rows_quarantined, rows_inserted, rows_updated, rows_deleted = metrics_line.split('|')
                    
                    # Display summary
                    st.success("✨ **Pipeline executed successfully!**")
                    st.balloons()
                    
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("Files Processed", files_processed)
                    with col2:
                        st.metric("Rows Quarantined", rows_quarantined, delta=None if rows_quarantined == '0' else f"-{rows_quarantined}", delta_color="inverse")
                    with col3:
                        st.metric("Rows Inserted", rows_inserted)
                    with col4:
                        st.metric("Rows Updated", rows_updated)
                    with col5:
                        st.metric("⚠️ Rows Deleted", rows_deleted, delta=None if rows_deleted == '0' else f"-{rows_deleted}", delta_color="inverse")
                else:
                    # Fallback to old parsing method
                    st.success("✨ **Pipeline executed successfully!**")
                
                # Clean log for display (remove METRICS line)
                display_log = '\n'.join([line for line in result_log.split('\n') if not line.startswith('METRICS|')])
                with st.expander("📜 View Detailed Log"):
                    st.code(display_log, language="text")
    
    st.divider()
    
    # LANDING ZONE FILE PREVIEW
    st.subheader("📦 Files in Landing Zone")
    st.caption("Files queued for processing")
    
    try:
        blob_list = list(landing_client.list_blobs())
        
        if not blob_list:
            st.info("📭 Landing Zone is empty. Upload files in the 'Upload New Data' tab.")
        else:
            st.success(f"Found {len(blob_list)} file(s) in the landing zone")
            
            # Show file list
            st.subheader("Files in Queue")
            for blob in blob_list:
                st.text(f"📄 {blob.name}")
            
            st.divider()
            
            # File preview
            if blob_list:
                st.subheader("📋 File Preview")
                selected_blob_name = st.selectbox(
                    "Select file to preview:",
                    [blob.name for blob in blob_list]
                )
                
                if selected_blob_name:
                    blob_client = landing_client.get_blob_client(selected_blob_name)
                    try:
                        data = blob_client.download_blob().readall()
                        if isinstance(data, str): data = data.encode('utf-8')
                        df_preview = pd.read_csv(io.BytesIO(data), nrows=10)
                        st.caption(f"Showing first 10 rows of **{selected_blob_name}**")
                        st.dataframe(df_preview, width="stretch")
                        
                        # Delete button with confirmation
                        st.divider()
                        if st.button("🗑️ Delete This File", type="secondary", key="delete_landing"):
                            st.session_state.confirm_delete_landing = selected_blob_name
                        
                        # Confirmation dialog
                        if st.session_state.get("confirm_delete_landing") == selected_blob_name:
                            st.warning(f"⚠️ Are you sure you want to delete `{selected_blob_name}`? This action cannot be undone.")
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.button("✅ Yes, Delete", type="primary", key="confirm_yes_landing"):
                                    try:
                                        blob_client.delete_blob()
                                        st.session_state.confirm_delete_landing = None
                                        st.toast(f"Deleted `{selected_blob_name}` from landing zone")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Failed to delete: {e}")
                            with col2:
                                if st.button("❌ Cancel", key="confirm_no_landing"):
                                    st.session_state.confirm_delete_landing = None
                                    st.rerun()
                    except Exception as e:
                        st.error(f"Error reading file: {e}")
    
    except Exception as e:
        st.error(f"Failed to load landing zone files: {e}")
    
    st.divider()
    
    # EXECUTION LOGS SECTION
    st.subheader("📈 Pipeline Execution History")
    st.caption("Metrics from previous pipeline runs")
    
    try:
        log_blobs = list(logs_client.list_blobs())
        
        if not log_blobs:
            st.info("📭 No execution logs found. Run the pipeline to generate logs.")
        else:
            st.success(f"Found {len(log_blobs)} execution log(s)")
            
            # Load all logs into a single dataframe
            all_logs = []
            for blob in sorted(log_blobs, key=lambda x: x.name, reverse=True):  # Most recent first
                try:
                    blob_client = logs_client.get_blob_client(blob.name)
                    log_data = blob_client.download_blob().readall()
                    if isinstance(log_data, str): log_data = log_data.encode('utf-8')
                    log_df = pd.read_csv(io.BytesIO(log_data))
                    all_logs.append(log_df)
                except Exception as e:
                    st.warning(f"Could not read {blob.name}: {e}")
            
            if all_logs:
                # Combine all logs
                combined_logs = pd.concat(all_logs, ignore_index=True)
                
                # Sort by timestamp (most recent first)
                combined_logs = combined_logs.sort_values('execution_timestamp', ascending=False)
                
                # Display summary metrics from most recent run
                if len(combined_logs) > 0:
                    latest = combined_logs.iloc[0]
                    
                    st.write("**Latest Pipeline Run:**")
                    col1, col2, col3, col4, col5 = st.columns(5)
                    
                    with col1:
                        st.metric("Files Processed", int(latest['files_processed']))
                    with col2:
                        st.metric("Rows Quarantined", int(latest['rows_quarantined']))
                    with col3:
                        st.metric("Rows Inserted", int(latest['rows_inserted']))
                    with col4:
                        st.metric("Rows Updated", int(latest['rows_updated']))
                    with col5:
                        st.metric("⚠️ Rows Deleted", int(latest['rows_deleted']))
                    
                    st.caption(f"Executed at: {latest['execution_timestamp']}")
                
                # Show full history table
                with st.expander("📊 View Full Execution History"):
                    # Format the dataframe for display
                    display_df = combined_logs.copy()
                    display_df['execution_timestamp'] = pd.to_datetime(display_df['execution_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    st.dataframe(
                        display_df,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "execution_timestamp": "Timestamp",
                            "files_processed": "Files",
                            "rows_quarantined": "Quarantined",
                            "rows_inserted": "Inserted",
                            "rows_updated": "Updated",
                            "rows_deleted": "Deleted"
                        }
                    )
                    
                    # Download option
                    csv_export = combined_logs.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Full Log History",
                        data=csv_export,
                        file_name="pipeline_execution_history.csv",
                        mime="text/csv"
                    )
    
    except Exception as e:
        st.error(f"Failed to load execution logs: {e}")

# ==========================================
# PAGE 3: DELETE RECORDS
# ==========================================
elif page == "🗑️ Delete Records":
    st.title("🗑️ Delete Records from Data Storage")
    st.caption("Upload a CSV with sample_id and test_date to permanently remove records")
    
    st.info("""
    **How it works:**
    1. Upload a CSV file containing two columns: `sample_id` and `test_date`
    2. The system will find matching records across all partitions
    3. Matching records will be permanently deleted from the data storage
    4. Updated parquet files will be saved back to storage
    """)
    
    st.warning("⚠️ **Warning:** Deletions are permanent and cannot be undone!")
    
    st.divider()
    
    # File uploader for deletion requests
    deletion_file = st.file_uploader(
        "Upload Deletion Request CSV",
        type="csv",
        help="CSV must contain 'sample_id' and 'test_date' columns"
    )
    
    if deletion_file:
        try:
            # Preview the deletion request
            deletion_df = pd.read_csv(deletion_file, dtype=str)
            
            # Validate columns
            if 'sample_id' not in deletion_df.columns or 'test_date' not in deletion_df.columns:
                st.error("❌ CSV must contain both 'sample_id' and 'test_date' columns!")
            else:
                st.success(f"✅ Found {len(deletion_df)} record(s) to delete")
                
                st.subheader("📋 Preview Deletion Request")
                st.dataframe(deletion_df, width="stretch")
                
                st.divider()
                
                # Upload button (just stages the file, doesn't process)
                if st.button("📤 Upload Deletion Request", type="primary"):
                    try:
                        from datetime import datetime
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"deletion_request_{timestamp}.csv"
                        
                        # Store in session state to simulate pending deletion requests
                        if 'pending_deletions' not in st.session_state:
                            st.session_state.pending_deletions = []
                        
                        st.session_state.pending_deletions.append({
                            'filename': filename,
                            'dataframe': deletion_df.copy()
                        })
                        
                        st.success(f"✅ Uploaded deletion request: `{filename}`")
                        st.info("""
                        **Next Steps:**
                        1. Review pending deletion requests below
                        2. Click **▶️ Trigger Delete Records Workflow** to process deletions
                        """)
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Failed to upload deletion request: {e}")
        
        except Exception as e:
            st.error(f"Error reading CSV file: {e}")
    else:
        st.info("📭 No file uploaded. Upload a CSV to begin.")
    
    st.divider()
    
    # Show existing deletion requests (using session state)
    st.subheader("📦 Pending Deletion Requests")
    
    if 'pending_deletions' not in st.session_state:
        st.session_state.pending_deletions = []
    
    if not st.session_state.pending_deletions:
        st.info("📭 No pending deletion requests")
    else:
        st.warning(f"⚠️ Found {len(st.session_state.pending_deletions)} pending deletion request(s)")
        
        # List files
        for item in st.session_state.pending_deletions:
            st.text(f"📄 {item['filename']}")
        
        st.divider()
        
        # Preview pending deletion files
        if st.session_state.pending_deletions:
            st.subheader("📋 Preview Deletion Requests")
            selected_deletion_file = st.selectbox(
                "Select file to preview:",
                [item['filename'] for item in st.session_state.pending_deletions],
                key="deletion_preview_selector"
            )
            
            if selected_deletion_file:
                selected_item = next(item for item in st.session_state.pending_deletions if item['filename'] == selected_deletion_file)
                preview_df = selected_item['dataframe']
                st.caption(f"Showing all rows of **{selected_deletion_file}**")
                st.dataframe(preview_df, width="stretch")
                
                # Show summary
                st.info(f"📊 Total records to delete: **{len(preview_df)}**")
        
        st.divider()
        
        # Trigger deletion workflow button
        st.subheader("🚀 Process Deletions")
        if st.button("▶️ Trigger Delete Records Workflow", type="primary", use_container_width=True):
            with st.status("🚀 Processing Delete Workflow...", expanded=True) as status:
                import time
                st.write(f"📋 Found {len(st.session_state.pending_deletions)} pending deletion request(s)")
                time.sleep(0.5)
                
                # Show what we're about to process
                for item in st.session_state.pending_deletions:
                    deletion_df = item['dataframe'].copy()
                    
                    # Parse test_date and calculate partitions for display
                    deletion_df['test_date'] = pd.to_datetime(deletion_df['test_date'])
                    deletion_df['partition'] = deletion_df['test_date'].apply(
                        lambda x: f"year={x.year}/week={x.isocalendar()[1]}"
                    )
                    
                    unique_partitions = deletion_df['partition'].unique()
                    st.write(f"📤 {item['filename']}: {len(deletion_df)} records across {len(unique_partitions)} partition(s)")
                    time.sleep(0.3)
                
                st.write("\n🔄 Executing deletions from data storage...")
                time.sleep(0.5)
                
                # Actually perform the deletions
                total_deleted, partitions_updated, log_messages = run_mock_deletions(st.session_state.pending_deletions)
                
                # Show log messages
                for msg in log_messages:
                    st.write(msg)
                    time.sleep(0.2)
                
                # Clear pending deletions
                st.session_state.pending_deletions = []
                
                if total_deleted > 0:
                    status.update(label="✅ Deletions Complete!", state="complete")
                    st.success(f"🗑️ Successfully deleted {total_deleted} record(s)!")
                    st.info("💡 **Demo Mode:** Records have been removed from the mock final report. In production, this would process parquet files across all partitions.")
                else:
                    status.update(label="✅ Workflow Complete", state="complete")
                    st.info("ℹ️ No matching records found to delete.")
                    st.caption("💡 **Demo Mode:** In production, this would trigger the delete_records.yaml GitHub Action.")

# ==========================================
# PAGE 4: FIX QUARANTINE
# ==========================================
elif page == "🛠️ Fix Quarantine":
    st.title("🛠️ Quarantine Manager")
    
    blob_list = list(quarantine_client.list_blobs())
    staged_names = [item['original_name'] for item in st.session_state.staged_fixes]
    remaining_blobs = [b.name for b in blob_list if b.name not in staged_names]
    
    if not remaining_blobs:
        if staged_names:
            st.info("Files staged for upload below.")
        else:
            st.success("🎉 Quarantine is empty!")
    else:
        sel = st.selectbox("Select file:", remaining_blobs)
        if sel:
            client = quarantine_client.get_blob_client(sel)
            stream = client.download_blob().readall()
            if isinstance(stream, str): stream = stream.encode('utf-8')
            
            df = pd.read_csv(io.BytesIO(stream), dtype=str)
            df = df.reset_index(drop=True)
            
            # Show column info
            st.caption(f"📋 Columns: {', '.join(df.columns.tolist())}")

            if "pipeline_error" in df.columns:
                errs = df["pipeline_error"].unique()
                st.warning(f"Errors: {', '.join(str(e) for e in errs)}")

            st.write("👇 **Double-click to edit:**")
            edited_df = st.data_editor(df, num_rows="dynamic", width="stretch")

            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Stage for Upload", width="stretch"):
                    clean_df = edited_df.drop(columns=["pipeline_error", "source_file"], errors='ignore')
                    st.session_state.staged_fixes.append({
                        "original_name": sel, "dataframe": clean_df
                    })
                    st.toast("Staged for upload!")
                    st.rerun()
            
            with col2:
                if st.button("🗑️ Delete File", type="secondary", width="stretch"):
                    st.session_state.confirm_delete_quarantine = sel
            
            # Confirmation dialog for quarantine deletion
            if st.session_state.get("confirm_delete_quarantine") == sel:
                st.warning(f"⚠️ Are you sure you want to delete `{sel}`? This action cannot be undone.")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ Yes, Delete", type="primary", key="confirm_yes_quarantine"):
                        try:
                            client = quarantine_client.get_blob_client(sel)
                            client.delete_blob()
                            st.session_state.confirm_delete_quarantine = None
                            st.toast(f"Deleted `{sel}` from quarantine")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to delete: {e}")
                with col2:
                    if st.button("❌ Cancel", key="confirm_no_quarantine"):
                        st.session_state.confirm_delete_quarantine = None
                        st.rerun()
    
    # REVIEW STAGED FIXES SECTION
    if st.session_state.staged_fixes:
        st.divider()
        st.subheader("📦 Review Staged Files")
        st.caption(f"{len(st.session_state.staged_fixes)} file(s) ready to upload")
        
        # List staged files
        for item in st.session_state.staged_fixes:
            st.text(f"📄 {item['original_name']} ({len(item['dataframe'])} rows)")
        
        # Preview staged files
        if st.session_state.staged_fixes:
            st.write("**Preview:**")
            preview_choice = st.selectbox(
                "Select staged file to preview:",
                [item['original_name'] for item in st.session_state.staged_fixes],
                key="staged_preview"
            )
            
            if preview_choice:
                selected_item = next(item for item in st.session_state.staged_fixes if item['original_name'] == preview_choice)
                df_preview = selected_item['dataframe'].head(10)
                st.caption(f"Showing first 10 rows of **{preview_choice}**")
                st.dataframe(df_preview, width="stretch")
        
        st.divider()
        
        # Upload button
        if st.button(f"🚀 Upload All {len(st.session_state.staged_fixes)} Fixed File(s) to Cloud", type="primary"):
            progress_bar = st.progress(0)
            
            for idx, item in enumerate(st.session_state.staged_fixes):
                fname = item['original_name']
                df = item['dataframe']
                csv_bytes = df.to_csv(index=False).encode('utf-8')
                landing_client.upload_blob(name=fname, data=csv_bytes, overwrite=True)
                st.write(f"✅ Promoted `{fname}`")
                q_blob = quarantine_client.get_blob_client(fname)
                q_blob.delete_blob()
                progress_bar.progress((idx + 1) / len(st.session_state.staged_fixes))
            
            st.session_state.staged_fixes = []
            st.session_state.upload_success = True
            st.rerun()
        
        # Show success message after rerun
        if st.session_state.upload_success:
            st.success("✨ Done! All fixed files uploaded to Landing Zone.")
            st.session_state.upload_success = False

# ==========================================
# PAGE 5: FINAL REPORT
# ==========================================
elif page == "📊 Final Report":
    st.title("📊 CDC Final Export Review")
    
    client = data_client.get_blob_client("final_cdc_export.csv")
    
    if not client.exists():
        st.warning("⚠️ No report found.")
    else:
        props = client.get_blob_properties()
        size_mb = (props.size / 1024 / 1024) if props.size else 0.0
        
        st.info(f"📅 Last: **{props.last_modified.strftime('%Y-%m-%d %H:%M:%S')}** | Size: **{size_mb:.2f} MB**")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("👁️ Preview (Top 1,000 Rows)"):
                data = client.download_blob().readall()
                if isinstance(data, str): data = data.encode('utf-8')
                st.session_state.preview_df = pd.read_csv(io.BytesIO(data), nrows=1000)

        with col2:
            if st.button("📥 Prepare Download"):
                data = client.download_blob().readall()
                if isinstance(data, str): data = data.encode('utf-8')
                st.session_state.full_download = data
        
        if "preview_df" in st.session_state:
            st.divider()
            st.subheader("Data Preview")
            st.dataframe(st.session_state.preview_df, width="stretch")
        
        if "full_download" in st.session_state:
            st.download_button(
                label="💾 Download Full CSV",
                data=st.session_state.full_download,
                file_name="final_cdc_export.csv",
                mime="text/csv"
            )