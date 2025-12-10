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
st.set_page_config(
    page_title="Lab Data Admin (Portfolio Demo)", 
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': """### Lab Data Pipeline Admin Console
        **Built by Daniel B. Cooper**
        
        An agile data engineering solution.
        
        - [GitHub](https://github.com/thedbcooper)
        - [LinkedIn](https://www.linkedin.com/in/danielblakecooper/)
        - [ORCID](https://orcid.org/0000-0002-2218-7916)
        """
    }
)

# ==========================================
# 🧠 SESSION STATE INIT
# ==========================================
if "staged_fixes" not in st.session_state:
    st.session_state.staged_fixes = []
if "upload_counter" not in st.session_state:
    st.session_state.upload_counter = 0
if "upload_success" not in st.session_state:
    st.session_state.upload_success = False
if "deletion_uploader_counter" not in st.session_state:
    st.session_state.deletion_uploader_counter = 0

# Show toast notifications after rerun
if "toast_message" in st.session_state:
    st.toast(st.session_state.toast_message)
    del st.session_state.toast_message

# ==========================================
# ☁️ MOCK CLIENT INITIALIZATION
# ==========================================
@st.cache_resource
def get_mock_clients():
    return (
        MockContainerClient("landing-zone"),
        MockContainerClient("quarantine"),
        MockContainerClient("data"),
        MockContainerClient("logs"),
        MockContainerClient("deletion-requests")
    )

landing_client, quarantine_client, data_client, logs_client, deletion_client = get_mock_clients()

# ==========================================
# 🤖 MINI-PIPELINE
# ==========================================
def run_mock_pipeline():
    from datetime import datetime
    
    # Initialize execution log
    execution_start = datetime.now()
    processing_log = []  # Track detailed processing events with emojis
    metrics = {
        'execution_timestamp': execution_start.isoformat(),
        'files_processed': 0,
        'rows_quarantined': 0,
        'rows_inserted': 0,
        'rows_updated': 0
    }
    
    log = []
    blobs = list(landing_client.list_blobs())
    if not blobs:
        # Save log even for empty runs
        processing_log.append("📭 No new files in landing zone")
        _save_mock_execution_log(metrics, processing_log)
        return "📭 No new files in Landing Zone."
    
    metrics['files_processed'] = len(blobs)
    log.append(f"📦 Found {len(blobs)} new files to process.")
    processing_log.append(f"📦 Found {len(blobs)} file(s) to process")

    report_blob = data_client.get_blob_client("final_cdc_export.csv")
    if report_blob.exists():
        history_bytes = report_blob.download_blob().readall()
        history_df = pd.read_csv(io.BytesIO(history_bytes), dtype=str)
        existing_ids = set(history_df['sample_id'].tolist()) if 'sample_id' in history_df.columns else set()
    else:
        history_df = pd.DataFrame(columns=["sample_id", "test_date", "result", "viral_load"])
        existing_ids = set()
    
    rows_before = len(history_df)
    log.append(f"📊 History contains {rows_before} rows.")

    all_valid_rows = []
    all_error_rows = []
    
    for blob_prop in blobs:
        b_client = landing_client.get_blob_client(blob_prop.name)
        data = b_client.download_blob().readall()
        try:
            if isinstance(data, str): data = data.encode('utf-8')
            df = pd.read_csv(io.BytesIO(data), dtype=str)
            
            # VALIDATE EACH ROW - Use list comprehension for better performance
            valid_count = 0
            error_count = 0
            
            # Process rows using to_dict('records') instead of iterrows for better performance
            rows = df.to_dict('records')
            for row in rows:
                try:
                    # Pydantic validation - Pydantic will handle type conversion from strings
                    valid_sample = LabResult(**row)  # type: ignore[arg-type]
                    all_valid_rows.append(valid_sample.model_dump())
                    valid_count += 1
                except ValidationError as e:
                    bad_row = row.copy()
                    bad_row['pipeline_error'] = str(e)
                    bad_row['source_file'] = blob_prop.name
                    all_error_rows.append(bad_row)
                    error_count += 1
            
            # Delete processed file
            b_client.delete_blob()
            log.append(f"✅ Processed & Deleted: {blob_prop.name} ({valid_count} valid, {error_count} errors)")
            processing_log.append(f"✅ Processed {blob_prop.name}: {valid_count} valid, {error_count} errors")
            
        except Exception as e:
            log.append(f"❌ CRITICAL ERROR reading {blob_prop.name}: {e}")
            processing_log.append(f"❌ Error reading {blob_prop.name}: {str(e)}")

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
        processing_log.append(f"⚠️ Quarantined {len(all_error_rows)} row(s) to {filename}")

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
        processing_log.append(f"📊 Processing {len(all_valid_rows)} record(s): ➕ {inserts} inserts, 🔄 {updates} updates")
        processing_log.append(f"✅ Data upserted: {rows_before} → {rows_after} total rows")
        log.append(f"✅ Report successfully updated!")
    elif not all_error_rows:
        log.append("⚠️ No valid data to process.")
        processing_log.append("⚠️ No valid data to process")
    
    # Save execution log with processing details
    _save_mock_execution_log(metrics, processing_log)
    
    # Return log string with metrics appended
    return "\n".join(log) + f"\n\nMETRICS|{metrics['files_processed']}|{metrics['rows_quarantined']}|{metrics['rows_inserted']}|{metrics['rows_updated']}"

def _save_mock_execution_log(metrics, processing_log):
    """Save execution log to logs container."""
    from datetime import datetime
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"execution_{timestamp}.csv"
        
        # Add processing details to metrics
        metrics['processing_details'] = ' | '.join(processing_log)
        
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

def run_mock_deletions():
    """Process deletion requests from the deletion-requests container and remove records from mock data."""
    from datetime import datetime
    
    total_deleted = 0
    partitions_updated = 0
    processing_log = []  # Use processing_log for CSV storage
    
    # Get pending deletion requests from container
    deletion_blobs = list(deletion_client.list_blobs())
    
    # Initialize deletion log metrics
    execution_start = datetime.now()
    metrics = {
        'execution_timestamp': execution_start.isoformat(),
        'files_processed': len(deletion_blobs),
        'rows_deleted': 0,
        'partitions_updated': 0
    }
    
    # Get the final report blob
    report_blob = data_client.get_blob_client("final_cdc_export.csv")
    
    if not report_blob.exists():
        processing_log.append("⚠️ No data found to delete from")
        _save_mock_deletion_log(metrics, processing_log)
        return 0, 0
    
    # Load existing data
    history_bytes = report_blob.download_blob().readall()
    current_df = pd.read_csv(io.BytesIO(history_bytes), dtype=str)
    original_count = len(current_df)
    
    processing_log.append(f"📊 Current data contains {original_count} records")
    
    # Collect all IDs to delete from deletion-requests container
    all_ids_to_delete = set()
    
    for blob in deletion_blobs:
        blob_client = deletion_client.get_blob_client(blob.name)
        data = blob_client.download_blob().readall()
        if isinstance(data, str): data = data.encode('utf-8')
        deletion_df = pd.read_csv(io.BytesIO(data), dtype=str)
        ids_from_file = set(deletion_df['sample_id'].tolist())
        all_ids_to_delete.update(ids_from_file)
        processing_log.append(f"✅ Processed {blob.name}: {len(ids_from_file)} deletion request(s)")
        
        # Delete processed deletion request file (mimics production behavior)
        blob_client.delete_blob()
    
    processing_log.append(f"🔍 Total unique deletion requests: {len(all_ids_to_delete)}")
    
    # Find which IDs actually exist in the data
    existing_ids = set(current_df['sample_id'].tolist())
    ids_to_actually_delete = all_ids_to_delete.intersection(existing_ids)
    
    # Filter out records to delete
    filtered_df = current_df[~current_df['sample_id'].isin(all_ids_to_delete)]
    
    rows_deleted = original_count - len(filtered_df)
    total_deleted = rows_deleted
    metrics['rows_deleted'] = rows_deleted
    
    if rows_deleted > 0:
        # Save updated data back
        csv_out = filtered_df.to_csv(index=False).encode('utf-8')
        report_blob.upload_blob(csv_out, overwrite=True)
        partitions_updated = 1  # Mock: treating the whole CSV as one "partition"
        metrics['partitions_updated'] = partitions_updated
        
        # Log which IDs were deleted
        ids_str = ', '.join(sorted(ids_to_actually_delete))
        processing_log.append(f"🗑️ Deleted {rows_deleted} record(s) | 🆔 IDs: {ids_str}")
        processing_log.append(f"📊 Remaining records: {len(filtered_df)}")
    else:
        processing_log.append("ℹ️ No matching records found to delete")
    
    # Save deletion log to CSV
    _save_mock_deletion_log(metrics, processing_log)
    
    return total_deleted, partitions_updated

def _save_mock_deletion_log(metrics, processing_log):
    """Save deletion log to logs container as CSV."""
    from datetime import datetime
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"deletion_{timestamp}.csv"
        
        # Add processing details to metrics
        metrics['processing_details'] = ' | '.join(processing_log)
        
        # Convert to DataFrame and CSV
        log_df = pd.DataFrame([metrics])
        log_csv = log_df.to_csv(index=False).encode('utf-8')
        
        # Upload to logs container
        logs_client.upload_blob(log_filename, log_csv, overwrite=True)
        print(f"✅ Deletion log saved: {log_filename}")  # Debug output
    except Exception as e:
        print(f"⚠️ Failed to save deletion log: {e}")
        import traceback
        traceback.print_exc()

# ==========================================
# SIDEBAR: NAVIGATION & CONTROLS
# ==========================================
with st.sidebar:
    st.header("🧬 Agile Data Pipeline: Demo Admin Console")
    st.info("ℹ️ **DEMO MODE ACTIVE**\n\nThis app uses in-memory mock storage. No real Azure resources are connected.")
    
    with st.expander("🏗️ **Production Architecture**"):
        st.markdown("""
        **Azure Blob Storage:**
        - `landing-zone` - Incoming CSV files
        - `quarantine` - Invalid rows for review
        - `data` - Partitioned Parquet files
        - `logs` - Execution history
        - `deletion-requests` - Pending deletes
        
        **GitHub Actions:**
        - `weekly_pipeline.yaml` - Data ingestion
        - `delete_records.yaml` - Record removal
        
        **Authentication:**
        - Azure: `DefaultAzureCredential`
        - GitHub: Personal Access Token
        """)

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
        ["🏠 Start Here", "📤 Upload New Data", "🛠️ Fix Quarantine", "🗑️ Delete Records", "⚙️ Data Ingestion", "📊 Final Report", "ℹ️ About"],
        key="nav_selection"
    )
    
    st.divider()
    
    # Profile links
    st.markdown("""
    <style>
    .profile-link {
        display: inline-block;
        transition: transform 0.3s ease, opacity 0.7s ease;
        opacity: 0.8;
    }
    .profile-link:hover {
        transform: scale(1.2) rotate(5deg);
        opacity: 1;
    }
    </style>
    <div style='display: flex; gap: 20px; align-items: center; margin-top: 10px;'>
        <a href='https://github.com/thedbcooper' target='_blank' title='GitHub' class='profile-link'>
            <img src='https://cdn-icons-png.flaticon.com/512/25/25231.png' width='36' height='36'/>
        </a>
        <a href='https://www.linkedin.com/in/danielblakecooper/' target='_blank' title='LinkedIn' class='profile-link'>
            <img src='https://cdn-icons-png.flaticon.com/512/174/174857.png' width='36' height='36'/>
        </a>
        <a href='https://orcid.org/0000-0002-2218-7916' target='_blank' title='ORCID' class='profile-link'>
            <img src='https://orcid.org/assets/vectors/orcid.logo.icon.svg' width='36' height='36'/>
        </a>
    </div>
    """, unsafe_allow_html=True)
    
    st.space()
    
    # Author credit
    st.caption("👨‍💻 Built by **Daniel B. Cooper**")
    st.caption("🎯 Data Engineering Portfolio")

# ==========================================
# PAGE 0: LANDING PAGE
# ==========================================
if page == "🏠 Start Here":
    st.title("🧬 Lab Data Pipeline: Admin Console")
    st.markdown("""
    **Welcome.** This dashboard allows users to safely manage the flow of data
    into an Azure Lakehouse without needing to write code.
    """)
    
    # Demo vs Production callout
    st.success("""
    🎯 **Portfolio Demo:** This is a fully-functional demonstration of the production admin tool. 
    Each page includes a **\"How this works in Production\"** expander explaining the Azure & GitHub Actions integration.
    Click the 🏗️ **Production Architecture** expander in the sidebar for an overview.
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
        * **Admin/Demo:** Go to **⚙️ Data Ingestion** tab and click **▶️ Trigger Weekly Pipeline**.
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
        st.markdown("Upload fixed files back to landing zone, then go to **⚙️ Data Ingestion** to trigger ingestion.")
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
    
    with st.expander("🔧 **How this works in Production**", expanded=False):
        st.markdown("""
        **Azure Blob Storage Integration:**
        - Files are uploaded to the `landing-zone` container in Azure Blob Storage
        - Uses `DefaultAzureCredential` for secure authentication (supports managed identity, Azure CLI, etc.)
        - The `BlobServiceClient` handles all upload operations with automatic retry logic
        
        *In this demo, uploads go to an in-memory mock storage that simulates Azure's behavior.*
        """)
    
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
        st.success("✨ Done! All files uploaded to Landing Zone. Go to **⚙️ Data Ingestion** to trigger the pipeline.")
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
# PAGE 2: DATA INGESTION
# ==========================================
elif page == "⚙️ Data Ingestion":
    st.title("⚙️ Data Ingestion")
    st.caption("View queued files, trigger pipeline processing, and review execution history")
    
    with st.expander("🔧 **How this works in Production**", expanded=False):
        st.markdown("""
        **GitHub Actions Workflow Dispatch:**
        - The "Trigger Weekly Pipeline" button calls the GitHub API to dispatch `weekly_pipeline.yaml`
        - Requires `GITHUB_TOKEN` with `repo` and `workflow` permissions in `.env`
        - Pipeline runs on GitHub-hosted runners with Azure credentials stored as GitHub Secrets
        
        **Automated Scheduling:**
        - Production pipeline runs automatically via cron schedule (weekly)
        - Manual triggers available for ad-hoc processing
        
        **Real-time Auto-Monitoring with Streamlit Fragments:**
        - Uses `@st.fragment(run_every="15s")` to poll GitHub Actions API every 15 seconds
        - Automatically detects when new workflow starts (vs. old runs) using UTC timestamps
        - Shows live status updates without full page reloads for better UX
        - Auto-stops monitoring when workflow completes and triggers full page refresh
        - Persists completion status in session state so success messages remain visible
        - Links directly to GitHub Actions logs for detailed debugging
        
        *In this demo, the pipeline runs locally in-browser using Python logic that mirrors the production scripts.*
        """)
    
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
                                        st.session_state.toast_message = f"Deleted `{selected_blob_name}` from landing zone"
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
                    _, files_processed, rows_quarantined, rows_inserted, rows_updated = metrics_line.split('|')
                    
                    # Display summary
                    st.success("✨ **Pipeline executed successfully!**")
                    st.balloons()
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Files Processed", files_processed)
                    with col2:
                        st.metric("Rows Quarantined", rows_quarantined, delta=None if rows_quarantined == '0' else f"-{rows_quarantined}", delta_color="inverse")
                    with col3:
                        st.metric("Rows Inserted", rows_inserted)
                    with col4:
                        st.metric("Rows Updated", rows_updated)
                else:
                    # Fallback to old parsing method
                    st.success("✨ **Pipeline executed successfully!**")
                
                # Clean log for display (remove METRICS line)
                display_log = '\n'.join([line for line in result_log.split('\n') if not line.startswith('METRICS|')])
                with st.expander("📜 View Detailed Log"):
                    st.code(display_log, language="text")
                
                # Rerun to refresh the landing zone view
                time.sleep(1)  # Brief pause to let user see the results
                st.rerun()
    
    st.divider()
    
    # EXECUTION LOGS SECTION
    st.subheader("📈 Pipeline Execution History")
    st.caption("Metrics from previous pipeline runs")
    
    try:
        # Get all logs and filter for execution logs only
        all_log_blobs = list(logs_client.list_blobs())
        log_blobs = [blob for blob in all_log_blobs if blob.name.startswith('execution_')]
        
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
                    
                    # Display processing details dropdown for all runs
                    if 'processing_details' in combined_logs.columns:
                        st.divider()
                        st.subheader("📋 Processing Details by Run")
                        
                        # Create dropdown to select which run to view
                        runs_with_details = combined_logs[combined_logs['processing_details'].notna() & (combined_logs['processing_details'] != '')].copy()
                        
                        if len(runs_with_details) > 0:
                            # Format timestamps for display
                            runs_with_details['display_timestamp'] = pd.to_datetime(runs_with_details['execution_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
                            run_options = [f"Run at {row['display_timestamp']}" for _, row in runs_with_details.iterrows()]
                            
                            selected_run = st.selectbox(
                                "Select a run to view details:",
                                options=run_options,
                                key="demo_pipeline_details_selector"
                            )
                            
                            # Find and display the selected run's details
                            selected_idx = run_options.index(selected_run)
                            selected_row = runs_with_details.iloc[selected_idx]
                            
                            st.write(f"**🕐 {selected_run}**")
                            details = selected_row['processing_details'].split(' | ')
                            for detail in details:
                                st.markdown(f"{detail}")
                        else:
                            st.info("No processing details available for any runs.")
                
                # Show full history table
                with st.expander("📊 View Full Execution History"):
                    # Format the dataframe for display
                    display_df = combined_logs.copy()
                    display_df['execution_timestamp'] = pd.to_datetime(display_df['execution_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Show main metrics table (without processing_details column)
                    metrics_columns = ['execution_timestamp', 'files_processed', 'rows_quarantined', 'rows_inserted', 'rows_updated']
                    display_metrics = display_df[metrics_columns] if all(col in display_df.columns for col in metrics_columns) else display_df
                    
                    st.dataframe(
                        display_metrics,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "execution_timestamp": "Timestamp",
                            "files_processed": "Files",
                            "rows_quarantined": "Quarantined",
                            "rows_inserted": "Inserted",
                            "rows_updated": "Updated"
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
    
    with st.expander("🔧 **How this works in Production**", expanded=False):
        st.markdown("""
        **Deletion Request Container:**
        - Deletion CSVs are uploaded to a dedicated `deletion-requests` container in Azure
        - This provides an audit trail of all deletion requests before processing
        
        **GitHub Actions Workflow:**
        - The "Trigger Delete Workflow" button dispatches `delete_records.yaml` via GitHub API
        - The workflow scans all year/week partitions in the `data` container
        - Matching records are removed from Parquet files and re-saved
        - Deletion logs are written to the `logs` container for compliance
        
        **Auto-Monitoring with Streamlit Fragments:**
        - Uses `@st.fragment(run_every="15s")` to automatically poll GitHub Actions API
        - Detects new deletion workflows vs. historical runs using timestamp comparison
        - Provides live progress updates every 15 seconds without interrupting the UI
        - Automatically stops monitoring and refreshes page when deletion completes
        - Success/failure status persists in session state for visibility after completion
        
        *In this demo, deletions are processed immediately in-memory against mock data.*
        """)
    
    st.warning("⚠️ **Warning:** Deletions are permanent and cannot be undone!")
    
    st.divider()
    
    # File uploader for deletion requests
    deletion_file = st.file_uploader(
        "Upload Deletion Request CSV",
        type="csv",
        help="CSV must contain 'sample_id' and 'test_date' columns",
        key=f"deletion_uploader_{st.session_state.deletion_uploader_counter}"
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
                
                # Upload button
                if st.button("📤 Upload Deletion Request", type="primary"):
                    try:
                        from datetime import datetime
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"deletion_request_{timestamp}.csv"
                        
                        # Upload to deletion-requests container (mimics production)
                        csv_data = deletion_df.to_csv(index=False).encode('utf-8')
                        deletion_client.upload_blob(filename, csv_data, overwrite=True)
                        
                        # Increment counter to clear the uploader on rerun
                        st.session_state.deletion_uploader_counter += 1
                        st.session_state.toast_message = f"Uploaded deletion request: `{filename}`"
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Failed to upload deletion request: {e}")
        
        except Exception as e:
            st.error(f"Error reading CSV file: {e}")
    else:
        st.info("📭 No file uploaded. Upload a CSV to begin.")
    
    st.divider()
    
    # Show existing deletion requests from container
    st.subheader("📦 Pending Deletion Requests")
    
    try:
        deletion_blobs = list(deletion_client.list_blobs())
        
        if not deletion_blobs:
            st.info("📭 No pending deletion requests")
        else:
            st.warning(f"⚠️ Found {len(deletion_blobs)} pending deletion request(s)")
            
            # Combined preview and file selector
            selected_deletion_file = st.selectbox(
                "Select file to preview:",
                [blob.name for blob in deletion_blobs],
                key="deletion_preview_selector"
            )
            
            if selected_deletion_file:
                blob_client = deletion_client.get_blob_client(selected_deletion_file)
                try:
                    data = blob_client.download_blob().readall()
                    if isinstance(data, str): data = data.encode('utf-8')
                    preview_df = pd.read_csv(io.BytesIO(data))
                    
                    st.info(f"📊 **{len(preview_df)}** record(s) to delete")
                    st.dataframe(preview_df, width="stretch")
                    
                    # Delete button with confirmation
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("🗑️ Delete This Request", type="secondary", use_container_width=True, key="delete_deletion_request"):
                            st.session_state.confirm_delete_deletion = selected_deletion_file
                    
                    # Confirmation dialog
                    if st.session_state.get("confirm_delete_deletion") == selected_deletion_file:
                        with col2:
                            st.warning("⚠️ Confirm deletion?")
                        
                        confirm_col1, confirm_col2 = st.columns(2)
                        with confirm_col1:
                            if st.button("✅ Yes, Delete", type="primary", key="confirm_yes_deletion"):
                                try:
                                    blob_client.delete_blob()
                                    st.session_state.confirm_delete_deletion = None
                                    st.session_state.toast_message = f"Deleted `{selected_deletion_file}` from deletion requests"
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to delete: {e}")
                        with confirm_col2:
                            if st.button("❌ Cancel", key="confirm_no_deletion"):
                                st.session_state.confirm_delete_deletion = None
                                st.rerun()
                    
                except Exception as e:
                    st.error(f"Error reading file: {e}")
        
            st.divider()
            
            # Trigger deletion workflow button
            st.subheader("🚀 Process Deletions")
            if st.button("▶️ Trigger Delete Records Workflow", type="primary", use_container_width=True):
                with st.status("🚀 Processing Delete Workflow...", expanded=True) as status:
                    import time
                    st.write("🔍 Scanning deletion-requests container...")
                    time.sleep(0.5)
                    
                    # Actually perform the deletions
                    total_deleted, partitions_updated = run_mock_deletions()
                    
                    if total_deleted > 0:
                        status.update(label="✅ Deletions Complete!", state="complete")
                        st.success(f"🗑️ Successfully deleted {total_deleted} record(s)!")
                        st.info("📊 View detailed deletion metrics in the Deletion Execution History section below")
                    else:
                        status.update(label="✅ Workflow Complete", state="complete")
                        st.info("ℹ️ No matching records found to delete.")
                    
                    # Rerun to refresh the pending deletion requests view
                    time.sleep(1)  # Brief pause to let user see the results
                    st.rerun()
    
    except Exception as e:
        st.error(f"Failed to process deletion requests: {e}")
    
    st.divider()
    
    # DELETION EXECUTION HISTORY SECTION
    st.subheader("📈 Deletion Execution History")
    st.caption("Logs from previous deletion runs")
    
    try:
        # Get all logs and filter for deletion logs only
        all_log_blobs = list(logs_client.list_blobs())
        deletion_log_blobs = [blob for blob in all_log_blobs if blob.name.startswith('deletion_')]
        
        if not deletion_log_blobs:
            st.info("📭 No deletion logs found. Run the delete workflow to generate logs.")
        else:
            st.success(f"Found {len(deletion_log_blobs)} deletion log(s)")
            
            # Load all deletion logs into a single dataframe
            all_deletion_logs = []
            for blob in sorted(deletion_log_blobs, key=lambda x: x.name, reverse=True):  # Most recent first
                try:
                    blob_client = logs_client.get_blob_client(blob.name)
                    log_data = blob_client.download_blob().readall()
                    if isinstance(log_data, str): log_data = log_data.encode('utf-8')
                    log_df = pd.read_csv(io.BytesIO(log_data))
                    all_deletion_logs.append(log_df)
                except Exception as e:
                    st.warning(f"Could not read {blob.name}: {e}")
            
            if all_deletion_logs:
                # Combine all logs
                combined_deletion_logs: pd.DataFrame = pd.concat(all_deletion_logs, ignore_index=True)
                
                # Sort by timestamp (most recent first)
                combined_deletion_logs = combined_deletion_logs.sort_values('execution_timestamp', ascending=False)
                
                # Display summary metrics from most recent run
                if len(combined_deletion_logs) > 0:
                    latest: pd.Series = combined_deletion_logs.iloc[0]
                    
                    st.write("**Latest Deletion Run:**")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Files Processed", int(latest['files_processed']))
                    with col2:
                        st.metric("Rows Deleted", int(latest['rows_deleted']))
                    with col3:
                        st.metric("Partitions Updated", int(latest['partitions_updated']))
                    
                    st.caption(f"Executed at: {latest['execution_timestamp']}")
                    
                    # Display processing details dropdown for all runs
                    if 'processing_details' in combined_deletion_logs.columns:
                        st.divider()
                        st.subheader("📋 Processing Details by Run")
                        
                        # Create dropdown to select which run to view
                        runs_with_details = combined_deletion_logs[combined_deletion_logs['processing_details'].notna() & (combined_deletion_logs['processing_details'] != '')].copy()
                        
                        if len(runs_with_details) > 0:
                            # Format timestamps for display
                            runs_with_details['display_timestamp'] = pd.to_datetime(runs_with_details['execution_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
                            run_options = [f"Run at {row['display_timestamp']}" for _, row in runs_with_details.iterrows()]
                            
                            selected_run = st.selectbox(
                                "Select a run to view details:",
                                options=run_options,
                                key="demo_deletion_details_selector"
                            )
                            
                            # Find and display the selected run's details
                            selected_idx = run_options.index(selected_run)
                            selected_row = runs_with_details.iloc[selected_idx]
                            
                            st.write(f"**🕐 {selected_run}**")
                            details = selected_row['processing_details'].split(' | ')
                            for detail in details:
                                st.markdown(f"{detail}")
                        else:
                            st.info("No processing details available for any runs.")
                
                # Show full history table
                with st.expander("📊 View Full Deletion History"):
                    # Format the dataframe for display
                    display_df: pd.DataFrame = combined_deletion_logs.copy()
                    display_df['execution_timestamp'] = pd.to_datetime(display_df['execution_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Show main metrics table (without processing_details column)
                    metrics_columns = ['execution_timestamp', 'files_processed', 'rows_deleted', 'partitions_updated']
                    display_metrics = display_df[metrics_columns] if all(col in display_df.columns for col in metrics_columns) else display_df
                    
                    st.dataframe(
                        display_metrics,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "execution_timestamp": "Timestamp",
                            "files_processed": "Files",
                            "rows_deleted": "Deleted",
                            "partitions_updated": "Partitions"
                        }
                    )
                    
                    # Download option
                    csv_export = combined_deletion_logs.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Full Deletion History",
                        data=csv_export,
                        file_name="deletion_execution_history.csv",
                        mime="text/csv"
                    )
    
    except Exception as e:
        st.error(f"Failed to load deletion logs: {e}")

# ==========================================
# PAGE 4: FIX QUARANTINE
# ==========================================
elif page == "🛠️ Fix Quarantine":
    st.title("🛠️ Quarantine Manager")
    
    with st.expander("🔧 **How this works in Production**", expanded=False):
        st.markdown("""
        **Quarantine Container:**
        - Invalid rows are automatically moved to the `quarantine` container during pipeline processing
        - Each quarantine file includes `pipeline_error` and `source_file` columns for debugging
        - Files persist until manually reviewed and fixed by an epidemiologist
        
        **Pydantic Validation:**
        - Data validation uses Pydantic models (`models.py`) to enforce schema rules
        - Validates: `sample_id` format, `test_date` as valid date, `result` in [POS, NEG, IND], `viral_load` as integer
        - Detailed error messages help identify exactly which field failed validation
        
        **Fix & Re-upload Flow:**
        - Fixed files are uploaded back to `landing-zone` container
        - Original quarantine file is deleted after successful fix
        - Re-trigger the pipeline to process the corrected data
        
        *In this demo, the quarantine container is simulated in-memory with sample error data.*
        """)
    
    blob_list = list(quarantine_client.list_blobs())
    staged_names = [item['original_name'] for item in st.session_state.staged_fixes]
    remaining_blobs = [b.name for b in blob_list if b.name not in staged_names]
    
    if not remaining_blobs:
        if staged_names:
            st.info("Files staged for upload below.")
        else:
            st.success("🎉 Quarantine is empty! Move to Data Ingestion.")
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
                    st.session_state.toast_message = "Staged for upload!"
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
                            st.session_state.toast_message = f"Deleted `{sel}` from quarantine"
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
                
                # Unstage button
                if st.button(f"↩️ Unstage {preview_choice}", type="secondary", key="unstage_button"):
                    st.session_state.staged_fixes = [item for item in st.session_state.staged_fixes if item['original_name'] != preview_choice]
                    st.session_state.toast_message = f"Unstaged `{preview_choice}` - you can edit it again"
                    st.rerun()
        
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
    
    with st.expander("🔧 **How this works in Production**", expanded=False):
        st.markdown("""
        **Data Container & Partitioning:**
        - Production data is stored in the `data` container as Parquet files
        - Partitioned by `year=YYYY/week=WW/` for efficient querying and incremental updates
        - The pipeline exports a consolidated `final_cdc_export.csv` for easy download
        
        **Upsert Logic:**
        - Records are matched by `sample_id` (primary key)
        - New records are inserted; existing records are updated with latest values
        - Sorted by `test_date` descending for most recent results first
        
        **Azure Blob Properties:**
        - Large files streamed directly to browser for download
        
        *In this demo, data is stored in-memory and exported as CSV on demand.*
        """)
    
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
            data = client.download_blob().readall()
            if isinstance(data, str): data = data.encode('utf-8')
            
            st.download_button(
                label="📥 Download Full CSV",
                data=data,
                file_name="final_cdc_export.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        if "preview_df" in st.session_state:
            st.divider()
            st.subheader("Data Preview")
            st.dataframe(st.session_state.preview_df, width="stretch")

# ==========================================
# PAGE 6: ABOUT
# ==========================================
elif page == "ℹ️ About":
    st.title("ℹ️ About This Project")
    
    st.markdown("""
    ## The Challenge
    
    As an epidemiologist in a state public health department, I observed a critical gap in our data infrastructure. 
    Program teams needed faster access to their data for real-time decision-making, but traditional centralized 
    data warehouses created bottlenecks. When disease surveillance data arrived in varied formats from multiple 
    partners, manual review and correction processes delayed insights by days or weeks.
    
    ## The Solution
    
    I designed and built this agile data pipeline to enable **self-service analytics** - empowering public health 
    teams to manage their own data flows without waiting on IT support. The system automatically validates incoming 
    lab results against schema requirements, quarantines errors for human review, and maintains a clean master 
    dataset ready for immediate analysis.
    
    ## Technical Implementation
    
    This project demonstrates full-stack data engineering capabilities:
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Cloud Infrastructure:**
        - Azure Lakehouse Architecture
        - Scalable blob storage with partitioned Parquet files
        - Year/week partitioning for efficient queries
        - Automated backups and version control
        
        **Data Quality:**
        - Pydantic validation models for type safety
        - Schema enforcement at ingestion
        - Detailed error logging and quarantine system
        - Audit trail for all data transformations
        """)
    
    with col2:
        st.markdown("""
        **Automation & DevOps:**
        - GitHub Actions CI/CD pipelines
        - Automated weekly batch processing
        - Manual override capability for ad-hoc runs
        - Real-time workflow monitoring with Streamlit fragments
        
        **User Experience:**
        - Streamlit admin interface for non-technical users
        - Excel-like data editor for error correction
        - Self-service file upload and monitoring
        - Downloadable execution logs and reports
        """)
    
    st.divider()
    
    st.markdown("""
    ## Impact
    
    This system demonstrates how modern data engineering practices can **democratize data access** in public health, 
    reducing time-to-insight while maintaining data integrity and audit trails required for regulatory compliance.
    
    By removing technical barriers, epidemiologists can focus on what they do best - analyzing disease trends and 
    protecting public health - rather than waiting for data access or wrestling with manual file processing.
    
    ## Technical Skills Demonstrated
    """)
    
    skill_col1, skill_col2, skill_col3 = st.columns(3)
    
    with skill_col1:
        st.markdown("""
        **Languages & Frameworks:**
        - Python (Pandas, Pydantic)
        - SQL
        - YAML (CI/CD)
        - Markdown
        """)
    
    with skill_col2:
        st.markdown("""
        **Cloud & Infrastructure:**
        - Azure Blob Storage
        - Azure Data Lake
        - GitHub Actions
        - Container orchestration
        """)
    
    with skill_col3:
        st.markdown("""
        **Data Engineering:**
        - ETL pipeline design
        - Data validation & quality
        - Lakehouse architecture
        - Error handling workflows
        """)
    
    st.divider()
    
    st.success("""
    **Built by Daniel B. Cooper** | Epidemiologist & Aspiring Data Engineer
    
    This portfolio project showcases the intersection of public health domain expertise and modern data engineering practices.
    """)
    
    # Contact section
    st.markdown("### 📬 Connect With Me")
    
    st.markdown("""
    
    <div style='display: flex; gap: 20px; align-items: center; margin-top: 10px;'>
        <a href='https://github.com/thedbcooper' target='_blank' title='GitHub' class='profile-link'>
            <img src='https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png' width='50' height='50' style='filter: invert(1);'/>
        </a>
        <a href='https://www.linkedin.com/in/danielblakecooper/' target='_blank' title='LinkedIn' class='profile-link'>
            <img src='https://cdn-icons-png.flaticon.com/512/174/174857.png' width='50' height='50'/>
        </a>
        <a href='https://orcid.org/0000-0002-2218-7916' target='_blank' title='ORCID' class='profile-link'>
            <img src='https://orcid.org/assets/vectors/orcid.logo.icon.svg' width='50' height='50'/>
        </a>
    </div>
    """, unsafe_allow_html=True)
