import streamlit as st
import os
import io
import requests
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# --- CONFIGURATION ---
st.set_page_config(page_title="Lab Data Admin", layout="wide")
load_dotenv()

# ==========================================
# 🔒 SECURITY GATE
# ==========================================
def check_password():
    """Returns `True` if the user had the correct password."""
    
    # 1. Check Session State (Already logged in?)
    if st.session_state.get("password_correct", False):
        return True

    # 2. Get the Secret Password (Supports Local .env OR Cloud Secrets)
    # Priority: Streamlit Secrets -> Environment Variable -> Default
    try:
        stored_password = st.secrets["ADMIN_PASSWORD"]
    except (FileNotFoundError, KeyError):
        stored_password = os.getenv("ADMIN_PASSWORD")

    if not stored_password:
        st.error("⚠️ Server Configuration Error: ADMIN_PASSWORD not set.")
        st.stop()

    # 3. Show Login Form
    st.header("🔒 Admin Access Required")
    st.write("Please log in to manage the Lab Data Pipeline.")
    
    input_password = st.text_input("Enter Admin Password", type="password")
    
    if st.button("Log In"):
        if input_password == stored_password:
            st.session_state.password_correct = True
            st.rerun() # Refresh to show the app
        else:
            st.error("❌ Incorrect Password")
            
    return False

# 🛑 STOP HERE if not logged in
if not check_password():
    st.stop()

# ==========================================
# 🚀 MAIN APP (Only runs after Login)
# ==========================================

# Azure Config
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_OWNER = os.getenv("REPO_OWNER")
REPO_NAME = os.getenv("REPO_NAME")

# --- SESSION STATE ---
if "staged_fixes" not in st.session_state:
    st.session_state.staged_fixes = []
if "upload_counter" not in st.session_state:
    st.session_state.upload_counter = 0
if "upload_success" not in st.session_state:
    st.session_state.upload_success = False

# --- AZURE CONNECTION ---
@st.cache_resource
def get_blob_service():
    credential = DefaultAzureCredential()
    return BlobServiceClient(ACCOUNT_URL, credential=credential)

try:
    blob_service = get_blob_service()
    landing_client = blob_service.get_container_client("landing-zone")
    quarantine_client = blob_service.get_container_client("quarantine")
    data_client = blob_service.get_container_client("data")
    logs_client = blob_service.get_container_client("logs")
except Exception as e:
    st.error(f"Failed to connect to Azure: {e}")
    st.stop()

# ==========================================
# SIDEBAR: NAVIGATION & CONTROLS
# ==========================================
with st.sidebar:
    st.header("🧬 Lab Data Admin")
    st.caption(f"Storage: `{ACCOUNT_NAME}`")
    
    page = st.radio(
        "Go to:", 
        ["🏠 Start Here", "📤 Review & Upload", "📦 Landing Zone", "🛠️ Fix Quarantine", "📊 Final Report", "📈 Execution Logs"],
        key="nav_selection"
    )
    
    st.divider()
    st.subheader("🤖 Robot Controls")
    
    # Add a refresh button to check latest run status
    col_trigger, col_status = st.columns([1, 1])
    
    with col_trigger:
        trigger_clicked = st.button("▶️ Trigger Weekly Pipeline", use_container_width=True)
    
    with col_status:
        check_status_clicked = st.button("📊 Check Latest Run", use_container_width=True)
    
    if trigger_clicked:
        if not GITHUB_TOKEN or not REPO_OWNER:
            st.error("❌ Missing GitHub credentials in .env")
        else:
            with st.status("🚀 Triggering Cloud Pipeline...", expanded=True) as status:
                url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/workflows/weekly_pipeline.yaml/dispatches"
                headers = {
                    "Authorization": f"Bearer {GITHUB_TOKEN}",
                    "Accept": "application/vnd.github.v3+json"
                }
                data = {"ref": "main"} 

                try:
                    response = requests.post(url, json=data, headers=headers)
                    if response.status_code == 204:
                        status.update(label="✅ Pipeline Triggered Successfully!", state="complete", expanded=True)
                        st.success("🎯 **Pipeline workflow has been queued**")
                        st.info("📊 The pipeline will:\n"
                                "- Process files from landing zone\n"
                                "- Validate data against schema\n"
                                "- Quarantine invalid rows\n"
                                "- Remove tombstoned records (sample_status='remove')\n"
                                "- Upsert valid data into partitioned storage")
                        st.markdown(f"### 👉 [View Real-Time Progress on GitHub →](https://github.com/{REPO_OWNER}/{REPO_NAME}/actions)")
                        st.caption("⏱️ Check the Actions tab to see processing status, logs, and any errors.")
                    else:
                        status.update(label="❌ Failed to Trigger", state="error", expanded=True)
                        st.error(f"**HTTP {response.status_code}**")
                        with st.expander("📄 Response Details"):
                            st.code(response.text, language="json")
                except Exception as e:
                    status.update(label="❌ Connection Error", state="error", expanded=True)
                    st.error(f"**Failed to connect to GitHub API**")
                    st.exception(e)
    
    # Check latest workflow run status
    if check_status_clicked:
        if not GITHUB_TOKEN or not REPO_OWNER:
            st.error("❌ Missing GitHub credentials in .env")
        else:
            with st.status("📊 Fetching Latest Pipeline Run...", expanded=True) as status:
                try:
                    # Get latest workflow runs
                    runs_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/workflows/weekly_pipeline.yaml/runs"
                    headers = {
                        "Authorization": f"Bearer {GITHUB_TOKEN}",
                        "Accept": "application/vnd.github.v3+json"
                    }
                    
                    response = requests.get(runs_url, headers=headers, params={"per_page": 1})
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if data.get("total_count", 0) == 0:
                            status.update(label="ℹ️ No Pipeline Runs Found", state="complete", expanded=True)
                            st.info("No workflow runs found. Trigger the pipeline to see results here.")
                        else:
                            run = data["workflow_runs"][0]
                            run_status = run["status"]
                            run_conclusion = run.get("conclusion")
                            run_id = run["id"]
                            created_at = run["created_at"]
                            updated_at = run["updated_at"]
                            
                            # Update status based on run state
                            if run_status == "completed":
                                if run_conclusion == "success":
                                    status.update(label="✅ Latest Run: Success", state="complete", expanded=True)
                                    st.success(f"**Pipeline completed successfully!**")
                                    
                                    st.caption(f"🕐 Started: {created_at}")
                                    st.caption(f"✓ Completed: {updated_at}")
                                    st.info("📄 View detailed logs and metrics on GitHub Actions")
                                    
                                elif run_conclusion == "failure":
                                    status.update(label="❌ Latest Run: Failed", state="error", expanded=True)
                                    st.error("**Pipeline failed!** Check the logs for details.")
                                else:
                                    status.update(label=f"⚠️ Latest Run: {run_conclusion}", state="complete", expanded=True)
                                    st.warning(f"Pipeline ended with status: {run_conclusion}")
                            elif run_status == "in_progress":
                                status.update(label="🔄 Pipeline Running...", state="running", expanded=True)
                                st.info("**Pipeline is currently running**")
                                st.caption(f"🕐 Started: {created_at}")
                            else:
                                status.update(label=f"ℹ️ Status: {run_status}", state="complete", expanded=True)
                                st.info(f"Current status: {run_status}")
                            
                            st.markdown(f"### [📋 View Full Logs on GitHub →](https://github.com/{REPO_OWNER}/{REPO_NAME}/actions/runs/{run_id})")
                            
                    else:
                        status.update(label="❌ Failed to Fetch Status", state="error", expanded=True)
                        st.error(f"**HTTP {response.status_code}**")
                        st.code(response.text, language="json")
                        
                except Exception as e:
                    status.update(label="❌ Error", state="error", expanded=True)
                    st.error("**Failed to fetch workflow status**")
                    st.exception(e)
                    
    # LOGOUT BUTTON
    st.divider()
    if st.button("Log Out"):
        st.session_state.password_correct = False
        st.rerun()

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
        st.markdown("Drag & drop raw CSVs to the **Landing Zone**.")
        st.info("📍 *Tab: 'Review & Upload'*")

    with col2:
        st.markdown("### 2. Processing")
        st.markdown("The robot wakes up, validates schema, and merges data.")
        st.warning("""
        **How to run it:**
        * **Batch Scheduling:** Auto-runs weekly (Cron Job).
        * **Ad-Hoc:** Click **▶️ Trigger Weekly Pipeline** in the sidebar.
        """)

    with col3:
        st.markdown("### 3. Master Report")
        st.markdown("Valid data is upserted into the CDC Export.")
        st.success("📍 *Tab: 'Final Report'*")

    st.divider()

    # --- WORKFLOW 2: THE EXCEPTION PATH ---
    st.subheader("🔴 Workflow B: Error Resolution")
    st.caption("What happens when the robot rejects a file.")

    

    q_col1, q_col2, q_col3 = st.columns(3)

    with q_col1:
        st.markdown("### 1. Alert")
        st.markdown("Files with errors (e.g. 'Positive' instead of 'POS') are **Quarantined**.")
        st.error("📍 *Tab: 'Fix Quarantine'*")

    with q_col2:
        st.markdown("### 2. Human Review")
        st.markdown("An admin corrects the specific cell using the Excel-like editor.")
        st.caption("✍️ *Manual Fix*")

    with q_col3:
        st.markdown("### 3. Re-Integration")
        st.markdown("The fixed file is promoted back to the Upload queue for the next run.")
        st.info("📍 *Click 'Stage for Upload'*")

    st.divider()
    
    # CALL TO ACTION
    st.success("### 🚀 Ready to begin?")
    st.markdown("Head over to the **📤 Review & Upload** tab to start processing new batches.")

# ==========================================
# PAGE 1: UPLOAD (Final Review)
# ==========================================
if page == "📤 Review & Upload":
    st.title("📤 Final Review & Upload")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1. New Files (From Disk)")
        uploaded_files = st.file_uploader(
            "Drag & Drop CSVs", 
            type="csv", 
            accept_multiple_files=True,
            key=f"file_uploader_{st.session_state.upload_counter}"
        )

    with col2:
        st.subheader("2. Fixed Files (From Quarantine)")
        if st.session_state.staged_fixes:
            for item in st.session_state.staged_fixes:
                st.text(f"📄 {item['original_name']} ({len(item['dataframe'])} rows)")
        else:
            st.info("No fixed files waiting.")

    st.divider()
    
    # UPLOAD BUTTON AT TOP
    total_new = len(uploaded_files) if uploaded_files else 0
    total_fixed = len(st.session_state.staged_fixes)
    
    if total_new + total_fixed > 0:
        if st.button(f"🚀 Upload All ({total_new + total_fixed} files) to Cloud", type="primary"):
            progress_bar = st.progress(0)
            current_step = 0
            total_steps = total_new + total_fixed
            
            # A. Process New Files
            if uploaded_files:
                for up_file in uploaded_files:
                    try:
                        landing_client.upload_blob(name=up_file.name, data=up_file, overwrite=True)
                        st.write(f"✅ Uploaded `{up_file.name}`")
                    except Exception as e:
                        st.error(f"❌ Failed `{up_file.name}`: {e}")
                    
                    current_step += 1
                    progress_bar.progress(current_step / total_steps)

            # B. Process Fixed Files
            if st.session_state.staged_fixes:
                for item in st.session_state.staged_fixes:
                    fname = item['original_name']
                    df = item['dataframe']
                    
                    try:
                        csv_buffer = df.to_csv(index=False)
                        landing_client.upload_blob(name=fname, data=csv_buffer, overwrite=True)
                        st.write(f"✅ Promoted `{fname}`")
                        
                        q_blob = quarantine_client.get_blob_client(fname)
                        q_blob.delete_blob()
                        
                    except Exception as e:
                        st.error(f"❌ Failed to promote `{fname}`: {e}")
                    
                    current_step += 1
                    progress_bar.progress(current_step / total_steps)
                
                st.session_state.staged_fixes = []
            
            # Increment counter to clear the uploader on rerun
            st.session_state.upload_counter += 1
            st.session_state.upload_success = True
            st.rerun()
    
    # Show success message after rerun
    if st.session_state.upload_success:
        st.success("✨ Done! All files uploaded to Landing Zone. Be sure to trigger the pipeline from the sidebar or wait for automatic runs.")
        st.balloons()
        st.session_state.upload_success = False
    
    st.divider()
    
    # PREVIEW SECTION
    preview_options = []
    
    # Add new files to preview options
    if uploaded_files:
        for f in uploaded_files:
            preview_options.append(("New: " + f.name, f))
    
    # Add fixed files to preview options
    if st.session_state.staged_fixes:
        for item in st.session_state.staged_fixes:
            preview_options.append(("Fixed: " + item['original_name'], item['dataframe']))
    
    if preview_options:
        st.subheader("📋 File Preview")
        preview_choice = st.selectbox(
            "Select file to preview:", 
            [opt[0] for opt in preview_options]
        )
        
        if preview_choice:
            # Find the selected file
            selected_data = next(opt[1] for opt in preview_options if opt[0] == preview_choice)
            
            try:
                if isinstance(selected_data, pd.DataFrame):
                    # It's a fixed file (already a DataFrame)
                    df_preview = selected_data.head(10)
                else:
                    # It's a new file (file object)
                    df_preview = pd.read_csv(selected_data, nrows=10)
                
                st.caption(f"Showing first 10 rows of **{preview_choice}**")
                st.dataframe(df_preview, width="stretch")
            except Exception as e:
                st.error(f"Error reading file: {e}")
    else:
        st.caption("Waiting for files...")

# ==========================================
# PAGE 2: LANDING ZONE PREVIEW
# ==========================================
elif page == "📦 Landing Zone":
    st.title("📦 Landing Zone Preview")
    st.caption("Files waiting to be processed by the pipeline")
    
    try:
        blob_list = list(landing_client.list_blobs())
        
        if not blob_list:
            st.info("📭 Landing Zone is empty. Upload files in the 'Review & Upload' tab.")
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
                        df_preview = pd.read_csv(io.BytesIO(data), nrows=10)
                        st.caption(f"Showing first 10 rows of **{selected_blob_name}**")
                        st.dataframe(df_preview, width="stretch")
                    except Exception as e:
                        st.error(f"Error reading file: {e}")
    
    except Exception as e:
        st.error(f"Failed to access Landing Zone: {e}")

# ==========================================
# PAGE 3: FIX QUARANTINE
# ==========================================
elif page == "🛠️ Fix Quarantine":
    st.title("🛠️ Quarantine Manager")
    
    blob_list = list(quarantine_client.list_blobs())
    staged_names = [item['original_name'] for item in st.session_state.staged_fixes]
    remaining_blobs = [b.name for b in blob_list if b.name not in staged_names]
    
    if not remaining_blobs:
        if staged_names:
            st.info("⚠️ Files are staged in the Upload tab! Go there to finish.")
        else:
            st.success("🎉 Quarantine is empty!")
    else:
        selected_file = st.selectbox(
            "Select a file to fix:", 
            remaining_blobs,
            index=0,
            key=f"quarantine_selector_{len(staged_names)}"
        )

        if selected_file:
            blob_client = quarantine_client.get_blob_client(selected_file)
            stream = blob_client.download_blob().readall()
            df = pd.read_csv(io.BytesIO(stream), dtype=str)
            
            # Show column info
            st.caption(f"📋 Columns: {', '.join(df.columns.tolist())}")

            if "pipeline_error" in df.columns:
                unique_errors = df["pipeline_error"].unique()
                st.warning(f"Reported Errors: {', '.join(str(e) for e in unique_errors)}")

            st.write("👇 **Double-click cells to edit:**")
            edited_df = st.data_editor(df, num_rows="dynamic", width="stretch", key=f"editor_{selected_file}")

            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Stage for Upload", width="stretch"):
                    cols_to_drop = ["pipeline_error", "source_file"]
                    final_df = edited_df.drop(columns=[c for c in cols_to_drop if c in edited_df.columns])
                    
                    st.session_state.staged_fixes.append({
                        "original_name": selected_file,
                        "dataframe": final_df,
                        "status": "Ready"
                    })
                    
                    st.toast(f"Moved `{selected_file}` to Upload Tab!")
                    st.rerun()
            
            with col2:
                if st.button("🗑️ Delete File", type="secondary", width="stretch"):
                    try:
                        blob_client = quarantine_client.get_blob_client(selected_file)
                        blob_client.delete_blob()
                        st.toast(f"Deleted `{selected_file}` from quarantine")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to delete: {e}")

# ==========================================
# PAGE 4: FINAL REPORT
# ==========================================
elif page == "📊 Final Report":
    st.title("📊 CDC Final Export Review")
    
    blob_name = "final_cdc_export.csv"
    blob_client = data_client.get_blob_client(blob_name)
    
    if not blob_client.exists():
        st.warning("⚠️ No report found. Run the pipeline first!")
    else:
        props = blob_client.get_blob_properties()
        file_size_mb = props.size / (1024 * 1024)
        last_modified = props.last_modified.strftime('%Y-%m-%d %H:%M:%S')
        
        st.info(f"📅 Last Generated: **{last_modified}** | 📦 Size: **{file_size_mb:.2f} MB**")

        col1, col2 = st.columns(2)

        # PREVIEW ACTION
        with col1:
            if st.button("👁️ Preview (Top 1,000 Rows)"):
                try:
                    stream = blob_client.download_blob()
                    preview_df = pd.read_csv(io.BytesIO(stream.readall()), nrows=1000)
                    st.session_state.preview_df = preview_df
                except Exception as e:
                    st.error(f"Preview failed: {e}")

        # DOWNLOAD ACTION
        with col2:
            if st.button("📥 Prepare Full Download"):
                with st.spinner("Downloading full file from Cloud..."):
                    full_data = blob_client.download_blob().readall()
                    st.session_state.full_download = full_data
                    st.success("Ready!")

        # RESULTS
        if "preview_df" in st.session_state:
            st.divider()
            st.subheader("Data Preview")
            st.dataframe(st.session_state.preview_df, width="stretch")
            st.caption(f"Showing first {len(st.session_state.preview_df)} rows.")

        if "full_download" in st.session_state:
            st.download_button(
                label="💾 Save CSV to Disk",
                data=st.session_state.full_download,
                file_name="final_cdc_export.csv",
                mime="text/csv",
            )

# ==========================================
# PAGE 5: EXECUTION LOGS
# ==========================================
elif page == "📈 Execution Logs":
    st.title("📈 Pipeline Execution History")
    
    st.info("""
        This page shows the execution history of the data processing pipeline.
        Each log entry captures metrics from a single pipeline run.
    """)
    
    # List all log files
    try:
        log_blobs = list(logs_client.list_blobs())
        
        if not log_blobs:
            st.warning("📭 No execution logs found. Run the pipeline to generate logs.")
        else:
            st.success(f"Found {len(log_blobs)} execution log(s)")
            
            # Load all logs into a single dataframe
            all_logs = []
            for blob in sorted(log_blobs, key=lambda x: x.name, reverse=True):  # Most recent first
                try:
                    blob_client = logs_client.get_blob_client(blob.name)
                    log_data = blob_client.download_blob().readall()
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
                    
                    st.subheader("Latest Pipeline Run")
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
                st.divider()
                st.subheader("Execution History")
                
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