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

# ==========================================
# ☁️ MOCK CLIENT INITIALIZATION
# ==========================================
@st.cache_resource
def get_mock_clients():
    return (
        MockContainerClient("landing-zone"),
        MockContainerClient("quarantine"),
        MockContainerClient("data")
    )

landing_client, quarantine_client, data_client = get_mock_clients()

# ==========================================
# 🤖 MINI-PIPELINE
# ==========================================
def run_mock_pipeline():
    log = []
    blobs = landing_client.list_blobs()
    if not blobs:
        return "📭 No new files in Landing Zone."
    
    log.append(f"Found {len(blobs)} new files to process.")

    report_blob = data_client.get_blob_client("final_cdc_export.csv")
    if report_blob.exists():
        history_bytes = report_blob.download_blob().readall()
        history_df = pd.read_csv(io.BytesIO(history_bytes), dtype=str)
    else:
        history_df = pd.DataFrame(columns=["sample_id", "test_date", "result", "viral_load"])
    
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
        
        csv_buffer = error_df.to_csv(index=False).encode('utf-8')
        quarantine_client.upload_blob(filename, csv_buffer, overwrite=True)
        log.append(f"⚠️ Quarantined {len(all_error_rows)} rows to {filename}")

    # Handle good data - upsert into report
    if all_valid_rows:
        new_batch = pd.DataFrame(all_valid_rows)
        
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
    
    return "\n".join(log)

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
        ["🏠 Start Here", "📤 Review & Upload", "📦 Landing Zone", "🛠️ Fix Quarantine", "📊 Final Report"],
        key="nav_selection"
    )
    
    st.divider()
    st.subheader("🤖 Robot Controls")
    
    if st.button("▶️ Trigger Weekly Pipeline"):
        with st.status("🤖 Robot Status", expanded=True) as status:
            st.write("Waking up...")
            time.sleep(1)
            result_log = run_mock_pipeline()
            time.sleep(0.5)
            
            if "CRITICAL ERROR" in result_log or "Failed" in result_log:
                status.update(label="Pipeline Failed", state="error", expanded=True)
                st.error("Errors found.")
                st.code(result_log)
            elif "No new files" in result_log:
                status.update(label="Pipeline Idle", state="complete", expanded=False)
                st.warning(result_log)
            else:
                status.update(label="Success!", state="complete", expanded=False)
                st.success("Complete")
                st.code(result_log)
                if "preview_df" in st.session_state:
                    del st.session_state.preview_df

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
        * **Production:** Auto-runs weekly (Cron Job).
        * **Admin/Demo:** Click **▶️ Trigger Weekly Pipeline** in the sidebar.
        """)

    with col3:
        st.markdown("### 3. Master Report")
        st.markdown("Valid data is upserted into the CDC Export.")
        st.success("📍 *Tab: 'Final Report'*")

    st.divider()

    # --- WORKFLOW 2: THE EXCEPTION PATH ---
    st.subheader("🔴 Workflow B: Error Resolution (Start here for Demo)")
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
# PAGE 1: UPLOAD
# ==========================================
elif page == "📤 Review & Upload":
    st.title("📤 Final Review & Upload")
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("1. New Files")
        uploaded_files = st.file_uploader(
            "Drag & Drop CSVs", 
            type="csv", 
            accept_multiple_files=True,
            key=f"file_uploader_{st.session_state.upload_counter}"
        )
    with col2:
        st.subheader("2. Fixed Files")
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
        if st.button(f"🚀 Upload All ({total_new + total_fixed} files)", type="primary"):
            progress_bar = st.progress(0)
            current_step = 0
            total_steps = total_new + total_fixed
            
            if uploaded_files:
                for up_file in uploaded_files:
                    file_bytes = up_file.getvalue()
                    landing_client.upload_blob(name=up_file.name, data=file_bytes, overwrite=True)
                    st.write(f"✅ Uploaded `{up_file.name}`")
                    current_step += 1
                    progress_bar.progress(current_step / total_steps)

            if st.session_state.staged_fixes:
                for item in st.session_state.staged_fixes:
                    fname = item['original_name']
                    df = item['dataframe']
                    csv_bytes = df.to_csv(index=False).encode('utf-8')
                    landing_client.upload_blob(name=fname, data=csv_bytes, overwrite=True)
                    st.write(f"✅ Promoted `{fname}`")
                    q_blob = quarantine_client.get_blob_client(fname)
                    q_blob.delete_blob()
                    current_step += 1
                    progress_bar.progress(current_step / total_steps)
                st.session_state.staged_fixes = []
            
            st.success("✨ Done! All files uploaded to Landing Zone. Be sure to trigger the pipeline from the sidebar.")
            
            # Increment counter to clear the uploader on rerun
            st.session_state.upload_counter += 1
            st.rerun()
    
    st.divider()
    
    # PREVIEW SECTION
    preview_options = []
    preview_data = {}
    
    # Add new files to preview options
    if uploaded_files:
        for f in uploaded_files:
            preview_options.append((f"New: {f.name}", f))
    
    # Add fixed files to preview options
    if st.session_state.staged_fixes:
        for item in st.session_state.staged_fixes:
            preview_options.append((f"Fixed: {item['original_name']}", item['dataframe']))
    
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
                st.dataframe(df_preview, use_container_width=True)
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
                        if isinstance(data, str): data = data.encode('utf-8')
                        df_preview = pd.read_csv(io.BytesIO(data), nrows=10)
                        st.caption(f"Showing first 10 rows of **{selected_blob_name}**")
                        st.dataframe(df_preview, use_container_width=True)
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
            st.info("Files staged in Upload tab.")
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

            if "pipeline_error" in df.columns:
                errs = df["pipeline_error"].unique()
                st.warning(f"Errors: {', '.join(str(e) for e in errs)}")

            st.write("👇 **Double-click to edit:**")
            edited_df = st.data_editor(df, num_rows="dynamic", width="stretch")

            if st.button("✅ Stage for Upload"):
                clean_df = edited_df.drop(columns=["pipeline_error", "source_file"], errors='ignore')
                st.session_state.staged_fixes.append({
                    "original_name": sel, "dataframe": clean_df
                })
                st.toast("Moved to Upload Tab!")
                st.rerun()

# ==========================================
# PAGE 4: FINAL REPORT
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
                st.success("Ready!")

        if "preview_df" in st.session_state:
            st.divider()
            st.dataframe(st.session_state.preview_df, width="stretch")

        if "full_download" in st.session_state:
            st.download_button("💾 Save CSV", st.session_state.full_download, "final_cdc_export.csv", "text/csv")