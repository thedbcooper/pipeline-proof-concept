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

# Azure Config
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_OWNER = os.getenv("REPO_OWNER")
REPO_NAME = os.getenv("REPO_NAME")

# --- SESSION STATE ---
if "staged_fixes" not in st.session_state:
    st.session_state.staged_fixes = []

# --- AZURE CONNECTION ---
@st.cache_resource
def get_blob_service():
    credential = DefaultAzureCredential()
    return BlobServiceClient(ACCOUNT_URL, credential=credential)

blob_service = get_blob_service()
landing_client = blob_service.get_container_client("landing-zone")
quarantine_client = blob_service.get_container_client("quarantine")
data_client = blob_service.get_container_client("data")

# ==========================================
# SIDEBAR: NAVIGATION & CONTROLS
# ==========================================
with st.sidebar:
    st.header("🧬 Lab Data Admin")
    st.caption(f"Storage: `{ACCOUNT_NAME}`")
    
    # NAVIGATION
    page = st.radio("Go to:", ["📤 Review & Upload", "🛠️ Fix Quarantine", "📊 Final Report"])
    
    st.divider()
    st.subheader("🤖 Robot Controls")
    
    if st.button("▶️ Trigger Weekly Pipeline"):
        if not GITHUB_TOKEN or not REPO_OWNER:
            st.error("❌ Missing GitHub credentials in .env")
        else:
            with st.spinner("Waking up the robot..."):
                url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/workflows/weekly_pipeline.yaml/dispatches"
                headers = {
                    "Authorization": f"Bearer {GITHUB_TOKEN}",
                    "Accept": "application/vnd.github.v3+json"
                }
                data = {"ref": "main"} 

                try:
                    response = requests.post(url, json=data, headers=headers)
                    if response.status_code == 204:
                        st.success("✅ Signal sent!")
                        st.markdown(f"👉 [Watch Progress](https://github.com/{REPO_OWNER}/{REPO_NAME}/actions)")
                    else:
                        st.error(f"❌ Failed: {response.status_code}")
                        st.caption(response.text)
                except Exception as e:
                    st.error(f"Connection Error: {e}")

# ==========================================
# PAGE 1: UPLOAD (Final Review)
# ==========================================
if page == "📤 Review & Upload":
    st.title("📤 Final Review & Upload")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1. New Files (From Disk)")
        uploaded_files = st.file_uploader("Drag & Drop CSVs", type="csv", accept_multiple_files=True)

    with col2:
        st.subheader("2. Fixed Files (From Quarantine)")
        if st.session_state.staged_fixes:
            for item in st.session_state.staged_fixes:
                st.text(f"📄 {item['original_name']} ({len(item['dataframe'])} rows)")
        else:
            st.info("No fixed files waiting.")

    st.divider()
    
    total_new = len(uploaded_files) if uploaded_files else 0
    total_fixed = len(st.session_state.staged_fixes)
    
    if total_new + total_fixed > 0:
        if st.button(f"🚀 Upload All ({total_new + total_fixed} files) to Cloud"):
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
            
            st.success("✨ All operations complete!")
            st.balloons()
    else:
        st.caption("Waiting for files to process...")

# ==========================================
# PAGE 2: FIX QUARANTINE
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
        selected_file = st.selectbox("Select a file to fix:", remaining_blobs)

        if selected_file:
            blob_client = quarantine_client.get_blob_client(selected_file)
            stream = blob_client.download_blob().readall()
            df = pd.read_csv(io.BytesIO(stream), dtype=str)

            if "pipeline_error" in df.columns:
                unique_errors = df["pipeline_error"].unique()
                st.warning(f"Reported Errors: {', '.join(str(e) for e in unique_errors)}")

            st.write("👇 **Double-click cells to edit:**")
            
            # CHANGE: Replaced use_container_width=True with width="stretch"
            edited_df = st.data_editor(df, num_rows="dynamic", width="stretch")

            if st.button("✅ Stage for Upload"):
                cols_to_drop = ["pipeline_error", "source_file"]
                final_df = edited_df.drop(columns=[c for c in cols_to_drop if c in edited_df.columns])
                
                st.session_state.staged_fixes.append({
                    "original_name": selected_file,
                    "dataframe": final_df,
                    "status": "Ready"
                })
                
                st.toast(f"Moved `{selected_file}` to Upload Tab!")
                st.rerun()

# ==========================================
# PAGE 3: FINAL REPORT
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
            
            # CHANGE: Replaced use_container_width=True with width="stretch"
            st.dataframe(st.session_state.preview_df, width="stretch")
            st.caption(f"Showing first {len(st.session_state.preview_df)} rows.")

        if "full_download" in st.session_state:
            st.download_button(
                label="💾 Save CSV to Disk",
                data=st.session_state.full_download,
                file_name="final_cdc_export.csv",
                mime="text/csv",
            )