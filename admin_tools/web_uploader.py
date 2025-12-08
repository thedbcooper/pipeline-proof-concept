import streamlit as st
import os
import io
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# --- CONFIGURATION ---
st.set_page_config(page_title="Lab Data Admin", layout="wide")
load_dotenv()

ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# --- SESSION STATE SETUP ---
# This acts as our "Shopping Cart" for fixed files
if "staged_fixes" not in st.session_state:
    st.session_state.staged_fixes = []

# --- AZURE CONNECTION HELPER ---
@st.cache_resource
def get_blob_service():
    credential = DefaultAzureCredential()
    return BlobServiceClient(ACCOUNT_URL, credential=credential)

blob_service = get_blob_service()
landing_client = blob_service.get_container_client("landing-zone")
quarantine_client = blob_service.get_container_client("quarantine")

st.title("🧬 Lab Data Admin Console")
st.caption(f"Connected to: `{ACCOUNT_NAME}`")

# --- TABS ---
tab_upload, tab_fix = st.tabs(["📤 Review & Upload", "🛠️ Fix Quarantine"])

# ==========================================
# TAB 2: FIX QUARANTINE (The Editor)
# ==========================================
with tab_fix:
    st.header("Quarantine Manager")
    
    # 1. List Files
    blob_list = list(quarantine_client.list_blobs())
    
    # Filter out files we have already staged (so you don't fix them twice)
    staged_names = [item['original_name'] for item in st.session_state.staged_fixes]
    remaining_blobs = [b.name for b in blob_list if b.name not in staged_names]
    
    if not remaining_blobs:
        if staged_names:
            st.info("⚠️ You have files staged in the Upload tab! Go there to finish.")
        else:
            st.success("🎉 Quarantine is empty!")
    else:
        selected_file = st.selectbox("Select a file to fix:", remaining_blobs)

        if selected_file:
            # 2. Download
            blob_client = quarantine_client.get_blob_client(selected_file)
            stream = blob_client.download_blob().readall()
            df = pd.read_csv(io.BytesIO(stream), dtype=str)

            # Show Error
            if "pipeline_error" in df.columns:
                unique_errors = df["pipeline_error"].unique()
                st.warning(f"Errors: {', '.join(str(e) for e in unique_errors)}")

            # 3. Editor
            st.write("👇 **Edit Data:**")
            edited_df = st.data_editor(df, num_rows="dynamic", use_container_width=True)

            # 4. "Stage" Button
            if st.button("✅ Stage for Upload"):
                # Clean columns
                cols_to_drop = ["pipeline_error", "source_file"]
                final_df = edited_df.drop(columns=[c for c in cols_to_drop if c in edited_df.columns])
                
                # Add to Session State (The "Shopping Cart")
                st.session_state.staged_fixes.append({
                    "original_name": selected_file,
                    "dataframe": final_df,
                    "status": "Ready"
                })
                
                st.toast(f"Moved `{selected_file}` to Upload Tab!")
                st.rerun()

# ==========================================
# TAB 1: UPLOAD (The Final Review)
# ==========================================
with tab_upload:
    st.header("Final Review & Upload")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1. New Files (From Disk)")
        uploaded_files = st.file_uploader("Drag & Drop CSVs", type="csv", accept_multiple_files=True)

    with col2:
        st.subheader("2. Fixed Files (From Quarantine)")
        if st.session_state.staged_fixes:
            for i, item in enumerate(st.session_state.staged_fixes):
                st.text(f"📄 {item['original_name']} ({len(item['dataframe'])} rows)")
        else:
            st.info("No fixed files waiting.")

    st.divider()
    
    # Calculate Total Actions
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
                        # 1. Upload to Landing Zone
                        csv_buffer = df.to_csv(index=False)
                        landing_client.upload_blob(name=fname, data=csv_buffer, overwrite=True)
                        st.write(f"✅ Promoted `{fname}`")
                        
                        # 2. Delete from Quarantine (ONLY happens if upload succeeds)
                        q_blob = quarantine_client.get_blob_client(fname)
                        q_blob.delete_blob()
                        
                    except Exception as e:
                        st.error(f"❌ Failed to promote `{fname}`: {e}")
                    
                    current_step += 1
                    progress_bar.progress(current_step / total_steps)
                
                # Clear the "Shopping Cart"
                st.session_state.staged_fixes = []
            
            st.success("✨ All operations complete!")
            st.balloons()
            
    else:
        st.write("waiting for files...")