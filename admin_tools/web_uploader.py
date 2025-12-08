import streamlit as st
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load the same .env file you use for everything else
load_dotenv()

# Use your existing environment variables
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

st.title("🧬 Lab Data Uploader")
st.write(f"Connected to: **{ACCOUNT_NAME}**")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    if st.button("Upload to Cloud"):
        try:
            # Authenticate using the same robot credentials as your pipeline
            credential = DefaultAzureCredential()
            blob_service_client = BlobServiceClient(ACCOUNT_URL, credential=credential)
            
            # Get the container (landing-zone)
            blob_client = blob_service_client.get_blob_client(container="landing-zone", blob=uploaded_file.name)
            
            # Upload
            blob_client.upload_blob(uploaded_file, overwrite=True)
            st.success(f"✅ Successfully uploaded `{uploaded_file.name}` to Landing Zone!")
            
        except Exception as e:
            st.error(f"❌ Upload failed: {e}")
            st.write("Check your terminal for detailed Azure identity errors.")