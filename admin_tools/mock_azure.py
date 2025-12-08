import streamlit as st
import copy
from datetime import datetime

# --- 1. THE GOLDEN IMAGE ---
INITIAL_STATE = {
    "landing-zone": {},
    "quarantine": {
        "quarantine_demo.csv": (
            b"sample_id,test_date,result,viral_load,pipeline_error,source_file\n"
            # CHANGE: ID is now TEST-999 to avoid collision with History
            b"TEST-999,2025-12-05,Positive,8000,\"Value error, Invalid result code...\",demo_upload.csv"
        )
    },
    "data": {
        "final_cdc_export.csv": (
            b"sample_id,test_date,result,viral_load\n"
            b"TEST-001,2025-12-01,POS,5000\n"
            b"TEST-002,2025-12-02,NEG,0"
        )
    }
}

def reset_mock_cloud():
    st.session_state.mock_cloud = copy.deepcopy(INITIAL_STATE)

def ensure_mock_cloud():
    if "mock_cloud" not in st.session_state:
        reset_mock_cloud()

# --- 2. MOCK CLASSES ---
class MockStreamDownloader:
    def __init__(self, data):
        self._data = data
    def readall(self):
        return self._data

class MockBlobProperties:
    def __init__(self, name, size=1024):
        self.name = name
        self.size = size
        self.last_modified = datetime.now()

class MockBlobClient:
    def __init__(self, container_name, blob_name):
        self.container = container_name
        self.name = blob_name

    def exists(self):
        ensure_mock_cloud()
        return self.name in st.session_state.mock_cloud.get(self.container, {})

    def download_blob(self):
        ensure_mock_cloud()
        data = st.session_state.mock_cloud[self.container].get(self.name, b"")
        return MockStreamDownloader(data)

    def delete_blob(self):
        ensure_mock_cloud()
        if self.name in st.session_state.mock_cloud[self.container]:
            del st.session_state.mock_cloud[self.container][self.name]

    def upload_blob(self, data, overwrite=True):
        ensure_mock_cloud()
        if hasattr(data, "read"):
            content = data.read()
            if hasattr(data, "seek"): data.seek(0)
        else:
            content = data
        st.session_state.mock_cloud[self.container][self.name] = content

    def get_blob_properties(self):
        ensure_mock_cloud()
        data = st.session_state.mock_cloud[self.container].get(self.name, b"")
        return MockBlobProperties(self.name, size=len(data))

class MockContainerClient:
    def __init__(self, name):
        self.name = name
        ensure_mock_cloud()

    def list_blobs(self):
        ensure_mock_cloud()
        files = st.session_state.mock_cloud.get(self.name, {})
        return [MockBlobProperties(f, len(data)) for f, data in files.items()]

    def get_blob_client(self, blob):
        return MockBlobClient(self.name, blob)

    def upload_blob(self, name, data, overwrite=True):
        client = self.get_blob_client(name)
        client.upload_blob(data, overwrite)