# 🧬 Public Health Data Pipeline (Agile Lakehouse)

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)](https://www.python.org)
[![Azure](https://img.shields.io/badge/Cloud-Azure%20Blob-0078D4?logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com)
[![Streamlit](https://img.shields.io/badge/UI-Streamlit-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io)
[![Polars](https://img.shields.io/badge/Data-Polars-CD792C?logo=polars)](https://pola.rs)
[![Pydantic](https://img.shields.io/badge/Validation-Pydantic-E92063?logo=pydantic&logoColor=white)](https://docs.pydantic.dev)

An automated, serverless data pipeline designed to ingest, validate, and aggregate sensitive public health laboratory data. This project implements a **"Human-in-the-Loop"** architecture where invalid data is automatically quarantined, fixed via a UI, and re-injected into the pipeline without code changes.

### 🎮 **[Live Portfolio Demo](https://public-health-data-agile-pipeline.streamlit.app/)**

> **Note to Viewer:** The "Real App" runs locally and is not deployed. This live demo uses **Interface Abstraction** to simulate Azure Blob Storage in memory, ensuring no connection to real cloud infrastructure.

-----

## 🏗️ Architecture

1.  **Landing Zone:** Partners upload raw CSVs via the Admin Console.
2.  **The Robot (Pipeline):** A GitHub Action triggers the processing script.
      * **Validation:** Uses `Pydantic` to enforce strict schema (e.g., `viral_load` must be int).
      * **Routing:** Good data $\rightarrow$ Data Lake (Parquet). Bad data $\rightarrow$ Quarantine (CSV).
3.  **Quarantine Loop:** Admins review rejected files in the Streamlit UI, fix errors (e.g., "Positive" $\rightarrow$ "POS"), and promote them back to the Landing Zone.
4.  **Reporting:** Clean data is aggregated into a master CDC Export file.

-----

## 📂 Repository Structure

Based on your file directory:

```text
.
├── .github/workflows/
│   └── weekly_pipeline.yaml      # The Cron Job (Production Automation)
├── admin_tools/
│   ├── demo_app.py               # 🎮 THE DEMO APP (Public Portfolio Frontend)
│   ├── generate_and_upload_mock_data.py
│   ├── mock_azure.py             # Cloud Emulation Logic
│   └── web_uploader.py           # 🔒 THE REAL APP (Local Production Admin Console)
├── pipeline/
│   ├── process_data_cloud.py     # The Core ETL Logic (Polars + Pydantic)
│   └── export_report.py          # Generates the final CDC aggregate report
├── models.py                     # Pydantic Schema Definitions
└── README.md
```

-----

## 🌟 Key Features

### 1\. "Self-Healing" Data Quality

Most pipelines crash on bad data. This one **side-steps** it.

  * **Polars + Pydantic:** Used for fast validation and data integrity.
  * **Quarantine:** Invalid files move to `quarantine/` and wait for human review.
  * **Human-in-the-Loop:** The Admin Console provides an Excel-like editor to fix the typo and retry.

### 2\. Cloud Abstraction (Security Highlight)

To share this project publicly without exposing Azure credentials, I implemented a **Mock Object Pattern**:

  * **Interface Abstraction:** The `mock_azure.py` class perfectly mirrors the official Azure SDK methods (`upload_blob`, `download_blob`, `list_blobs`).
  * **Safety:** The Demo App injects these mock clients instead of real Azure clients, ensuring the full UI workflow runs safely in the browser's memory.

-----

## 🚀 How to Run

### Option A: The Portfolio Demo (Browser)

Simply visit the **[Live App](https://public-health-data-agile-pipeline.streamlit.app/)**. No setup required.

### Option B: The Real Production App (Local)

*Note: Requires active Azure Credentials in `.env`*

1.  **Install dependencies:**
    ```bash
    uv sync
    ```
2.  **Run the Admin Console:**
    ```bash
    uv run streamlit run admin_tools/web_uploader.py
    ```
3.  **Trigger the Pipeline:**
    Click the "Trigger Weekly Pipeline" button in the sidebar (requires GitHub Token) to process the files you upload.

-----

### 👨‍💻 Created by Daniel Cooper

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/danielblakecooper/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/thedbcooper)
[![ORCID](https://img.shields.io/badge/ORCID-0000--0002--2218--7916-A6CE39?style=for-the-badge&logo=orcid&logoColor=white)](https://orcid.org/0000-0002-2218-7916)

*Epidemiologist & Analytics Engineer*