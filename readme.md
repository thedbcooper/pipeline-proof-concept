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

### Core Data Flow

1.  **Landing Zone:** User uploads raw CSVs via Streamlit Admin Console → `landing-zone` container.
2.  **Automated Processing (GitHub Actions):** `weekly_pipeline.yaml` triggers on schedule or manual dispatch.
      * **Validation:** `Pydantic` enforces strict schema (sample_id, test_date, result, viral_load).
      * **Routing:** Valid data → `data` container (partitioned Parquet by week). Invalid data → `quarantine` container (CSV).
      * **Logging:** Emoji-rich processing logs with detailed metrics saved to `logs` container as `execution_TIMESTAMP.csv`.
3.  **Quarantine Resolution:** Admins review errors in UI, fix data (e.g., "Positive" → "POS"), reupload to `landing-zone` for automatic reprocessing.
4.  **Deletion Workflow:** Two-step process for permanent record removal:
      * Upload deletion request CSV (sample_id + test_date) → `deletion-requests` container
      * Trigger `delete_records.yaml` GitHub Action → removes from partitioned data
      * Logs deleted sample IDs to `logs` container as `deletion_TIMESTAMP.csv`
5.  **Reporting:** Aggregated clean data exported to `final_cdc_export.csv` with complete audit trail.

### Storage Containers

- **landing-zone**: Raw CSV uploads from partners
- **quarantine**: Invalid records awaiting manual review
- **data**: Validated records in partitioned Parquet format (year=YYYY/week=WW/)
- **logs**: Processing and deletion execution logs (CSV with processing_details)
- **deletion-requests**: Pending deletion requests (CSV with sample_id and test_date)

-----

## 📂 Repository Structure

```text
.
├── .github/workflows/
│   ├── weekly_pipeline.yaml      # Scheduled pipeline automation (production)
│   └── delete_records.yaml       # Manual deletion workflow trigger
├── admin_tools/
│   ├── demo_app.py               # 🎮 THE DEMO APP (Public Portfolio Frontend)
│   ├── web_uploader.py           # 🔒 THE REAL APP (Local Production Admin Console)
│   ├── mock_azure.py             # Cloud Emulation Logic for Demo
│   ├── fetch_errors.py           # Utility: Download quarantine files
│   ├── reingest_fixed_data.py    # Utility: Re-upload fixed data
│   ├── generate_and_upload_mock_data.py  # Utility: Generate test data
│   └── test_connection.py        # Utility: Test Azure connection
├── pipeline/
│   ├── process_data_cloud.py     # Core ETL Logic (Polars + Pydantic)
│   ├── export_report.py          # Generate final CDC aggregate report
│   └── delete_records.py         # Process deletion requests from CSV
├── models.py                     # Pydantic Schema Definitions
├── pyproject.toml                # Project dependencies (uv)
└── README.md
```

-----

## 🌟 Key Features

### 1\. "Self-Healing" Data Quality

Most pipelines crash on bad data. This one **side-steps** it.

  * **Polars + Pydantic:** Used for fast validation and data integrity.
  * **Quarantine:** Invalid files move to `quarantine/` and wait for human review.
  * **Human-in-the-Loop:** The Admin Console provides an Excel-like editor to fix the typo and retry.
  * **Detailed Logging:** Every pipeline run saves comprehensive logs with emoji-rich processing details for easy debugging.

### 2\. Data Deletion Workflow

A dedicated workflow for data corrections:

  * **Two-Step Process:** Upload deletion requests (CSV with sample_id and test_date), then trigger workflow.
  * **Partition-Aware:** Automatically calculates which partitions to check based on test dates.
  * **Audit Trail:** Logs which sample IDs were deleted from which partitions with full timestamp tracking.
  * **GitHub Actions Integration:** Secure, authenticated deletion via automated workflow.

### 3\. Cloud Abstraction (Security Highlight)

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

## 🚀 Deployment Prerequisites (Required for Real App & Pipeline)

To run the full production pipeline, you must establish a secure connection between **Azure Storage** and **GitHub Actions**.

### 1\. Azure Storage Setup 🟦

Create a Storage Account and the following private containers, which serve as the structure for your Data Lake:

| Container Name | Purpose |
| :--- | :--- |
| **landing-zone** | Receives raw uploaded CSVs from the Admin Console. |
| **quarantine** | Holds CSV files that failed Pydantic validation (for human review). |
| **data** | Stores the finalized, cleaned data in partitioned parquet files. |
| **logs** | Stores execution and deletion logs as CSV files for audit trail. |
| **deletion-requests** | Holds pending deletion request CSVs before processing. |

### 2\. Generating Secrets (Service Principal) 🔑

The GitHub Action needs a **Service Principal (SP)** to act as the "robot" with specific access rights. This is the most secure way to grant CI/CD access to your cloud resources.

Run the Azure CLI command below to generate the necessary credentials:

```bash
az ad sp create-for-rbac \
--name "GitHubPipelineRobot" \
--role "Storage Blob Data Contributor" \
--scopes /subscriptions/YOUR_SUBSCRIPTION_ID/resourceGroups/YOUR_RESOURCE_GROUP
```

This command will output three values that you must save:

  * `appId` (This is your **AZURE\_CLIENT\_ID**)
  * `password` (This is your **AZURE\_CLIENT\_SECRET**)
  * `tenant` (This is your **AZURE\_TENANT\_ID**)

### 3\. Adding Secrets to GitHub Actions 🐙

Navigate to your repository settings on GitHub (`Settings` $\rightarrow$ `Secrets and variables` $\rightarrow$ `Actions`). Add the following repository secrets based on the values you generated above:

| Secret Name | Source / Value | Used By |
| :--- | :--- | :--- |
| `AZURE_CLIENT_ID` | The `appId` value from the CLI. | GitHub Actions workflows |
| `AZURE_CLIENT_SECRET` | The `password` value from the CLI. | GitHub Actions workflows |
| `AZURE_TENANT_ID` | The `tenant` value from the CLI. | GitHub Actions workflows |
| `AZURE_STORAGE_ACCOUNT` | Your storage account name (e.g., `labdata01`). | GitHub Actions workflows |

### 4\. Configuring Local Environment (.env) 🔧

For the Streamlit apps to authenticate with Azure and trigger/monitor GitHub Actions, create a `.env` file in the project root with these variables:

```env
# Azure Authentication (same values as GitHub secrets)
AZURE_CLIENT_ID=<your_appId>
AZURE_CLIENT_SECRET=<your_password>
AZURE_TENANT_ID=<your_tenant>
AZURE_STORAGE_ACCOUNT=<your_storage_account_name>

# GitHub API Access (for triggering workflows from Streamlit)
GITHUB_TOKEN=<your_personal_access_token>
REPO_OWNER=<your_github_username>
REPO_NAME=<your_repository_name>
```

**Note:** The `GITHUB_TOKEN` must be a Personal Access Token (PAT) with `workflow` scope to trigger Actions and read run status.

| Secret Name     | Source / Value                                         | Used By                |
| :-------------- | :----------------------------------------------------- | :--------------------- |
| `GITHUB_TOKEN`  | A Fine-Grained PAT with **actions:write** scope.       | Workflow Trigger Buttons |
| `REPO_OWNER`    | Your GitHub username (e.g., `thedbcooper`).            | Workflow Trigger Buttons |
| `REPO_NAME`     | Your repository name (e.g., `pipeline-proof-concept`). | Workflow Trigger Buttons |

-----

### 👨‍💻 Created by Daniel Cooper

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/danielblakecooper/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/thedbcooper)
[![ORCID](https://img.shields.io/badge/ORCID-0000--0002--2218--7916-A6CE39?style=for-the-badge&logo=orcid&logoColor=white)](https://orcid.org/0000-0002-2218-7916)

*Epidemiologist & Analytics Engineer*