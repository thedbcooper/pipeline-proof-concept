# Serverless Lab Data Lakehouse (Agile Informatics)

## 📋 Project Overview

This project implements a **Serverless Data Lakehouse** for automating the ingestion, validation, and reporting of public health laboratory data.

Unlike traditional "Schema-on-Write" relational databases (like SQL Server), this system utilizes an **Agile Informatics** approach. It decouples storage from compute, allowing for rapid iteration of data schemas and zero-cost maintenance during idle time.

### Key Features

  * **Zero-Cost Idle:** Uses Azure Blob Storage and GitHub Actions; costs are incurred only during the seconds of processing.
  * **Time Travel:** Automatically partitions data by `year` and `week` based on sample dates, not upload dates.
  * **Dead Letter Queue:** Robust "Human-in-the-Loop" workflow for repairing and re-ingesting bad data without stopping the pipeline.
  * **Schema-on-Read:** Uses Pydantic for flexible, code-defined validation that is easier to update than database tables.

-----

## 🏗 Architecture

1.  **Ingestion:** Lab results (CSV) arrive in **SharePoint**. Power Automate triggers and moves them to the Azure **Landing Zone**.
2.  **Processing (The Robot):** A GitHub Action triggers `1_process_data_cloud.py`.
      * Validates data using Pydantic models.
      * **Pass:** Merges data into the **Data Lake** (Parquet/CSV partition folders).
      * **Fail:** Offloads bad rows to the **Quarantine** container.
3.  **Reporting:** `2_export_report.py` queries the Data Lake (using client-side pruning) to generate longitudinal datasets for the CDC/Dashboarding.

-----

## 🛠 Project Structure

This system is split into two logical domains to separate production automation from administrative tools.

### 1\. The Production Pipeline (`/pipeline`)

*Runs automatically via GitHub Actions.*

  * `1_process_data_cloud.py`: The core ETL engine. Handles validation, deduplication, and upserting logic.
  * `2_export_report.py`: Generates aggregate reports by pulling specific partitions from the lake.
  * `models.py`: Shared Pydantic data definitions.
  * `.github/workflows/weekly_pipeline.yaml`: The scheduler configuration.

### 2\. Admin & Repair Tools (`/admin-tools`)

*Runs manually on a local analyst machine.*

  * `99_fetch_errors.py`: Downloads quarantined files for manual review.
  * `6_reingest_fixed_data.py`: Pre-validates fixed files locally before pushing them back to the Landing Zone.
  * `1_generate_mock_data.py`: Generates historical dummy data for stress testing.

-----

## 🚀 Usage Guide

### A. The Automated Workflow

The pipeline runs automatically every **Monday at 12:00 UTC**.
To trigger it manually:

1.  Go to the **Actions** tab in GitHub.
2.  Select **Weekly Lab Pipeline**.
3.  Click **Run workflow**.

### B. The "Dead Letter" Repair Loop

When the pipeline detects validation errors (e.g., typos like "Positive" instead of "POS"), follow this process:

1.  **Fetch:** Run the admin script to retrieve errors.
    ```bash
    python 99_fetch_errors.py
    ```
    *Files will appear in `fix_me_please/`.*
2.  **Fix:** Open the CSV in Excel. Correct the data in the row. **Save.**
3.  **Re-ingest:** Run the gatekeeper script.
    ```bash
    python 6_reingest_fixed_data.py
    ```
    *If the fix is valid, it automatically uploads to the Landing Zone and cleans up the Quarantine.*

-----

## ⚙️ Configuration

To run these scripts locally, create a `.env` file in the root directory:

```ini
# Service Principal Credentials
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
AZURE_TENANT_ID=your-tenant-id

# Storage Configuration
AZURE_STORAGE_ACCOUNT=yourstorageaccountname
```

**Note:** Never commit `.env` to GitHub. These secrets are managed via **GitHub Secrets** for the automated pipeline.

-----

## 🧠 Why "Agile Informatics"?

This architecture was chosen to solve specific challenges in public health surveillance:

1.  **Velocity vs. Perfection:** We prioritize the ability to ingest new data types immediately over strict database constraints.
2.  **Resilience:** If one file is corrupt, it is quarantined. The rest of the pipeline continues. In a traditional database pipeline, a single schema error often halts the entire batch.
3.  **Portability:** The data lives in open formats (CSV/Parquet). It can be consumed by Python, R, PowerBI, or Tableau without needing a JDBC driver or VPN connection.