# 🧬 Public Health Data Pipeline (Agile Lakehouse)

[](https://python.org)
[](https://azure.microsoft.com)
[](https://streamlit.io)
[](https://pola.rs)
[](https://www.google.com/search?q=https://docs.pydantic.dev/)

An automated, serverless data pipeline designed to ingest, validate, and aggregate sensitive public health laboratory data. This project implements a **"Human-in-the-Loop"** architecture where invalid data is automatically quarantined, fixed via a UI, and re-injected into the pipeline without code changes.

### 🎮 **[Live Portfolio Demo](https://public-health-data-agile-pipeline.streamlit.app/)**

> **Note:** This live demo runs in a secure "Sandbox Mode." It uses **Dependency Injection** to simulate Azure Blob Storage in-memory, ensuring no connection to real cloud infrastructure or sensitive data.

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

```text
.
├── .github/workflows/
│   └── weekly_pipeline.yaml      # The Cron Job (Monday 12:00 PM) & Manual Trigger
├── admin_tools/
│   ├── web_app.py                # 🔒 THE REAL APP (Local Production Admin Console)
│   ├── demo_portfolio_app.py     # 🎮 THE DEMO APP (Public, Mock-Cloud version)
│   └── mock_azure.py             # Cloud Emulation Logic (Interface Abstraction)
├── pipeline/
│   ├── process_data_cloud.py     # The Core ETL Logic (Polars + Pydantic)
│   └── export_report.py          # Generates the final CDC aggregate report
├── models.py                     # Pydantic Schema Definitions
├── requirements.txt
└── README.md
```

-----

## 🌟 Key Features

### 1\. "Self-Healing" Data Quality

Most pipelines crash on bad data. This one **side-steps** it.

  * If a file contains `viral_load: "High"`, the pipeline **does not fail**.
  * It moves that specific file to a `quarantine/` container and continues processing the rest.
  * The Admin Console provides an Excel-like editor to fix the typo and retry.

### 2\. High-Performance ETL

  * **Polars:** Used for blazing-fast data manipulation (replacing Pandas).
  * **Parquet:** Data is stored in compressed column-oriented format for efficiency.
  * **Pydantic:** Enforces strict type checking before data ever touches the Lakehouse.

### 3\. Secure Portfolio Deployment

To share this project publicly without exposing Azure credentials, I implemented a **Cloud Emulation Pattern**:

  * **Interface Abstraction:** Created a `MockContainerClient` class that mirrors the official Azure SDK methods (`upload_blob`, `download_blob`, `list_blobs`).
  * **Dependency Injection:** The Demo App injects these mock clients instead of real Azure clients, allowing the full UI workflow to run entirely in the browser's memory.

-----

## 🚀 How to Run

### Option A: The Portfolio Demo (Browser)

Simply visit the **[Live App](https://public-health-data-agile-pipeline.streamlit.app/)**. No setup required.

### Option B: The Real Production App (Local)

*Note: Requires active Azure Credentials in `.env`*

1.  **Clone the repo:**
    ```bash
    git clone https://github.com/yourusername/lab-data-pipeline.git
    cd lab-data-pipeline
    ```
2.  **Install dependencies:**
    ```bash
    uv sync  # Or pip install -r requirements.txt
    ```
3.  **Run the Admin Console:**
    ```bash
    uv run streamlit run admin_tools/web_app.py
    ```
4.  **Trigger the Pipeline:**
    Click the "Trigger Weekly Pipeline" button in the sidebar (requires GitHub Token).

-----

## 🛡️ Security & Design Decisions

  * **Local-First Admin:** The production Admin Console (`web_app.py`) is designed to run on a secure internal network or VPN, not on the public internet.
  * **Secrets Management:** Uses `python-dotenv` for local development and GitHub Secrets for CI/CD.
  * **Role-Based Access (RBAC):** The pipeline uses a specialized Azure Service Principal with `Storage Blob Data Contributor` scope, adhering to the Principle of Least Privilege.

-----

---

### 👨‍💻 Created by Daniel Cooper

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/danielblakecooper/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/thedbcooper)

*Epidemiologist & Analytics Engineer*