import duckdb
from dotenv import load_dotenv
import os

# Load credentials so DuckDB can see them
load_dotenv()
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")

# Initialize DuckDB
con = duckdb.connect()

# --- 1. SETUP AZURE CONNECTION ---
# We install the 'azure' extension which teaches DuckDB how to talk to Blob Storage
print("🔌 Configuring DuckDB for Azure...")
con.sql("INSTALL azure; LOAD azure;")

# We create a 'Secret' using the CREDENTIAL_CHAIN provider.
# This tells DuckDB: "Look for the AZURE_CLIENT_ID/SECRET env vars I already have."
con.sql(f"""
    CREATE SECRET (
        TYPE AZURE,
        PROVIDER CREDENTIAL_CHAIN,
        ACCOUNT_NAME '{ACCOUNT_NAME}'
    );
""")

# --- 2. THE MAGICAL QUERY ---
# Notice the path: azure://<account>.blob.core.windows.net/data/*/*/data.csv
# The wildcards (*/*) tell DuckDB to look in ALL year folders and ALL week folders.
print("📊 Querying all partitions across history...")

# Let's run a quick analysis first (Aggregation)
summary_df = con.sql(f"""
    SELECT 
        year,
        count(*) as total_samples,
        avg(viral_load)::int as avg_load
    FROM read_csv_auto('azure://{ACCOUNT_NAME}.blob.core.windows.net/data/*/*/data.csv', hive_partitioning=1)
    GROUP BY year
    ORDER BY year DESC
""").df()

print("\n--- DATA SUMMARY ---")
print(summary_df)
print("--------------------\n")

# --- 3. EXPORT SINGLE CSV (CDC REQUIREMENT) ---
output_filename = "final_cdc_export.csv"
print(f"📦 Generating cumulative export: {output_filename}...")

# We select EVERYTHING and write to a single local CSV
con.sql(f"""
    COPY (
        SELECT 
            sample_id,
            test_date,
            result,
            viral_load
        FROM read_csv_auto('azure://{ACCOUNT_NAME}.blob.core.windows.net/data/*/*/data.csv', hive_partitioning=1)
        ORDER BY test_date DESC
    ) TO '{output_filename}' (FORMAT CSV, HEADER)
""")

print("✅ Export Complete! You can now upload this file to the CDC.")