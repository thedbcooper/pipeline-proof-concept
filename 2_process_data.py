import duckdb
import pandas as pd
from pydantic import BaseModel, field_validator, ValidationError
from datetime import date, datetime
import os
import glob

# --- CONFIGURATION ---
LANDING_ZONE = "local_azure/landing_zone"
QUARANTINE_ZONE = "local_azure/quarantine"
DATA_ZONE = "local_azure/data"

os.makedirs(QUARANTINE_ZONE, exist_ok=True)
os.makedirs(DATA_ZONE, exist_ok=True)

# --- PYDANTIC MODEL ---
class LabResult(BaseModel):
    sample_id: str
    test_date: date
    result: str
    viral_load: int

    @field_validator('result')
    def check_result_code(cls, v):
        allowed = ['POS', 'NEG', 'N/A']
        if v not in allowed:
            raise ValueError(f"Invalid result code: '{v}'. Must be POS, NEG, or N/A")
        return v

# --- HELPER: GET WEEK FROM DATE ---
def get_partition_path(date_obj):
    # Returns "year=2025/week=42" based on the DATE of the sample
    y = date_obj.year
    w = date_obj.isocalendar()[1]
    return f"year={y}/week={w}"

def process_pipeline():
    # 1. Find Files
    csv_files = glob.glob(f"{LANDING_ZONE}/*.csv")
    if not csv_files:
        print("No new files found in landing zone.")
        return

    # 2. Validation Loop
    all_valid_rows = []
    all_error_rows = []
    
    for file in csv_files:
        print(f"Processing {file}...")
        # Read incoming CSV as string to protect leading zeros during validation
        df = pd.read_csv(file, dtype=str)
        
        for index, row in df.iterrows():
            try:
                # Validate row (Pydantic handles type conversion, e.g., string "500" -> int 500)
                valid_sample = LabResult(**row.to_dict())
                all_valid_rows.append(valid_sample.model_dump())
            
            except ValidationError as e:
                # --- CAPTURE THE BAD DATA ---
                bad_row = row.to_dict()
                bad_row['pipeline_error'] = str(e)
                bad_row['source_file'] = os.path.basename(file)
                all_error_rows.append(bad_row)

        # Cleanup raw file
        os.remove(file)

    # --- 3. HANDLE BAD DATA (QUARANTINE) ---
    if all_error_rows:
        error_df = pd.DataFrame(all_error_rows)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = f"{QUARANTINE_ZONE}/quarantine_{timestamp}.csv"
        error_df.to_csv(error_file, index=False)
        print(f"⚠️  {len(all_error_rows)} rows failed validation. Saved to: {error_file}")
    else:
        print("✅ No validation errors found.")

    if not all_valid_rows:
        print("No valid data to upsert.")
        return

    # --- 4. HANDLE GOOD DATA (UPSERT BY PARTITION - CSV MODE) ---
    
    full_df = pd.DataFrame(all_valid_rows)
    full_df['partition_path'] = full_df['test_date'].apply(get_partition_path)

    con = duckdb.connect()
    unique_partitions = full_df['partition_path'].unique()

    for part_path in unique_partitions:
        # Filter: Get only the new rows that belong to THIS week
        new_batch_df = full_df[full_df['partition_path'] == part_path].copy()
        
        target_dir = f"{DATA_ZONE}/{part_path}"
        # CHANGED: Target file is now .csv
        target_file = f"{target_dir}/data.csv"
        
        os.makedirs(target_dir, exist_ok=True)

        if os.path.exists(target_file):
            print(f"Found existing history for {part_path}. Merging...")
            
            # Upsert Logic:
            # CHANGED: Read existing CSV with 'all_varchar=True' to protect data types
            history_df = con.query(f"SELECT * FROM read_csv_auto('{target_file}', all_varchar=True)").to_df()
            
            con.register('history_table', history_df)
            con.register('new_batch_table', new_batch_df)
            
            # Note: We cast viral_load to INT in the SQL ensuring types match for union
            merged_df = con.query("""
                SELECT 
                    sample_id, 
                    CAST(test_date AS DATE) as test_date, 
                    result, 
                    CAST(viral_load AS INTEGER) as viral_load 
                FROM history_table 
                WHERE sample_id NOT IN (SELECT sample_id FROM new_batch_table)
                
                UNION ALL
                
                SELECT sample_id, test_date, result, viral_load FROM new_batch_table
            """).to_df()
            
            con.register('final_table', merged_df)
            
            # CHANGED: Write back as CSV
            con.sql(f"COPY final_table TO '{target_file}' (FORMAT CSV, HEADER)")
            
        else:
            print(f"No history for {part_path}. Creating new file...")
            new_batch_df = new_batch_df.drop(columns=['partition_path'])
            con.register('final_table', new_batch_df)
            
            # CHANGED: Write as CSV
            con.sql(f"COPY final_table TO '{target_file}' (FORMAT CSV, HEADER)")

    print("Pipeline Complete! Check local_azure/data for partitioned CSVs.")

if __name__ == "__main__":
    process_pipeline()