# inspect_data.py

import pandas as pd
from pathlib import Path
import duckdb # ⬅️ New import

# Define the project root based on your structure
PROJECT_ROOT = Path(__file__).resolve().parent

# Define layer directories and the DuckDB path
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
DUCKDB_PATH = PROJECT_ROOT / "data" / "analytics.duckdb" # Path to your DuckDB file

def inspect_parquet(file_path, num_rows=5):
    """Loads a Parquet file and prints structure and head."""
    if not file_path.exists():
        print(f"🛑 Error: File not found at {file_path}")
        return

    print(f"\n--- 📂 Inspecting: {file_path.name} ---")
    df = pd.read_parquet(file_path)
    
    print(f"Total Rows: {len(df)}")
    print(f"Columns and Data Types:")
    print(df.dtypes)
    print(f"\nFirst {num_rows} Rows:")
    print(df.head(num_rows).to_markdown(index=False)) 
    print("-" * 50)


def run_sql_queries():
    """Connects to DuckDB and runs verification queries on Silver/Gold tables."""
    print("\n\n=== 🔎 SQL Verification Queries (Via DuckDB) ===")
    
    try:
        # Connect to DuckDB (needed because the Silver layer created the DB file)
        con = duckdb.connect(database=str(DUCKDB_PATH), read_only=True)
    except Exception as e:
        print(f"🛑 Error connecting to DuckDB: {e}")
        return

    # Define paths for direct querying
    SILVER_FILE = SILVER_DIR / "fct_orders_tickets.parquet"
    TICKETS_MART_FILE = GOLD_DIR / "order_ticket_count_mart.parquet"
    
    
    # --- QUERY 1: Cross-Checking AOV against Silver Layer (USING THE SILVER PARQUET FILE) ---
    print("\n--- QUERY 1: Cross-Checking AOV against Silver Layer ---")
    aov_check_query = f"""
    SELECT 
        AVG(order_total) 
    FROM (
        -- 🔑 Query the Silver Parquet file directly
        SELECT DISTINCT order_id, order_total 
        FROM '{SILVER_FILE}'
    );
    """ # Note: We use an f-string and the path variable here
    aov_result = con.execute(aov_check_query).fetchdf()
    print(aov_result.to_markdown(index=False))
    
    
    # --- QUERY 2: Verify Maximum Tickets per Order (USING THE GOLD PARQUET FILE) ---
    print("\n--- QUERY 2: Find Max Tickets per Single Order (Using Gold Mart) ---")
    max_tickets_query = f"""
    SELECT 
        order_id, 
        num_tickets 
    FROM '{TICKETS_MART_FILE}' -- 🔑 Query the Gold Parquet file directly
    ORDER BY num_tickets DESC 
    LIMIT 1;
    """
    max_tickets_result = con.execute(max_tickets_query).fetchdf()
    print(max_tickets_result.to_markdown(index=False))
    
    
    # --- QUERY 3: Check Data Quality (Orders without Revenue Data) ---
    print("\n--- QUERY 3: Orders with Missing Revenue Data (Data Quality Check) ---")
    null_check_query = f"""
    SELECT 
        COUNT(order_id) AS orders_with_null_revenue
    FROM '{SILVER_FILE}' 
    WHERE order_total IS NULL;
    """
    null_result = con.execute(null_check_query).fetchdf()
    print(null_result.to_markdown(index=False))

    con.close()
    print("\nVerification complete.")
    

if __name__ == "__main__":
    # Ensure all pipeline assets have been materialized using 'dagster dev' before running this script!

    # --- INSPECT PARQUET FILES ---
    inspect_parquet(SILVER_DIR / "fct_orders_tickets.parquet")
    inspect_parquet(GOLD_DIR / "aov_mart.parquet")
    inspect_parquet(GOLD_DIR / "order_ticket_count_mart.parquet")
    
    # --- RUN SQL VERIFICATION ---
    run_sql_queries()