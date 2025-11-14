import pandas as pd
from dagster import AssetIn, asset, get_dagster_logger
from pathlib import Path
import requests
import json 
from dotenv import load_dotenv
import os
from azure.storage.blob import ContainerClient
import duckdb

# Load environment variables outside the asset for immediate access during definition/pathing
load_dotenv()

# --- CORRECTED PATH LOGIC ---
# 1. Get the path to the current file (assets.py)
CURRENT_DIR = Path(__file__).parent 

# 2. Go up one level to the Project Root (where 'data' and 'raw_data' live)
PROJECT_ROOT = CURRENT_DIR.parent

# 3. Define the directories relative to the Project Root
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True) 

RAW_DATA_DIR = PROJECT_ROOT / "raw_data" # References raw_data folder
# 🔑 Define the path for the temporary DuckDB file (our transformation engine)
DUCKDB_PATH = PROJECT_ROOT / "data" / "analytics.duckdb"

# --- Continue with the RAW_FILES Definition ---
RAW_FILES = {
    "customers": RAW_DATA_DIR / "raw_customers.csv",
    "items": RAW_DATA_DIR / "raw_items.csv",
    "orders": RAW_DATA_DIR / "raw_orders.csv",
    "products": RAW_DATA_DIR / "raw_products.csv",
    "stores": RAW_DATA_DIR / "raw_stores.csv",
    "supplies": RAW_DATA_DIR / "raw_supplies.csv",
}

AZURE_SAS_URL = os.getenv("AZURE_SAS_URL")

@asset(
    name="raw_support_tickets_bronze",
    description="Extracts JSONL ticket data directly from Azure Blob Storage using SAS key.",
    group_name="bronze_layer"
)
def raw_support_tickets_bronze(context):
    logger = get_dagster_logger()
    
    if not AZURE_SAS_URL:
        logger.error("AZURE_SAS_URL is not set in the .env file.")
        raise ValueError("Cannot connect to Azure: SAS URL is missing.")

    output_path = BRONZE_DIR / "support_tickets.csv"
    data_list = []
    
    try:
        # Create a ContainerClient using the SAS URL
        container_client = ContainerClient.from_container_url(AZURE_SAS_URL)
        
        # NOTE: If the SAS URL points directly to the file, list_blobs might fail.
        # We assume the URL points to the container, and we expect one file.
        # If the URL points directly to the file, we would use BlobClient.
        
        # --- Simplified Blob Download (Assuming a Container-level SAS) ---
        blobs = container_client.list_blobs()
        
        for blob in blobs:
            if blob.name.endswith('.jsonl'):
                blob_client = container_client.get_blob_client(blob.name)
                blob_data = blob_client.download_blob().readall()
                
                # Process the JSONL content line by line
                for line in blob_data.decode('utf-8').splitlines():
                    if line.strip():
                        data_list.append(json.loads(line))

                logger.info(f"Successfully processed blob: {blob.name}")
            
        if not data_list:
            raise Exception("No JSONL data found or processed from Azure container.")

    except Exception as e:
        # If connection fails, raise a descriptive error
        logger.error(f"Azure Connection or Download Failed: {e}")
        raise

    # 3. Convert to DataFrame and Store in Bronze Layer (as CSV)
    raw_tickets_df = pd.DataFrame(data_list)
    raw_tickets_df.to_csv(output_path, index=False) 
    
    context.log.info(f"Successfully saved raw tickets data as CSV to: {output_path}")
    
    return raw_tickets_df

def create_raw_csv_asset(name, file_path):
    """
    Utility function to dynamically create a Dagster asset for a local raw CSV file,
    saving the output *as a CSV* in the Bronze layer.
    """

    @asset(
        name=f"raw_{name}_bronze",
        description=f"Ingests the raw {name} data and saves it as CSV in the Bronze layer.",
        group_name="bronze_layer"
    )
    def _asset():
        print(f"Starting ingestion for {name} from {file_path}")
        
        if not file_path.exists():
            raise FileNotFoundError(f"Raw data file not found at: {file_path}. Did you download it?")
        
        df = pd.read_csv(file_path)
        
        # 🚨 KEY CHANGE: SAVING AS CSV
        output_path = BRONZE_DIR / f"{name}.csv"
        
        # index=False prevents pandas from writing a row index column into the CSV
        df.to_csv(output_path, index=False) 
        
        print(f"Successfully saved raw {name} data as CSV to: {output_path}")
        
        return df

    return _asset


# --- SILVER LAYER ASSETS (Cleaned, Joined Data) ---

@asset(
    name="joined_orders_tickets_silver",
    description="Cleans and joins orders/tickets using DuckDB, saving the output as Parquet.",
    group_name="silver_layer",
    ins={
        "raw_orders": AssetIn(key="raw_orders_bronze"),
        "raw_tickets": AssetIn(key="raw_support_tickets_bronze"),
    },
)
def joined_orders_tickets_silver(raw_orders: pd.DataFrame, raw_tickets: pd.DataFrame):
    logger = get_dagster_logger()
    
    # 1. Transformation and Loading into DuckDB
    con = duckdb.connect(database=str(DUCKDB_PATH), read_only=False)
    
    # Register DataFrames as temporary tables for SQL joining
    con.register('raw_orders_table', raw_orders)
    con.register('raw_tickets_table', raw_tickets)

    # 2. 🔑 CORE SQL TRANSFORMATION (Cleaning and Joining)
    sql_query = """
    SELECT
        T1.id AS order_id,
        T1.customer AS customer_id,  -- Renaming the column here
        T1.ordered_at,
        T1.store_id,
        T1.subtotal,
        T1.tax_paid,
        T1.order_total,
        T2.ticket_id,
        T2.priority,
        T2.status,
        T2.tags,
        T2.resolved_at,
        CASE WHEN T2.ticket_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_support_ticket
    FROM raw_orders_table AS T1
    LEFT JOIN raw_tickets_table AS T2
      ON T1.id = T2.order_id 
    """
    
    # Execute SQL and store result in a new DuckDB table for Gold layer to reference
    con.execute("CREATE OR REPLACE TABLE fct_orders_tickets_silver AS " + sql_query)
    
    # 3. Export to Parquet (The final file output)
    SILVER_DIR = Path(__file__).parent.parent / "data" / "silver"
    SILVER_DIR.mkdir(exist_ok=True)
    output_path = SILVER_DIR / "fct_orders_tickets.parquet"
    
    # Use DuckDB's powerful COPY command to export the table directly to Parquet
    con.execute(f"COPY fct_orders_tickets_silver TO '{output_path}' (FORMAT PARQUET)")
    
    # Disconnect
    con.close()
    
    logger.info(f"Silver layer table processed via DuckDB and saved as Parquet: {output_path}")
    
    # Returning True/None is sufficient if the downstream asset reads the file directly
    return True

# ----- Gold layer ----

@asset(
    name="avg_order_value_gold",
    description="Calculates AOV using DuckDB SQL and exports to Parquet.",
    group_name="gold_marts",
    # Must wait for Silver table to be created in DuckDB
    ins={"fct_orders_tickets": AssetIn(key="joined_orders_tickets_silver")},
)
def avg_order_value_gold(fct_orders_tickets):
    logger = get_dagster_logger()
    
    con = duckdb.connect(database=str(DUCKDB_PATH), read_only=True) # Read-only connection

    # 1. 🔑 CORE SQL AGGREGATION (Calculates AOV)
    sql_query = """
    SELECT 
        'average_order_value' AS metric_name,
        AVG(order_total) AS value
    FROM (
        -- Select only unique orders to ensure correct AOV calculation
        SELECT DISTINCT order_id, order_total
        FROM fct_orders_tickets_silver
    )
    """
    
    # 2. Export to Parquet (Final output file)
    GOLD_DIR = Path(__file__).parent.parent / "data" / "gold"
    GOLD_DIR.mkdir(exist_ok=True)
    output_path = GOLD_DIR / "aov_mart.parquet"
    
    # Use DuckDB to run query and export directly to Parquet
    con.execute(f"COPY ({sql_query}) TO '{output_path}' (FORMAT PARQUET)")
    
    con.close()
    logger.info(f"Gold layer AOV mart saved as Parquet: {output_path}")
    
    return True


@asset(
    name="tickets_per_order_gold",
    description="Calculates tickets per order using DuckDB SQL and exports to Parquet.",
    group_name="gold_marts",
    ins={"fct_orders_tickets": AssetIn(key="joined_orders_tickets_silver")},
)
def tickets_per_order_gold(fct_orders_tickets):
    logger = get_dagster_logger()
    
    con = duckdb.connect(database=str(DUCKDB_PATH), read_only=True)

    # 1. 🔑 CORE SQL AGGREGATION (Count unique tickets per order)
    sql_query = """
    SELECT 
        order_id,
        COUNT(DISTINCT ticket_id) AS num_tickets
    FROM fct_orders_tickets_silver
    WHERE has_support_ticket = TRUE
    GROUP BY 1
    """
    
    # 2. Export to Parquet (Final output file)
    GOLD_DIR = Path(__file__).parent.parent / "data" / "gold"
    output_path = GOLD_DIR / "order_ticket_count_mart.parquet"
    
    # Use DuckDB to run query and export directly to Parquet
    con.execute(f"COPY ({sql_query}) TO '{output_path}' (FORMAT PARQUET)")
    
    con.close()
    logger.info(f"Gold layer ticket count mart saved as Parquet: {output_path}")
    
    return True



# 1. Create the 6 dynamic CSV assets
RAW_CSV_ASSETS = [
    create_raw_csv_asset(name, file_path) 
    for name, file_path in RAW_FILES.items()
]

# 2. Collect ALL Bronze/Silver/Gold assets into a single list
# This list is what load_assets_from_modules will find.
ALL_DEFINED_ASSETS = RAW_CSV_ASSETS + [
    raw_support_tickets_bronze,
    joined_orders_tickets_silver,
    avg_order_value_gold,
    tickets_per_order_gold,
]