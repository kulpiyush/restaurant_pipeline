import pandas as pd
from dagster import materialize
from pathlib import Path
import pytest
import requests 
import json
import os
# Import the definitions that contain your assets
from restaurant_pipeline.assets import (
    RAW_FILES, 
    create_raw_csv_asset, 
    raw_support_tickets_bronze, 
    joined_orders_tickets_silver,
    avg_order_value_gold,
    tickets_per_order_gold,
    PROJECT_ROOT 
)

# -----------------------------------------------------
# 🔑 FIX: DEFINE ALL DIRECTORIES GLOBALLY FOR FIXTURE USE
# -----------------------------------------------------
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"  # Moved up
GOLD_DIR = PROJECT_ROOT / "data" / "gold"      # Moved up
# -----------------------------------------------------


# --- Setup Fixtures and Teardown ---

# Generate the list of all assets defined in your project
RAW_ASSETS = [
    create_raw_csv_asset(name, path) 
    for name, path in RAW_FILES.items()
]

# The complete list of all Bronze assets to test (6 CSVs + 1 Azure)
ALL_BRONZE_ASSETS = RAW_ASSETS + [raw_support_tickets_bronze]

# The complete list of ALL assets for full pipeline tests
ALL_PIPELINE_ASSETS = ALL_BRONZE_ASSETS + [
    joined_orders_tickets_silver,
    avg_order_value_gold,
    tickets_per_order_gold,
]


@pytest.fixture(scope="session", autouse=True)
def cleanup_data_dirs():
    """Cleans up all data layers before and after testing."""
    
    # 1. Clean up before tests run
    for file in BRONZE_DIR.glob("*.csv"):
        file.unlink()
    for file in SILVER_DIR.glob("*.parquet"):
        file.unlink()
    for file in GOLD_DIR.glob("*.parquet"):
        file.unlink()
    
    yield # Run the tests

    # 2. Clean up after tests run
    for file in BRONZE_DIR.glob("*.csv"):
        file.unlink()
    for file in SILVER_DIR.glob("*.parquet"):
        file.unlink()
    for file in GOLD_DIR.glob("*.parquet"):
        file.unlink()


# --- Tests Bronze Layer (Kept for completeness) ---

def test_all_bronze_assets_materialize_successfully():
    """Tests that all 7 raw ingestion assets can run without error."""
    
    result = materialize(ALL_BRONZE_ASSETS)
    assert result.success
    assert len(result.get_asset_materialization_events()) == 7

def test_bronze_layer_files_are_created_as_csv():
    """Tests that the correct number of CSV files are created in the Bronze directory."""
    
    materialize(ALL_BRONZE_ASSETS) 
    
    bronze_files = list(BRONZE_DIR.glob("*.csv"))
    assert len(bronze_files) == 7
    
    test_file_path = BRONZE_DIR / "customers.csv"
    df = pd.read_csv(test_file_path)
    assert "id" in df.columns
    assert "name" in df.columns
    assert "Unnamed: 0" not in df.columns

    test_tickets_path = BRONZE_DIR / "support_tickets.csv"
    df_tickets = pd.read_csv(test_tickets_path)
    assert "ticket_id" in df_tickets.columns 
    assert "order_id" in df_tickets.columns
    assert len(df_tickets) > 0


# ----- Test Silver Layer -----

def test_silver_layer_joins_and_saves_parquet():
    """
    Tests that the Silver asset runs successfully, creates a Parquet file, 
    and correctly performs the join logic.
    """
    
    # Run the entire pipeline up to the Silver layer
    result = materialize(ALL_BRONZE_ASSETS + [joined_orders_tickets_silver])
    
    assert result.success
    
    # 2. Check the output file structure
    output_path = SILVER_DIR / "fct_orders_tickets.parquet"
    assert output_path.exists()
    
    # 3. Read the output Parquet file
    silver_df = pd.read_parquet(output_path)
    
    # 4. Assert data transformations and column presence
    assert "order_id" in silver_df.columns
    assert "customer_id" in silver_df.columns
    assert "ticket_id" in silver_df.columns
    assert "has_support_ticket" in silver_df.columns
    assert len(silver_df) > 100 


# ----- Test Gold Layer -----

def test_gold_aov_calculation_and_materialization():
    """Tests the Average Order Value (AOV) asset runs and produces a single metric."""
    
    # Run the full pipeline including the Gold layer
    result = materialize(ALL_PIPELINE_ASSETS) 
    assert result.success

    # 2. Check the output file structure
    output_path = GOLD_DIR / "aov_mart.parquet"
    assert output_path.exists()
    
    # 3. Read the output Parquet file
    aov_df = pd.read_parquet(output_path)
    
    # 4. Assert data structure and content
    assert len(aov_df) == 1
    assert "metric_name" in aov_df.columns
    assert "value" in aov_df.columns
    assert aov_df['value'].iloc[0] > 0
    
def test_gold_tickets_per_order_materialization():
    """Tests the Tickets Per Order asset runs and produces the correct analytical table."""
    
    # Run the full pipeline (using the same ALL_PIPELINE_ASSETS list ensures all dependencies run)
    result = materialize(ALL_PIPELINE_ASSETS) 
    assert result.success

    # 2. Check the output file structure
    output_path = GOLD_DIR / "order_ticket_count_mart.parquet"
    assert output_path.exists()
    
    # 3. Read the output Parquet file
    tickets_mart_df = pd.read_parquet(output_path)
    
    # 4. Assert data structure and content
    assert "order_id" in tickets_mart_df.columns
    assert "num_tickets" in tickets_mart_df.columns
    assert len(tickets_mart_df) > 0
    assert tickets_mart_df['num_tickets'].min() >= 1