from dagster import Definitions

# 🔑 KEY: Correctly import the necessary objects from assets.py
from .assets import RAW_FILES, create_raw_csv_asset 
from .assets import raw_support_tickets_bronze # Assuming you added the Azure asset

# 1. Generate the 6 CSV assets
raw_csv_assets = [
    create_raw_csv_asset(name, file_path) 
    for name, file_path in RAW_FILES.items()
]

# 2. Define the overall Definitions object
defs = Definitions(
    # Combine the 6 CSV assets and the 1 Azure asset
    assets=raw_csv_assets + [
        raw_support_tickets_bronze
    ],
)