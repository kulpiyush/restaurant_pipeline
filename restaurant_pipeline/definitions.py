from dagster import Definitions, load_assets_from_modules, ScheduleDefinition

from restaurant_pipeline import assets  # noqa: TID252

# Load all assets
all_assets = load_assets_from_modules([assets])

# 🔑 Define the Daily Schedule
daily_schedule = ScheduleDefinition(
    # The cron schedule to run the job daily at midnight (00:00)
    cron_schedule="25 17 * * *", 
    # The name of the job to run. Dagster automatically names the job 
    # that materializes all assets in the module as '__ASSET_JOB'.
    job_name="__ASSET_JOB", 
    # A user-friendly name
    name="daily_restaurant_etl_schedule",
)

# Define the overall Definitions object, including the new schedule
defs = Definitions(
    assets=all_assets,
    schedules=[daily_schedule], # 🔑 Register the new schedule here
)
