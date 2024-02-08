import os
from dagster import Definitions
from .jobs.jobs_using_ops.smart_meters_forecasting_upload_minio import upload_smart_meters_forecasting_data_job
from .jobs.jobs_using_ops.smart_meters_monthly_upload import upload_historical_smart_meters_data_job
from .jobs.jobs_using_ops.smart_meters_day_ahead_predictions import compute_day_ahead_forecasts
from .sensors import compute_day_ahead_predictions_sensor
from .schedules import load_forecasting_daily_schedule
from .resources import RESOURCES_ENV_BY_FILE

resources_by_deployment_name = {
    # "prod": RESOURCES_PROD,
    # "staging": RESOURCES_STAGING,
    # "local": RESOURCES_LOCAL,
    "by_env_file": RESOURCES_ENV_BY_FILE
}

all_assets = [
    # *forecasting_data_assets
]

all_jobs = [
    upload_historical_smart_meters_data_job,
    upload_smart_meters_forecasting_data_job,
    compute_day_ahead_forecasts
]

deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "by_env_file")

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    resources=resources_by_deployment_name[deployment_name],
    schedules=[load_forecasting_daily_schedule],
    sensors=[compute_day_ahead_predictions_sensor],
)
