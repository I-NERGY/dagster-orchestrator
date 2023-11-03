import os
from dagster import Definitions
from .jobs.jobs_using_ops.smart_meters_forecasting_upload_minio import upload_smart_meters_forecasting_data_job
from .jobs.jobs_using_ops.smart_meters_monthly_upload import upload_historical_smart_meters_data_job
from .resources import RESOURCES_LOCAL

resources_by_deployment_name = {
    # "prod": RESOURCES_PROD,
    # "staging": RESOURCES_STAGING,
    "local": RESOURCES_LOCAL,
}

all_assets = [
    # *forecasting_data_assets
]

all_jobs = [
    upload_historical_smart_meters_data_job,
    upload_smart_meters_forecasting_data_job
]

deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    resources=resources_by_deployment_name[deployment_name],
    schedules=[],
    sensors=[],
)
