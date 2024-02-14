from dagster import run_status_sensor, DagsterRunStatus, RunRequest, RunConfig, DefaultSensorStatus
from load_forecasting.jobs.jobs_using_ops.smart_meters_day_ahead_predictions import compute_day_ahead_forecasts, \
    MinioLocationConfig
from load_forecasting.jobs.jobs_using_ops.smart_meters_forecasting_upload_minio import \
    upload_smart_meters_forecasting_data_job


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    request_job=compute_day_ahead_forecasts,
    monitored_jobs=[upload_smart_meters_forecasting_data_job],
    default_status=DefaultSensorStatus.RUNNING
)
def compute_day_ahead_predictions_sensor(context):
    run_config = RunConfig({
        "fetch_recent_load_data": MinioLocationConfig(
            bucket="load-forecasting",
            bucket_directory="forecasting-data"
        ),
        "predict_clusters": MinioLocationConfig(
            bucket="load-forecasting",
            bucket_directory="day_ahead_forecasts"
        ),

    })
    return RunRequest(run_key=None, run_config=run_config)

