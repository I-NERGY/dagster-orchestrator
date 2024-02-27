from dagster import schedule, ScheduleEvaluationContext, DefaultScheduleStatus
from dagster import RunRequest, RunConfig
from load_forecasting.jobs.jobs_using_ops.smart_meters_forecasting_upload_minio import \
    upload_smart_meters_forecasting_data_job, SmartMeterForecastingUploadOpConfig, SmartMeterForecastingQueryConfig


# def should_execute(context: ScheduleEvaluationContext):
#     threshold = datetime.now(pytz.timezone('EET')).replace(hour=22, minute=00)
#     print(threshold)
#     run_ids = context.instance.get_run_ids(
#         filters=RunsFilter(
#             job_name="upload_smart_meters_forecasting_data_job",
#             updated_after=threshold.astimezone(pytz.utc),
#         ),
#         limit=1
#     ),
#     print(f'Run IDS : {run_ids}')
#     print(f' Run[0] : {run_ids[0]}')
#     run = context.instance.get_run_by_id(run_id=run_ids[0][0])
#     print(f'Run: {run}')
#     return run.is_success


@schedule(job=upload_smart_meters_forecasting_data_job, cron_schedule="15 15 * * *", execution_timezone='EET', default_status=DefaultScheduleStatus.RUNNING)
def load_forecasting_daily_schedule(context: ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=None,
        run_config=RunConfig({
            'upload_smart_meters_forecasting': {'ops': {
                "generate_query_boundaries": SmartMeterForecastingQueryConfig(
                    window_size=14,
                    upper_date_threshold=scheduled_date
                ),
                "upload_forecasting_data_to_minio": SmartMeterForecastingUploadOpConfig(
                    bucket="load-forecasting",
                    bucket_directory="forecasting-data"
                ),
            }}
        }),
        tags={"date": scheduled_date},
    )