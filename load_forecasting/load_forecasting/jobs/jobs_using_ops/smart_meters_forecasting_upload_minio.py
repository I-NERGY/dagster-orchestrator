import re
import os
from typing import Tuple, Dict, Union
from dagster import Definitions, Config, RunConfig, get_dagster_logger, Failure, op, job, Output, graph, Out
from pydantic import Field, validator
from datetime import datetime, timedelta
from load_forecasting.resources.s3_resource import S3Resource
from load_forecasting.resources.mongo_resource import MongoResource
from load_forecasting.utils.asm_uc7.asm_uc7_mongo_processing import smart_meters_load_forecasting_processing

TIME_INTERVALS = [f"{hour:02d}:{minute:02d}:00" for hour in range(24) for minute in range(0, 60, 30)]


# ------------------------------------- CONFIG ---------------------------------------------------------
class SmartMeterForecastingQueryConfig(Config):
    window_size: int = Field(
        description="Range in days [1,14] to generate data for forecasting",
        ge=1,
        le=14,
        default=14
    )
    upper_date_threshold: str = Field(
        description="The upper date threshold (e.g., 'YYYY-MM-DD')",
        default=datetime.now().strftime('%Y-%m-%d')
    )

    @validator('upper_date_threshold')
    def validate_date_format(cls, value):
        # regex validation
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', value):
            raise ValueError("The date format should be 'YYYY-MM-DD'")
        return value


class SmartMeterForecastingUploadOpConfig(Config):
    bucket: str = Field(default='load-forecasting')
    bucket_directory: str = Field(default='forecasting-data')


# ---------------------------------------------------------------------------------------------------

@op(
    description="Op computing the lower and upper boundaries for fetching data for forecasting"
)
def generate_query_boundaries(config: SmartMeterForecastingQueryConfig) -> Tuple[datetime, datetime, str]:
    log = get_dagster_logger()
    if config.upper_date_threshold is None:
        upper_datetime_threshold = datetime.now()
    else:
        upper_datetime_threshold = datetime.strptime(config.upper_date_threshold, "%Y-%m-%d")
    lower_datetime_threshold = upper_datetime_threshold - timedelta(days=config.window_size)
    lower_datetime_threshold.replace(hour=0, minute=0, second=0)
    log.info(f'Current datetime: {upper_datetime_threshold} Lower threshold datetime: {lower_datetime_threshold}')
    upper_date_threshold = upper_datetime_threshold.strftime('%Y-%m-%d')
    lower_date_threshold = lower_datetime_threshold.strftime('%Y-%m-%d')
    range_id = f'{lower_date_threshold}_{upper_date_threshold}'
    # result = mongo_conn.aggregate(lower_datetime_threshold, upper_datetime_threshold)
    return lower_datetime_threshold, upper_datetime_threshold, range_id


@op(
    description="Op creating temporary file with data for forecasting in case data found in mongo.",
    out={"no_data": Out(int, is_required=False), "upload": Out(dict, is_required=False)}
)
def store_smart_meter_forecasting_data_locally(range_tuple: Tuple[datetime, datetime, str],
                                               mongo_conn: MongoResource) -> Union[int, dict]:
    log = get_dagster_logger()
    lower_datetime_threshold, upper_datetime_threshold, range_id = range_tuple
    pipeline = [
        {
            "$match": {
                "date": {
                    "$gte": lower_datetime_threshold.strftime("%Y-%m-%d"),
                    "$lte": upper_datetime_threshold.strftime("%Y-%m-%d")
                }
            }
        }
    ]
    data_cursor = mongo_conn.aggregate(pipeline)
    if not data_cursor.alive:
        log.info("No data found for specific range.")
        yield Output(1, "no_data")
    else:
        file_name = smart_meters_load_forecasting_processing(data_cursor=data_cursor, file_id=range_id,
                                                             local_dir_name='smart_meters_forecasting_data')
        yield Output({'file_path': file_name, 'file_name': range_id}, "upload")


@op(
    description="Op uploading data file to MinIO and then deleting it."
)
def upload_forecasting_data_to_minio(file_dict: Dict[str, str], s3_conn: S3Resource,
                                     config: SmartMeterForecastingUploadOpConfig
                                     ) -> None:
    log = get_dagster_logger()
    file_path = file_dict["file_path"]
    file_name = file_dict["file_name"]
    bucket = config.bucket
    directory = config.bucket_directory
    try:
        s3_conn.store(
            bucket=bucket,
            file_path=file_path,
            file_name=os.path.join(f"{directory}/{file_name}.csv")
        )
        os.remove(file_path)
        log.info(f"File '{file_name}' has been deleted.")
    except FileNotFoundError as fnf:
        log.error(fnf)
        raise Failure(
            description="File not found",
        )
    except Exception as e:
        log.error(f"An error occurred: {e}")
        raise Failure(
            description="Error uploading the file",
        )


@graph
def upload_smart_meters_forecasting():
    boundaries = generate_query_boundaries()
    no_data, upload = store_smart_meter_forecasting_data_locally(boundaries)
    upload_forecasting_data_to_minio(upload)


@job(
    description="Job fetching and uploading data to Minio for last (maximum 14) days."
)
def upload_smart_meters_forecasting_data_job():
    upload_smart_meters_forecasting()

# defs = Definitions(
#     # assets=[store_smart_meter_forecasting_data_locally,],
#     jobs=[upload_smart_meters_forecasting_data],
#     resources={
#         "mongo_conn": MongoResource(),
#         "s3_conn": S3Resource()
#     },
# )

# if __name__ == "__main__":
#     result = defs.get_job_def('upload_smart_meters_forecasting_data').execute_in_process(
#         run_config=RunConfig({
#             'upload_smart_meters_forecasting': {'ops': {
#                 "generate_query_boundaries": SmartMeterForecastingQueryConfig(
#                     window_size=14,
#                     upper_date_threshold="2021-01-26"
#                 ),
#                 "upload_forecasting_data_to_minio": SmartMeterForecastingUploadOpConfig(
#                     bucket="load-forecasting",
#                     bucket_directory="historical_monthly_data"
#                 ), }}
#         }))
