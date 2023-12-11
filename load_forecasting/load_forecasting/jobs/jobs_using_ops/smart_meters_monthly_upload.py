import re
import os
from typing import List, Generator, Tuple, Dict
from pydantic import Field, validator
from datetime import datetime, timedelta
from dagster import job, Definitions, Output, DynamicOutput, DynamicOut, op, Config, RunConfig, Out, get_dagster_logger, \
    Failure, graph
from load_forecasting.resources.s3_resource import S3Resource
from load_forecasting.resources.mongo_resource import MongoResource
from load_forecasting.utils.asm_uc7.asm_uc7_mongo_processing import smart_meters_load_forecasting_processing

TIME_INTERVALS = [f"{hour:02d}:{minute:02d}:00" for hour in range(24) for minute in range(0, 60, 30)]


# ---------------------------------------- CONFIG ---------------------------------------------------------
class SmartMeterMonthlyQueryOpConfig(Config):
    lower_date_threshold: str = Field(
        ...,
        description="The lower date threshold (e.g., 'YYYY-MM-DD')",
    )
    upper_date_threshold: str = Field(
        ...,
        description="The upper date threshold (e.g., 'YYYY-MM-DD')",
    )

    @validator('upper_date_threshold', 'lower_date_threshold')
    def validate_date_format(cls, value):
        # regex validation
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', value):
            raise ValueError("The date format should be 'YYYY-MM-DD'")
        return value


class SmartMeterMonthlyUploadOpConfig(Config):
    bucket: str = Field(
        description="The MinIO bucket name",
        default='load-forecasting'
    )
    bucket_directory: str = Field(
        description="The directory name relative to bucket where files will be uploaded",
        default='smart_meters_monthly_data'
    )


# ---------------------------------------------------------------------------------------------------

@op(
    description="Op computing uploaded files corresponding to monthly data",
    out={"uploaded_files": Out(description="A list with the file names uploaded in Minio")}
)
def find_uploaded_months(s3_conn: S3Resource, config: SmartMeterMonthlyUploadOpConfig) -> List[str]:
    log = get_dagster_logger()
    bucket: str = config.bucket
    directory = config.bucket_directory
    uploaded = []
    try:
        objects = s3_conn.list_objects(
            bucket=bucket,
            prefix=directory
        )
        for obj in objects:
            file_name = obj.object_name[len(directory):].lstrip('/')
            uploaded.append(file_name)
    except Exception as e:
        log.error(f"An error occurred: {e}")
        raise Failure(
            description="Error fetching objects",
        )
    else:
        return uploaded


@op(
    description="A tuple containing each month's start,end datetime and id",
    out=DynamicOut(Tuple[datetime, datetime, str])
)
def smart_meter_monthly_data_generator(uploaded_files: List[str], config: SmartMeterMonthlyQueryOpConfig
                                       ) -> Generator[Tuple[datetime, datetime, str], None, None]:
    log = get_dagster_logger()

    current_datetime = datetime.strptime(config.upper_date_threshold, "%Y-%m-%d")
    lower_datetime_threshold = datetime.strptime(config.lower_date_threshold, "%Y-%m-%d")

    log.info(f'Current datetime: {current_datetime} Lower threshold datetime: {lower_datetime_threshold}')
    while current_datetime > lower_datetime_threshold:
        first_day_of_month = current_datetime.replace(day=1)
        last_day_of_month = (first_day_of_month + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        last_day_of_month.replace(hour=23, minute=59, second=59)

        month_id = f'{first_day_of_month.month:02d}_{first_day_of_month.year}'
        # check if data for this month have been already uploaded
        log.info(f'Query start date: {first_day_of_month} Query end date: {last_day_of_month}')
        if f'{month_id}.csv' not in uploaded_files:
            yield DynamicOutput(
                (first_day_of_month, last_day_of_month, month_id),
                mapping_key=f'{month_id}'
            )

        current_datetime = current_datetime - timedelta(days=current_datetime.day)


@op(
    description="Op fetching and resampling monthly data",
    out={"no_data": Out(int, is_required=False), "upload": Out(dict, is_required=False)}
)
def fetch_smart_meter_monthly_data_locally(month_tuple: Tuple[datetime, datetime, str],
                                           mongo_conn: MongoResource) -> dict:
    log = get_dagster_logger()
    first_day_of_month, last_day_of_month, month_id = month_tuple
    pipeline = [
        {
            "$match": {
                "date": {
                    "$gte": first_day_of_month.strftime("%Y-%m-%d"),
                    "$lte": last_day_of_month.strftime("%Y-%m-%d")
                }
            }
        }
    ]

    data_cursor = mongo_conn.aggregate(pipeline)
    if not data_cursor.alive:
        log.info("No data found for specific range.")
        yield Output(1, "no_data")
    else:
        file_name = smart_meters_load_forecasting_processing(data_cursor=data_cursor, file_id=month_id,
                                                             local_dir_name='smart_meters_historical_monthly_data')
        yield Output({'file_path': file_name, 'file_name': month_id}, "upload")


@op(
    description="Op uploading and then deleting the local monthly data file"
)
def upload_to_minio(file_dict: Dict[str, str], s3_conn: S3Resource, config: SmartMeterMonthlyUploadOpConfig) -> None:
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


@graph(
    description="A graph enabling the conditional upload of a data file given it is not empty."
)
def upload_historical_smart_meters_data():
    def process_upload(month):
        no_data, monthly_data = fetch_smart_meter_monthly_data_locally(month)
        upload_to_minio(monthly_data)

    objects = find_uploaded_months()
    months = smart_meter_monthly_data_generator(objects)
    months.map(process_upload)


@job(
    description="A job resampling, transforming and uploading all data stored in MongoDB "
                "between a given range to MinIO for analytics and AI modelling purposes."
)
def upload_historical_smart_meters_data_job():
    upload_historical_smart_meters_data()


# Comment out for Python execution
# defs = Definitions(
#     jobs=[upload_historical_smart_meters_data_job],
#     resources={
#         "mongo_conn": MongoResource(
#             user='user',
#             password='pass',
#             address='host:port',
#             database='db',
#             collection='collection'
#         ),
#         "s3_conn": S3Resource(
#             url='host:port',
#             access_key='access',
#             secret_key='secret',
#         )
#     },
# )
#
# if __name__ == "__main__":
#     result = defs.get_job_def('upload_historical_smart_meters_data_job').execute_in_process(
#         run_config=RunConfig({
#             'upload_historical_smart_meters_data': {
#                 'ops': {
#                     "smart_meter_monthly_data_generator": SmartMeterMonthlyQueryOpConfig(
#                         lower_date_threshold="2023-09-01",
#                         upper_date_threshold="2023-10-26"
#                     ),
#                     "upload_to_minio": SmartMeterMonthlyUploadOpConfig(
#                         bucket="load-forecasting",
#                         bucket_directory="historical_monthly_data"
#                     ),
#                     "find_uploaded_months": SmartMeterMonthlyUploadOpConfig(
#                         bucket="load-forecasting",
#                         bucket_directory="historical_monthly_data"
#                     )
#                 }
#             }
#         }))
