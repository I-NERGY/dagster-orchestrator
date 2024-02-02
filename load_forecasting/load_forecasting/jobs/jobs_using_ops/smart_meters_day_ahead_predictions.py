import io
import os
import sys
import dagster
import requests
import pandas as pd
from typing import Tuple, List
from pydantic import Field
from datetime import datetime, timedelta
from dagster import job, Definitions, op, Config, RunConfig, Out, get_dagster_logger, Failure, success_hook

from load_forecasting.resources.flexdr_service import FlexDRService
from load_forecasting.resources.s3_resource import S3Resource
from load_forecasting.resources.mongo_resource import MongoResource
from load_forecasting.resources.forecasting_service import LoadForecastingService
from load_forecasting.resources.clustering_service import ClustersPredictionService


class MinioLocationConfig(Config):
    bucket: str = Field(
        description="The MinIO bucket name",
    )
    bucket_directory: str = Field(
        description="The directory name relative to bucket where files will be uploaded",
    )


# ---------------------------------------------------------------------------------------------------

@op(
    description="Op fetching load data to feed into forecasting model",
    out={"load_data": Out(description="Smart meters data for the last (up to 14) days")}
)
def fetch_recent_load_data(s3_conn: S3Resource, config: MinioLocationConfig) -> pd.DataFrame:
    log = get_dagster_logger()
    bucket: str = config.bucket
    directory: str = config.bucket_directory
    # current date
    today = datetime.now().strftime("%Y-%m-%d")
    # TODO: REMOVE
    # today = datetime.strptime('2024-01-31', "%Y-%m-%d").strftime("%Y-%m-%d")
    # # today = datetime.now().date()
    try:
        objects = s3_conn.list_objects(
            bucket=bucket,
            prefix=directory
        )
        # object_name = obj.object_name[len(directory):].lstrip('/')
        object_name = next((obj.object_name for obj in objects if today in obj.object_name), None)
        if object_name is not None:
            response = s3_conn.get_object(bucket=bucket, object_name=object_name)
            return pd.read_csv(io.BytesIO(response))
        else:
            raise Failure(description=f"Load data not found for {today}.")
    except dagster.Failure as df:
        raise df
    except Exception as e:
        log.error(f"An error occurred: {e}")
        raise Failure(description="Error fetching objects")


@op(
    description="Op fetching load data to feed into forecasting model",
    out={"load_data_single_format": Out(description="Smart meters data for the last (up to 14) days")}
)
def transform_load_data(load_data: pd.DataFrame) -> pd.DataFrame:
    log = get_dagster_logger()
    # keep only data related to consumption
    cons_df = load_data[load_data['tag_name'].str.contains("Apiu")]
    # drop tag_name
    cons_df.drop(['tag_name'], axis=1, inplace=True)
    # melt dataframe to generate single timeseries format dataframe
    melted_df = pd.melt(cons_df, id_vars=['device_id', 'date'], var_name='time', value_name='value')
    # combine date and time columns to create a new datetime column
    melted_df['datetime'] = melted_df['date'] + ' ' + melted_df['time']
    # convert the datetime column to a datetime format
    melted_df['datetime'] = pd.to_datetime(melted_df['datetime'], format='%Y-%m-%d %H:%M:%S')
    # drop columns
    melted_df.drop(['date', 'time'], axis=1, inplace=True)
    # Sort by device id and datetime
    sorted_df = melted_df.sort_values(by=['device_id', 'datetime'])
    return sorted_df


@op(
    description="Op fetching smart meters registered in FlexDR",
    out={"smart_meters": Out(description="Smart meters registered in FlexDR")}
)
def fetch_registered_smart_meters(flex_dr_mongo_conn: MongoResource) -> List:
    log = get_dagster_logger()
    flex_dr_db = flex_dr_mongo_conn.get_database()
    smart_meters = flex_dr_db['meters'].find()
    if not smart_meters.alive:
        raise Failure(description="No smart meters found. Cannot proceed")
    return list(smart_meters)


@op(
    description="Op using DeepTSF model service to get predictions for the day ahead",
    out={"day_ahead_forecasts": Out(description="Day ahead forecasts file path.")}
)
def get_day_ahead_load_forecasts(load_data_single_format: pd.DataFrame, smart_meters: List,
                                 forecasting_service: LoadForecastingService) -> Tuple[str, str]:
    log = get_dagster_logger()
    day_ahead = datetime.now() + timedelta(days=1)
    hourly_range = pd.date_range(start=day_ahead.replace(hour=0, minute=0, second=0),
                                 end=day_ahead.replace(hour=23, minute=0, second=0),
                                 freq='H').time
    columns = [col.strftime("%H:%M:%S") for col in hourly_range]
    # TODO: columns are expected to refer to day ahead, but this may not happen
    predictions_df = pd.DataFrame(columns=columns)
    for sm_doc in smart_meters:  # smart_meters:
        sm = sm_doc['device_id']
        device_data = load_data_single_format[load_data_single_format['device_id'] == sm]
        if device_data.shape[0] == 0:
            continue
        series = device_data.drop(['device_id'], axis=1)
        series.set_index('datetime', inplace=True)
        try:
            response = forecasting_service.predict(timeseries=series, horizon=24, device_id=sm)
            response.raise_for_status()
            result_df = pd.read_json(response.text)
        except requests.exceptions.ConnectionError as e:
            log.info(e)
            raise Failure(description="Connection error")
        except requests.exceptions.Timeout as e:
            log.info(e)
            raise Failure(description="Connection timeout. Load forecasting service not reachable")
        except requests.exceptions.RequestException as e:
            print(f"Error getting predictions. {e}")
        else:
            predictions_df.loc[sm] = result_df['value'].tolist()
    predictions_df['date'] = day_ahead.strftime("%Y-%m-%d")
    predictions_df.reset_index(drop=False, names='device_id', inplace=True)
    folder_path = os.path.join(os.path.dirname(os.path.abspath(sys.modules['load_forecasting'].__file__)), "generated",
                               'day_ahead_forecasts')
    output_file_name = f'{day_ahead.strftime("%Y-%m-%d")}.csv'
    output_file = os.path.join(folder_path, output_file_name)
    predictions_df.to_csv(output_file)
    return output_file, output_file_name


@op(
    description="Op computing the clusters for the forecasted day ahead load",
    out={"day_ahead_forecasts": Out(description="Day ahead forecasts file path.")}
)
def predict_clusters(day_ahead_forecasts: Tuple[str, str], clustering_service: ClustersPredictionService, config: MinioLocationConfig ) -> Tuple[
    str, str]:
    log = get_dagster_logger()
    file_path, file_name = day_ahead_forecasts
    load_forecasts = pd.read_csv(file_path, index_col=0)
    payload = load_forecasts.drop(columns=['device_id', 'date'])
    try:
        response = clustering_service.predict(timeseries=payload)
        response.raise_for_status()
        result_df = pd.read_json(response.text)
    except requests.exceptions.ConnectionError as e:
        log.info(e)
        raise Failure(description="Connection error")
    except requests.exceptions.Timeout as e:
        log.info(e)
        raise Failure(description="Connection timeout. Load forecasting service not reachable")
    except requests.exceptions.RequestException as e:
        print(f"Error getting predictions. {e}")
    else:
        load_forecasts['cluster'] = result_df.values
        load_forecasts.to_csv(file_path)
        return file_path, file_name


@success_hook(required_resource_keys={"s3_conn"})
def day_ahead_forecasts_success_hook(context):
    log = get_dagster_logger()
    file_path, file_name = context.op_output_values['day_ahead_forecasts']
    bucket = context.op_config['bucket']
    directory = context.op_config['bucket_directory']
    try:
        context.resources.s3_conn.store(
            bucket=bucket,
            file_path=file_path,
            file_name=os.path.join(f"{directory}/{file_name}")
        )
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


@op(
    description="Op uploading and then deleting the local monthly data file"
)
def create_assignments(day_ahead_forecasts: Tuple[str, str], flexdr_service: FlexDRService) -> None:
    log = get_dagster_logger()
    file_path, file_name = day_ahead_forecasts
    data = pd.read_csv(file_path, index_col=0)
    load_data = data.drop(columns=['device_id', 'cluster', 'date'])
    # future implementation will handle multiple assignments,
    # so requests will not be done in for loop
    for idx, row in data.iterrows():
        try:
            response = flexdr_service.insert_forecasts(
                device_id=row['device_id'],
                cluster=int(row['cluster']),
                day_ahead_load=load_data.loc[idx].values.tolist(),
                forecast_date=row['date']
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            log.warning(f"Error creating assignment for {row['device_id']}")
            log.error(f'{e.response.status_code} {e.response.reason} {e.response.text} ')
    os.remove(file_path)
    log.info(f"File '{file_name}' has been deleted.")


@job(
    description="A job responsible for acquiring day ahead load and clusters for smart meters involved in FlexDR"
)
def compute_day_ahead_forecasts():
    load_data = fetch_recent_load_data()
    transformed_load_data = transform_load_data(load_data)
    smart_meters = fetch_registered_smart_meters()
    day_ahead_forecasts = get_day_ahead_load_forecasts(transformed_load_data, smart_meters)
    day_ahead_clusters = predict_clusters.with_hooks({day_ahead_forecasts_success_hook})(day_ahead_forecasts)
    create_assignments(day_ahead_clusters)


# # Comment out for Python execution
# defs = Definitions(
#     jobs=[compute_day_ahead_forecasts],
#     resources={
#         "flex_dr_mongo_conn": MongoResource(
#             user='',
#             password='',
#             address='',
#             database='',
#             collection='collection'
#         ),
#         "s3_conn": S3Resource(
#             url='localhost:9000',
#             access_key='',
#             secret_key='',
#         ),
#         "forecasting_service": LoadForecastingService(
#             endpoint=''
#         ),
#         "clustering_service": ClustersPredictionService(
#             endpoint=''
#         ),
#         "flexdr_service": FlexDRService(
#             endpoint=''
#         )
#     },
# )
# # #
# if __name__ == "__main__":
#     result = defs.get_job_def('compute_day_ahead_forecasts').execute_in_process(
#         run_config=RunConfig({
#             "fetch_recent_load_data": MinioLocationConfig(
#                 bucket="load-forecasting",
#                 bucket_directory="forecasting-data"
#             ),
#             "predict_clusters": MinioLocationConfig(
#                 bucket="load-forecasting",
#                 bucket_directory="day_ahead_forecasts"
#             ),
#
#         }))
