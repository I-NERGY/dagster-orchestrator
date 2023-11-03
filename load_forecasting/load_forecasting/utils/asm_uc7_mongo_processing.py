import os
import sys
import pandas as pd
import pymongo
from dagster import Failure, get_dagster_logger

TIME_INTERVALS = [f"{hour:02d}:{minute:02d}:00" for hour in range(24) for minute in range(0, 60, 30)]


def smart_meters_load_forecasting_processing(data_cursor: pymongo.cursor, file_id: str, local_dir_name: str) -> str:
    log = get_dagster_logger()

    # create time intervals columns
    columns = ["device_id", "tag_name", "date"] + TIME_INTERVALS
    headers_df = pd.DataFrame(columns=columns)

    # create folder to store data
    folder_path = os.path.join(os.path.dirname(os.path.abspath(sys.modules['load_forecasting'].__file__)), "generated",
                               local_dir_name)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # create csv file initially only with headers
    file = os.path.join(folder_path, f'{file_id}.csv')

    if os.path.exists(file):
        log.error(f'{file} exists in folder and may not be complete.')
        raise Failure(description=f'{file} exists in folder and may not be complete.')

    headers_df.to_csv(file, mode='a', index=False)

    for doc in data_cursor:
        smart_meter = doc["device_id"]  # smart meter id
        date = doc["date"]  # date of measurements
        doc_df = pd.DataFrame(doc["meter"])  # time series data
        doc_df["datetime"] = pd.to_datetime(date + ' ' + doc_df["time"])  # add column datetime
        # drop time, quality, quality_detail, opc_quality columns
        doc_df.drop(columns=['time', 'quality', 'quality_detail', 'opc_quality'], axis=1, inplace=True)
        # group data by tag name to resample properly, remember that data at this point refers to a single smart meter and a single day
        grouped = doc_df.groupby('tag_name')
        # initialise empty DataFrame
        resampled_df = pd.DataFrame()

        for tag_name, group_data in grouped:
            # resample data within the tag_name group in 30 minutes intervals
            resampled_group = group_data.resample('30T', on='datetime', closed='right', label='right').agg(
                {'value': 'sum'}
            )

            # create a full day index
            full_day_date_range = pd.date_range(start=(date + ' ' + '00:00:00'), end=(date + ' ' + '23:59:59'),
                                                freq='30T')
            # reindex to expand to full day, even with NaNs
            resampled_group = resampled_group.reindex(full_day_date_range)
            resampled_group.reset_index(inplace=True)

            resampled_group["device_id"] = smart_meter  # add smart_meter id to data
            resampled_group["tag_name"] = tag_name  # add tag_name in data
            resampled_group["date"] = date  # add date in data
            resampled_group.rename(columns={'index': 'datetime'}, inplace=True)

            # Pivot the DataFrame
            pivoted_df = resampled_group.pivot(index=['device_id', 'tag_name', 'date'], columns='datetime',
                                               values='value')
            pivoted_df.columns = pivoted_df.columns.strftime('%H:%M:%S')

            # Concatenate the resampled group with the overall resampled DataFrame
            resampled_df = pd.concat([resampled_df, pivoted_df])

        # append data to monthly record
        resampled_df = resampled_df.reset_index()
        resampled_df.to_csv(file, mode='a', header=False, index=False)
        # TODO: remove
    log.info(f'File location: {file}')
    return file
