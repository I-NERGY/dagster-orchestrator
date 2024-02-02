import os
import sys
import pandas as pd
import pymongo
from typing import Tuple
from dagster import Failure, get_dagster_logger
from math import nan

PRODUCTION_TAG_NAME = '2_8_0'
CONSUMPTION_TAG_NAME = '1_8_0'

TIME_INTERVALS = {
    '30': [f"{hour:02d}:{minute:02d}:00" for hour in range(24) for minute in range(0, 60, 30)],
    '60': [f"{hour:02d}:{minute:02d}:00" for hour in range(24) for minute in range(0, 60, 60)],
}
RESAMPLING_FREQS = {
    '30': '30T',
    '60': '1H',
}
DEFAULT_TIME_INTERVALS = TIME_INTERVALS['60']
DEFAULT_RESAMPLING_FREQ = RESAMPLING_FREQS['60']

smart_meters_specs_df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'smart_meter_description.csv'))


def retrieve_specs(device_id: str) -> Tuple[bool, float, bool, float]:
    log = get_dagster_logger()
    try:
        smart_meter_specs = smart_meters_specs_df.loc[(smart_meters_specs_df['id'] == device_id)].iloc[0].to_dict()
        production_max = smart_meter_specs['Production (kW)']
        consumption_max = smart_meter_specs['Contractual power (kW)']
        return production_max >= 0, production_max, consumption_max >= 0, consumption_max
    except IndexError:
        # iloc[0] index error when smart meter is missing from csv with specs.
        log.warning(f'Smart meter {device_id} does not exist in contract')
        return False, nan, False, nan


def remove_outliers(sm_df: pd.DataFrame, max_value: float, contract_exists: bool) -> None:
    min_value = 0
    max_value = max_value if contract_exists else 200
    if contract_exists and max_value == 0:
        sm_df['value'] = nan
    else:
        sm_df.loc[(sm_df['value'] < min_value) | (sm_df['value'] > max_value), 'value'] = nan


def apply_naming_convention(smart_meter_name: str) -> str:
    # Fix missing B in smart meters name
    if smart_meter_name.startswith('BB'):
        if not smart_meter_name.startswith('BBB'):
            smart_meter_name = 'B' + smart_meter_name
    else:
        raise Failure(description=f'Smart meter {smart_meter_name} does not follow the BBB naming convention')
    return smart_meter_name


def smart_meters_load_forecasting_processing(data_cursor: pymongo.cursor, file_id: str, local_dir_name: str) -> str:
    log = get_dagster_logger()

    # create time intervals columns
    columns = ["device_id", "tag_name", "date"] + DEFAULT_TIME_INTERVALS
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
        smart_meter = apply_naming_convention(doc["device_id"])  # smart meter id
        # fetch smart meters specs
        supports_prod, prod_max, supports_cons, cons_max = retrieve_specs(device_id=smart_meter)
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
            resampled_group = group_data.resample(DEFAULT_RESAMPLING_FREQ, on='datetime', closed='left',
                                                  label='left').agg({'value': 'mean'})

            # create a full day index
            full_day_date_range = pd.date_range(start=(date + ' ' + '00:00:00'), end=(date + ' ' + '23:59:59'),
                                                freq=DEFAULT_RESAMPLING_FREQ)
            # reindex to expand to full day, even with NaNs
            resampled_group = resampled_group.reindex(full_day_date_range)
            resampled_group.reset_index(inplace=True)

            resampled_group["device_id"] = smart_meter  # add smart_meter id to data
            resampled_group["tag_name"] = tag_name  # add tag_name in data
            resampled_group["date"] = date  # add date in data
            resampled_group.rename(columns={'index': 'datetime'}, inplace=True)

            # remove outliers based on contractual power and production
            # in place operation
            if PRODUCTION_TAG_NAME in tag_name:
                remove_outliers(sm_df=resampled_group, max_value=prod_max, contract_exists=supports_prod)
            elif CONSUMPTION_TAG_NAME in tag_name:
                remove_outliers(sm_df=resampled_group, max_value=cons_max, contract_exists=supports_cons)
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
