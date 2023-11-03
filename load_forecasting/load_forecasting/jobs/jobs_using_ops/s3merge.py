from typing import List
from minio import Minio
import pandas as pd
import minio
import logging
import os

client = Minio(
    "localhost:9000",
    access_key="accessminio",
    secret_key="secretminio",
    secure=False
)


def generate_single_file(months_range: List[str], file_name: str) -> None:
    time_intervals_cols = [f"{hour:02d}:{minute:02d}:00" for hour in range(24) for minute in range(0, 60, 30)]
    columns = ["device_id", "tag_name", "date"] + time_intervals_cols
    headers_df = pd.DataFrame(columns=columns)
    headers_df.to_csv(file_name, mode='a', index=False)

    for m in months_range:
        try:
            response = client.get_object("load-forecasting", f"smart_meters_monthly_data/{m}.csv")
            df = pd.read_csv(response)
            df.to_csv(file_name, mode='a', index=False, header=False)
        except minio.S3Error as s3e:
            if s3e.code == "NoSuchKey":
                logging.warning(f'No key found corresponding to {m}')
                continue
            else:
                raise s3e
        finally:
            try:
                response.close()
                response.release_conn()
            except NameError as ne:
                logging.info(ne)


if __name__ == '__main__':
    range_2021 = [f'{month:02d}_2021' for month in range(1, 13)]
    range_2022 = [f'{month:02d}_2022' for month in range(1, 13)]
    range_2023 = [f'{month:02d}_2023' for month in range(1, 13)]
    full_range = range_2021 + range_2022 + range_2023
    file_name = "full.csv"
    if os.path.exists(file_name):
        raise Exception(f"File with name {file_name} already exists.")
    generate_single_file(months_range=full_range, file_name=file_name)
