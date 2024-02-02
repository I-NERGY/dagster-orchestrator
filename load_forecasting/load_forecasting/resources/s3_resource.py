import minio.datatypes
from typing import Iterator
from dagster import get_dagster_logger
from minio import Minio
from dagster import ConfigurableResource
from pydantic import PrivateAttr


class S3Resource(ConfigurableResource):
    url: str
    access_key: str
    secret_key: str
    _client: Minio = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        self._client = Minio(
            self.url,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,
        )

    def store(self, bucket: str, file_path: str, file_name: str) -> None:
        log = get_dagster_logger()
        found = self._client.bucket_exists(bucket)
        if not found:
            self._client.make_bucket(bucket)
            log.info(f"Bucket {bucket} created")
        else:
            log.info(f"Bucket {bucket} already exists")

        self._client.fput_object(bucket, file_name, file_path)
        log.info(f"File {file_name} uploaded to minio")

    def list_objects(self, bucket: str, prefix: str) -> Iterator[minio.datatypes.Object]:
        objects = self._client.list_objects(bucket_name=bucket, prefix=prefix, recursive=True)
        return objects

    def get_object(self, bucket: str, object_name: str):
        log = get_dagster_logger()
        try:
            response = self._client.get_object(bucket_name=bucket, object_name=object_name)
            return response.data
        except minio.S3Error as s3e:
            log.error(f"Error in fetching object {object_name} - {s3e}")
        finally:
            response.close()
            response.release_conn()
