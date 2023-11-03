from dagster import EnvVar
from .s3_resource import S3Resource
from .mongo_resource import MongoResource

mongo_source = {"mongo_conn": MongoResource(
    user=EnvVar("MONGO_USER"),
    password=EnvVar("MONGO_PASS"),
    address=EnvVar("MONGO_ADDRESS"),
    database=EnvVar("MONGO_DB_NAME"),
    collection=EnvVar("MONGO_COLLECTION")
), }

RESOURCES_LOCAL = {
    **mongo_source,
    "s3_conn": S3Resource(
        url=EnvVar("LOCAL_S3_URL"),
        access_key=EnvVar("LOCAL_S3_ACCESS_KEY"),
        secret_key=EnvVar("LOCAL_S3_SECRET_KEY"),
    )
}

RESOURCES_PROD = {
    **mongo_source,
    "s3_conn": S3Resource(
        url=EnvVar("PROD_S3_URL"),
        access_key=EnvVar("PROD_S3_ACCESS_KEY"),
        secret_key=EnvVar("PROD_S3_SECRET_KEY"),
    )
}
