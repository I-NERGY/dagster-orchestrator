from dagster import EnvVar
from .s3_resource import S3Resource
from .mongo_resource import MongoResource
from .forecasting_service import LoadForecastingService
from .clustering_service import ClustersPredictionService
from .flexdr_service import FlexDRService

mongo_source = {"mongo_conn": MongoResource(
    user=EnvVar("MONGO_USER"),
    password=EnvVar("MONGO_PASS"),
    address=EnvVar("MONGO_ADDRESS"),
    database=EnvVar("MONGO_DB_NAME"),
    collection=EnvVar("MONGO_COLLECTION")),
}

forecasting_service_source = {"forecasting_service": LoadForecastingService(
    endpoint=EnvVar("LOAD_FORECASTING_SERVICE_URL")
)}

clustering_service_source = {"clustering_service": ClustersPredictionService(
    endpoint=EnvVar("CLUSTERING_SERVICE_URL")
)}

RESOURCES_LOCAL = {
    **clustering_service_source,
    **forecasting_service_source,
    **mongo_source,
    "flexdr_service": FlexDRService(
        endpoint=EnvVar("LOCAL_FLEX_DR_ENDPOINT")
    ),
    "s3_conn": S3Resource(
        url=EnvVar("LOCAL_S3_URL"),
        access_key=EnvVar("LOCAL_S3_ACCESS_KEY"),
        secret_key=EnvVar("LOCAL_S3_SECRET_KEY"),
    ),
    "flex_dr_mongo_conn": MongoResource(
        user=EnvVar("LOCAL_FLEX_DR_MONGO_USER"),
        password=EnvVar("LOCAL_FLEX_DR_MONGO_PASS"),
        address=EnvVar("LOCAL_FLEX_DR_MONGO_ADDRESS"),
        database=EnvVar("LOCAL_FLEX_DR_MONGO_DB_NAME"),
    )
}

RESOURCES_PROD = {
    **clustering_service_source,
    **forecasting_service_source,
    **mongo_source,
    "flexdr_service": FlexDRService(
        endpoint=EnvVar("FLEX_DR_ENDPOINT")
    ),
    "s3_conn": S3Resource(
        url=EnvVar("PROD_S3_URL"),
        access_key=EnvVar("PROD_S3_ACCESS_KEY"),
        secret_key=EnvVar("PROD_S3_SECRET_KEY"),
    ),
    "flex_dr_mongo_conn": MongoResource(
        user=EnvVar("LOCAL_FLEX_DR_MONGO_USER"),
        password=EnvVar("LOCAL_FLEX_DR_MONGO_PASS"),
        address=EnvVar("LOCAL_FLEX_DR_MONGO_ADDRESS"),
        database=EnvVar("LOCAL_FLEX_DR_MONGO_DB_NAME"),
    )
}
