from dagster import EnvVar
from .s3_resource import S3Resource
from .mongo_resource import MongoResource
from .forecasting_service import LoadForecastingService
from .clustering_service import ClustersPredictionService
from .flexdr_service import FlexDRService

inergy_db_mongo_source = {"mongo_conn": MongoResource(
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

minio_conn = {"s3_conn": S3Resource(
    url=EnvVar("MINIO_ENDPOINT_URL"),
    access_key=EnvVar("MINIO_ACCESS_KEY"),
    secret_key=EnvVar("MINIO_SECRET_KEY"),
)}

flex_dr_mongo_db = {"flex_dr_mongo_conn": MongoResource(
    user=EnvVar("FLEX_DR_MONGO_USER"),
    password=EnvVar("FLEX_DR_MONGO_PASS"),
    address=EnvVar("FLEX_DR_MONGO_ADDRESS"),
    database=EnvVar("FLEX_DR_MONGO_DB_NAME"),
)}

flex_dr_service_source = {"flexdr_service": FlexDRService(
    endpoint=EnvVar("FLEX_DR_ENDPOINT")
), }

RESOURCES_ENV_BY_FILE = {
    **clustering_service_source,
    **forecasting_service_source,
    **inergy_db_mongo_source,
    **minio_conn,
    **flex_dr_mongo_db,
    **flex_dr_service_source,
}

# RESOURCES_LOCAL = {
#     **clustering_service_source,
#     **forecasting_service_source,
#     **inergy_db_mongo_source,
#     "flexdr_service": FlexDRService(
#         endpoint=EnvVar("FLEX_DR_ENDPOINT")
#     ),
#     "s3_conn": S3Resource(
#         url=EnvVar("MINIO_ENDPOINT_URL"),
#         access_key=EnvVar("MINIO_ACCESS_KEY"),
#         secret_key=EnvVar("MINIO_SECRET_KEY"),
#     ),
#     "flex_dr_mongo_conn": MongoResource(
#         user=EnvVar("FLEX_DR_MONGO_USER"),
#         password=EnvVar("FLEX_DR_MONGO_PASS"),
#         address=EnvVar("FLEX_DR_MONGO_ADDRESS"),
#         database=EnvVar("FLEX_DR_MONGO_DB_NAME"),
#     )
# }
#
# RESOURCES_PROD = {
#     **clustering_service_source,
#     **forecasting_service_source,
#     **inergy_db_mongo_source,
#     "flexdr_service": FlexDRService(
#         endpoint=EnvVar("FLEX_DR_ENDPOINT")
#     ),
#     "s3_conn": S3Resource(
#         url=EnvVar("PROD_S3_URL"),
#         access_key=EnvVar("PROD_S3_ACCESS_KEY"),
#         secret_key=EnvVar("PROD_S3_SECRET_KEY"),
#     ),
#     "flex_dr_mongo_conn": MongoResource(
#         user=EnvVar("FLEX_DR_MONGO_USER"),
#         password=EnvVar("FLEX_DR_MONGO_PASS"),
#         address=EnvVar("FLEX_DR_MONGO_ADDRESS"),
#         database=EnvVar("FLEX_DR_MONGO_DB_NAME"),
#     )
# }
