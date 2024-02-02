from typing import Optional
import pymongo
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import PrivateAttr


class MongoResource(ConfigurableResource):
    user: str
    password: str
    address: str
    database: str
    collection: Optional[str]
    _client: pymongo.MongoClient = PrivateAttr()
    _db: pymongo.database.Database = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        log = get_dagster_logger()
        log.info('Initializing mongo connection....')
        self._client = pymongo.MongoClient(f'mongodb://{self.user}:{self.password}@{self.address}')
        self._db = self._client[self.database]

    def aggregate(self, pipeline, collection: str = None) -> pymongo.cursor:
        if collection is None:
            collection = self.collection
        result = self._db[collection].aggregate(pipeline)
        return result

    def get_database(self):
        return self._db

    def teardown_after_execution(self, context) -> None:
        log = get_dagster_logger()
        log.info('Closing mongo connection....')
        self._client.close()
