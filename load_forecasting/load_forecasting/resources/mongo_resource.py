import pymongo
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import PrivateAttr


class MongoResource(ConfigurableResource):
    user: str
    password: str
    address: str
    database: str
    collection: str
    _client: pymongo.MongoClient = PrivateAttr()
    _collection: pymongo.collection = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        log = get_dagster_logger()
        log.info('Initializing mongo connection....')
        self._client = pymongo.MongoClient(f'mongodb://{self.user}:{self.password}@{self.address}/{self.database}')
        self._collection = self._client[self.database][self.collection]

    def aggregate(self, pipeline) -> pymongo.cursor:
        result = self._collection.aggregate(pipeline)
        return result

    def teardown_after_execution(self, context) -> None:
        log = get_dagster_logger()
        log.info('Closing mongo connection....')
        self._client.close()
