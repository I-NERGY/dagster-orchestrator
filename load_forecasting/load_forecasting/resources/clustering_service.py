import pandas as pd
from dagster import ConfigurableResource
import requests
from requests import Response
import json


class ClustersPredictionService(ConfigurableResource):
    endpoint: str

    def predict(self, timeseries: pd.DataFrame) -> Response:
        payload = json.dumps(timeseries.values.tolist())
        response = requests.post(
            f"{self.endpoint}/predict_clustering",
            data=payload,
        )
        return response
