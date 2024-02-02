from typing import List
from dagster import ConfigurableResource
import requests
from requests import Response


class FlexDRService(ConfigurableResource):
    endpoint: str

    def insert_forecasts(self, device_id: str, cluster: int, day_ahead_load: List[float],
                         forecast_date: str) -> Response:
        payload = {
            'meter_id': device_id,
            'ml_model_id': '64f99f5dd7dccbfc89e01ef2',
            'cluster_assigned': cluster,
            'forecasted_load': day_ahead_load,
            'forecast_date': forecast_date
        }
        response = requests.post(url=f'{self.endpoint}/assignments/new', json=payload)
        return response
