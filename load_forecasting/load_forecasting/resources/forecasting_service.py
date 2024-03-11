import pandas as pd
from dagster import ConfigurableResource
import requests
from requests import Response
from requests_toolbelt import MultipartEncoder


class LoadForecastingService(ConfigurableResource):
    endpoint: str

    def predict(self, timeseries: pd.DataFrame, horizon: int, device_id: str) -> Response:
        payload = MultipartEncoder(
            fields={
                "n": str(horizon),
                "series": timeseries.to_json(),
                "meter_id": device_id,
            }
        )
        response = requests.post(
            f"{self.endpoint}/predict_uc7",
            data=payload,
            headers={"Content-Type": payload.content_type}
        )
        return response
