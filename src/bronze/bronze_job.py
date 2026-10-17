from datetime import date

from src.bronze.api_client import ApiClient
from src.bronze.writer import BronzeWriter


class BronzeJob:
    def __init__(
        self,
        api_client: ApiClient,
        bronze_writer: BronzeWriter,
    ) -> None:
        self.api_client = api_client
        self.bronze_writer = bronze_writer

    def run(
        self,
        city: str,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        timezone: str,
    ) -> str:
        payload = self.api_client.fetch_historical_weather(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
        )

        return self.bronze_writer.write(
            payload=payload,
            city=city,
            latitude=latitude,
            longitude=longitude,
        )