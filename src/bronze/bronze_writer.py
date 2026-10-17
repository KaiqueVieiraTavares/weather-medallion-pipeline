from datetime import datetime, timezone
from typing import Any
import json 


class BronzeWriter:

    def __init__(self, s3_client: Any, bucket: str):
        self.s3_client = s3_client
        self.bucket = bucket

    def write(self, payload: dict[str, Any], city: str, latitude: float, longitude: float) -> str:
        ingestion_time = datetime.now(timezone.utc)
        key: str = self._build_key(city=city, ingestion_time = ingestion_time)
        record: dict[str, Any] = {
            "metadata": {
                "city": city,
                "latitude": latitude,
                "longitude": longitude,
                "ingestion_timestamp": ingestion_time.isoformat(),
                "source": "open-meteo",
            },
            "payload": payload,
        }
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(
                record,
                ensure_ascii=False,
            ).encode("utf-8"),
            ContentType="application/json",
        )

        return f"s3://{self.bucket}/{key}"

    @staticmethod
    def _build_key(city: str, ingestion_time: datetime) -> str:
        city_slug = city.strip().lower().replace(" ", "-")
        return (
                "bronze/open-meteo/"
                f"year={ingestion_time:%Y}/"
                f"month={ingestion_time:%m}/"
                f"day={ingestion_time:%d}/"
                f"city={city_slug}/"
                f"weather_{ingestion_time:%Y%m%dT%H%M%S%f}.json"
            )