import requests 
from datetime import date

class ApiClient:
    url = "https://archive-api.open-meteo.com/v1/archive"


    def fetch_historical_weather(self, latitude: float, longitude: float, start_date: date, end_date: date, timezone: str) -> dict:
        params = self._build_request_params(latitude, longitude, start_date, end_date, timezone)
        return self._make_request(self.url, params)


    def _build_request_params(self, latitude: float, longitude: float, start_date: date, end_date: date, timezone: str) -> dict:
        return {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "timezone": timezone,
            "hourly": "temperature_2m,"
                    "apparent_temperature,"
                    "relative_humidity_2m,"
                    "precipitation,"
                    "pressure_msl,"
                    "cloud_cover,"
                    "wind_speed_10m,"
                    "wind_direction_10m,"
                    "weather_code"
    }

    def _make_request(self, url: str, params: dict) -> dict:
        try:
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.Timeout as error:
            raise RuntimeError(
                "A requisição à Open-Meteo excedeu o tempo limite."
            ) from error
        except requests.exceptions.ConnectionError as error:
            raise RuntimeError(
                "Não foi possível conectar à Open-Meteo."
            ) from error

        except requests.exceptions.HTTPError as error:
            status_code = (
                error.response.status_code
                if error.response is not None
                else "desconhecido"
            )
            raise RuntimeError(
                f"Erro HTTP {status_code} ao acessar a Open-Meteo."
            ) from error

        except requests.exceptions.RequestException as error:
            raise RuntimeError(
                "Ocorreu um erro inesperado ao realizar a requisição."
            ) from error