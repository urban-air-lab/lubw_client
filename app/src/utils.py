import os
import time
import json
import paho.mqtt.publish as publish
import requests
from requests.auth import AuthBase
import base64
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import numpy as np
from dotenv import load_dotenv
import logging
import sys
import yaml

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)


def get_timestamps_with_offset() -> tuple[str, str]:
    now = datetime.now(ZoneInfo("Europe/Berlin")).replace(minute=0, second=0, microsecond=0)
    start_time = now - timedelta(hours=3)
    end_time = now - timedelta(hours=2)
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%S')
    end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S')
    return end_time_str, start_time_str


class UTF8BasicAuth(AuthBase):
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def __call__(self, request):
        auth_str = f'{self.username}:{self.password}'
        b64_encoded = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
        request.headers['Authorization'] = f'Basic {b64_encoded}'
        return request


def get_config(file_path: str) -> dict:
    try:
        with open(os.path.join(os.path.dirname(__file__), file_path), 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error(f"No config found in directory")
    except IOError:
        logging.error(f"IOError: An I/O error occurred")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def fetch_station_data(station: str, components: list, start_time: str, end_time: str) -> pd.DataFrame | None:
    if components is None:
        raise ValueError(f"Unknown station: {station}")

    station_data = {}
    for component in components:
        params = {
            'komponente': component,
            'von': start_time,
            'bis': end_time,
            'station': station
        }

        next_link = None
        while True:
            try:
                data = get_lubw_data(next_link, params)
                extract_data(station_data, component, data)
                next_link = data.get('nextLink')
                if not next_link:
                    break

            except requests.exceptions.RequestException as e:
                logging.info(f"Error fetching data for {component} at {station}: {e}")
                return None
    if not station_data:
        return None
    station_dataframe = pd.DataFrame(list(station_data.values()))
    station_dataframe['datetime'] = pd.to_datetime(station_dataframe['datetime'])
    station_dataframe = station_dataframe.sort_values(by='datetime').reset_index(drop=True)
    return station_dataframe


def extract_data(station_data: dict, component: dict, data: dict):
    for entry in data['messwerte']:
        dt = entry['endZeit']
        value = entry['wert']
        if dt not in station_data:
            station_data[dt] = {'datetime': dt}
        station_data[dt][component] = value


def get_lubw_data(next_link: str, params: dict) -> dict:
    response = requests.get(next_link or os.getenv("LUBW_BASE_URL"), params=params,
                            auth=UTF8BasicAuth(os.getenv("LUBW_USERNAME"), os.getenv("LUBW_PASSWORD")))
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.json()


def convert_values(station_data: pd.DataFrame) -> pd.DataFrame:
    for col in station_data.columns:
        if col not in ["datetime", "datetime_utc", "unixtime"]:
            station_data[col] = station_data[col].astype(float)
    return station_data


def convert_timestamps(station_data: pd.DataFrame) -> pd.DataFrame:
    station_data['datetime_utc'] = pd.to_datetime(station_data['datetime']).dt.tz_convert('UTC').dt.strftime(
        '%Y-%m-%dT%H:%M:%S')
    station_data['unix_time'] = (station_data['datetime'].astype(np.int64) // 10 ** 9).astype(int)
    return station_data


def publish_sensor_data(data: pd.DataFrame, topic: str) -> None:
    # TODO: works, but needs refactoring :)
    json_str = data.to_json(orient='records')
    payload = json.loads(json_str)[0]
    payload = json.dumps(payload)

    publish.single(
        topic=topic,
        payload=payload,
        hostname=os.getenv("MQTT_SERVER"),
        port=int(os.getenv("MQTT_PORT"))
    )
    logging.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Published to {topic}: {payload}")