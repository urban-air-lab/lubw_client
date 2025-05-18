import os
import time
import json
import paho.mqtt.publish as publish
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
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

def get_timestamps_with_offset():
    germany_tz = ZoneInfo("Europe/Berlin")
    end_time = datetime.now(germany_tz)
    end_time = end_time.replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=3)
    end_time = start_time + timedelta(hours=1)
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%S')
    end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S')
    return end_time_str, start_time_str


class UTF8BasicAuth(AuthBase):
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __call__(self, r):
        auth_str = f'{self.username}:{self.password}'
        b64_encoded = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
        r.headers['Authorization'] = f'Basic {b64_encoded}'
        return r


def get_config(file_path: str) -> dict:
    try:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error(f"No config found in directory")
    except IOError:
        logging.error(f"IOError: An I/O error occurred")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def fetch_station_data(station, components, start_time, end_time):
    if components is None:
        raise ValueError(f"Unknown station: {station}")

    all_data = {}
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
                extract_data(all_data, component, data)
                next_link = data.get('nextLink')
                if not next_link:
                    break

            except requests.exceptions.RequestException as e:
                logging.info(f"Error fetching data for {component} at {station}: {e}")
                return None

    df = pd.DataFrame(list(all_data.values()))
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values(by='datetime').reset_index(drop=True)
    logging.info(df)
    return df


def extract_data(all_data, component, data):
    for entry in data['messwerte']:
        dt = entry['endZeit']
        value = entry['wert']
        if dt not in all_data:
            all_data[dt] = {'datetime': dt}
        all_data[dt][component] = value


def get_lubw_data(next_link, params):
    response = requests.get(next_link or os.getenv("LUBW_BASE_URL"), params=params,
                            auth=UTF8BasicAuth(os.getenv("LUBW_USERNAME"), os.getenv("LUBW_PASSWORD")))
    response.raise_for_status()
    response.encoding = 'utf-8'
    data = response.json()
    return data


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


def main():
    end_time_str, start_time_str = get_timestamps_with_offset()
    station_components = get_config("./stations.yaml")

    for station in station_components.keys():
        df = fetch_station_data(station, station_components[station], start_time_str, end_time_str)
        if df is not None:  # TODO: Invert if clause to fail early
            df['datetime_utc'] = pd.to_datetime(df['datetime']).dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%S')
            df['unix_time'] = (df['datetime'].astype(np.int64) // 10 ** 9).astype(int)

            # --- CONVERT ALL COLUMNS (EXCEPT `datetime`) TO FLOAT ---
            for col in df.columns:
                if col not in ["datetime", "datetime_utc", "unixtime"]:
                    df[col] = df[col].astype(float)  # Ensure float type
            try:
                publish_sensor_data(df, f"sensors/lubw-hour/{station}")
            except Exception as e:
                logging.info(f"Could not publish data at  time {start_time_str} for {station}, {e}")


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'interval', hours=1, next_run_time=datetime.now())
    logging.info("Starting scheduler...")
    scheduler.start()
