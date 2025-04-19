import os

import requests
from requests.auth import AuthBase
import base64
import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import numpy as np
from influxdb_client import InfluxDBClient, Point, WriteOptions
from dotenv import load_dotenv

load_dotenv()


class UTF8BasicAuth(AuthBase):
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __call__(self, r):
        auth_str = f'{self.username}:{self.password}'
        b64_encoded = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
        r.headers['Authorization'] = f'Basic {b64_encoded}'
        return r


#TODO: config file
station_components = {
    "DEBW015": ['PM10', 'PM2.5', 'NO', 'NO2', 'O3', 'TEMP', 'RLF', 'NSCH', 'STRG', 'WIV', 'WIR'],
    "DEBW152": ['NO2', 'CO']
}

def fetch_station_data(station, start_time, end_time):
    components = station_components.get(station)
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

        # TODO: Should never be a next link, since only asked for 1 hour entry
        next_link = None
        while True:
            try:
                response = requests.get(next_link or os.getenv("LUBW_BASE_URL"), params=params, auth=UTF8BasicAuth(os.getenv("LUBW_USERNAME"), os.getenv("LUBW_PASSWORD")))
                response.raise_for_status()
                response.encoding = 'utf-8'
                data = response.json()

                if 'messwerte' not in data or not isinstance(data['messwerte'], list):
                    break

                # TODO: Should always just one entry, since only 1 hour is requested
                # TODO: No loop needed, no outer dict needed
                for entry in data['messwerte']:
                    dt = entry['endZeit']
                    value = entry['wert']
                    if dt not in all_data:
                        all_data[dt] = {'datetime': dt}
                    all_data[dt][component] = value

                # TODO: Should never be a next link
                next_link = data.get('nextLink')
                if not next_link:
                    break

            except requests.exceptions.RequestException as e:
                print(f"Error fetching data for {component} at {station}: {e}")
                return None

    df = pd.DataFrame(list(all_data.values()))
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values(by='datetime').reset_index(drop=True) #TODO: Not neede when always asked for 1 hour
    print(df)
    return df


def rename_columns(df, column_mapping):
    existing_columns = {old: new for old, new in column_mapping.items() if old in df.columns}
    df.rename(columns=existing_columns, inplace=True)
    return df


def main():
    germany_tz = ZoneInfo("Europe/Berlin")
    end_time = datetime.now(germany_tz)
    end_time = end_time.replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=3)
    end_time = start_time + timedelta(hours=1) # Erzeugt Zeitversatz von zwei Stunden
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%S')
    end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S')

    for station in station_components.keys():
        df = fetch_station_data(station, start_time_str, end_time_str)
        if df is not None: # TODO: Invert if clause to fail early
            df['datetime_utc'] = pd.to_datetime(df['datetime']).dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%S')
            df['unix_time'] = (df['datetime'].astype(np.int64) // 10 ** 9).astype(int)

            # --- CONVERT ALL COLUMNS (EXCEPT `datetime`) TO FLOAT ---
            for col in df.columns:
                if col not in ["datetime", "datetime_utc", "unixtime"]:
                    df[col] = df[col].astype(float)  # Ensure float type

            # TODO: Send df to Mosquitto to have same route then ual sensors
            # --- CONFIGURE INFLUXDB CONNECTION ---
            INFLUXDB_URL = "http://localhost:8086"  # Change if needed
            INFLUXDB_TOKEN = os.getenv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN")
            INFLUXDB_ORG = "urban-air-lab"
            INFLUXDB_BUCKET = "lubw-hour"

            # --- CONNECT TO INFLUXDB ---
            client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

            # --- WRITE DATAFRAME TO INFLUXDB ---
            with client.write_api(write_options=WriteOptions(batch_size=500, flush_interval=10_000)) as write_api:
                for _, row in df.iterrows():
                    if station == "DEBW015":
                        point = (
                            Point(station)  # Measurement name
                            .time(row["datetime"])  # Timestamp column
                            .field("PM10", row["PM10"])
                            .field("PM2p5", row["PM2.5"])
                            .field("NO2", row["NO2"])
                            .field("RLF", row["RLF"])
                            .field("NSCH", row["NSCH"])
                            .field("STRG", row["STRG"])
                            .field("WIV", row["WIV"])
                            .field("TEMP", row["TEMP"])
                            .field("O3", row["O3"])
                            .field("NO", row["NO"])
                            .field("WIR", row["WIR"])
                        )

                    elif station == "DEBW152":
                        point = (
                            Point(station)  # Measurement name
                            .time(row["datetime"])  # Timestamp column
                            .field("NO2", row["NO2"])
                            .field("CO", row["CO"])
                        )

                    else:
                        print(f"Station {station} not mapped for transfer to influx")
                        break

                    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

            print("Wait for write to fininsh...")
            client.close()
            print("Data successfully written to InfluxDB!")




# Run the main function
# TODO: Run main function in scedular
if __name__ == "__main__":
    main()