import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from app.src.utils import *


def main():
    # date format '%Y-%m-%dT%H:%M:%S' -> '2025-05-10T00:00:00+01:00'
    start_time_str = '2025-05-18T00:00:00+01:00'
    end_time_str = '2025-05-19T00:00:00+01:00'
    date_range = pd.date_range(start=start_time_str, end=end_time_str, freq='h')
    station_components = get_config("./stations.yaml")

    logging.info(f"start fetching lubw data from {start_time_str} to {end_time_str}")

    for date_time in date_range:
        for station in station_components.keys():
            station_data = fetch_station_data(station, station_components[station], date_time, date_time + timedelta(hours=1))

            if station_data is None:
                logging.error(f"No data received from station: {station}")
                return

            station_data = convert_timestamps(station_data)
            station_data = convert_values(station_data)
            try:
                publish_sensor_data(station_data, f"sensors/lubw-hour/{station}")
            except Exception as e:
                logging.info(f"Could not publish data at  time {start_time_str} for {station}, {e}")

    logging.info("Finished fetching data")


if __name__ == "__main__":
    main()

