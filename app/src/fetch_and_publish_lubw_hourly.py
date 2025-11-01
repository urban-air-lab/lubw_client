from apscheduler.schedulers.blocking import BlockingScheduler
from app.src.utils import *


def main():
    end_time_str, start_time_str = get_timestamps_with_offset()
    station_components = get_config("./stations.yaml")

    for station in station_components.keys():
        station_data = fetch_station_data(station, station_components[station], start_time_str, end_time_str)

        if station_data is None:
            logging.error(f"No data received from station: {station}")
            continue

        station_data = convert_timestamps(station_data)
        station_data = convert_values(station_data)
        try:
            publish_sensor_data(station_data, f"sensors/lubw-hour/{station}")
        except Exception as e:
            logging.info(f"Could not publish data at  time {start_time_str} for {station}, {e}")


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'interval', hours=1, next_run_time=datetime.now())
    logging.info("Starting scheduler...")
    scheduler.start()
