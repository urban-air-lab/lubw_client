from app.src.utils import *


def main():
    # date format '%Y-%m-%dT%H:%M:%S' -> '2025-05-10T00:00:00+01:00'
    start_time_str = '2024-10-15T00:00:00+01:00'
    end_time_str = '2025-05-25T00:00:00+01:00'
    date_range = pd.date_range(start=start_time_str, end=end_time_str, freq='h')
    station_components = get_config("./stations.yaml")

    chunk_size = 100
    chunks = [date_range[i:i + chunk_size] for i in range(0, len(date_range), chunk_size)]

    logging.info(f"start fetching lubw data from {start_time_str} to {end_time_str}")

    for chunk in chunks:
        for station in station_components.keys():
            station_data = fetch_station_data(station, station_components[station], chunk[0], chunk[-1])

            if station_data is None:
                logging.error(f"No data received from station: {station}")
                continue

            station_data = convert_timestamps(station_data)
            station_data = convert_values(station_data)
            try:
                publish_sensor_data(station_data, f"sensors/lubw-hour/{station}")
            except Exception as e:
                logging.info(f"Could not publish data at  time {start_time_str} for {station}, {e}")

    logging.info("Finished fetching data")


if __name__ == "__main__":
    main()

