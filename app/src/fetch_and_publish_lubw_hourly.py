from apscheduler.schedulers.blocking import BlockingScheduler
from ual.mqtt.mqtt_client import MQTTClient

from app.src.utils import *


def main():
    end_time_str, start_time_str = get_timestamps_with_offset()
    station_components = get_config("./stations.yaml")
    # TODO: Dont generate everytime, give as parameter
    mqtt_client: MQTTClient = MQTTClient(os.getenv("MQTT_SERVER"), int(os.getenv("MQTT_PORT")),
                                         os.getenv("MQTT_USERNAME"), os.getenv("MQTT_PASSWORD"))


    for station in station_components.keys():
        station_data = fetch_station_data(station, station_components[station], start_time_str, end_time_str)

        if station_data is None:
            logging.error(f"No data received from station: {station}")
            continue

        station_data = convert_timestamps(station_data)
        station_data = convert_values(station_data)
        station_data["datetime"] = station_data["datetime"].astype(str)
        station_data = station_data.to_dict(orient="records")
        for element in station_data:
            mqtt_client.publish_data(element, f"sensors/lubw-hour/{station}")

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'cron', minute=0)
    logging.info("Starting scheduler...")
    scheduler.start()
