## lubw client

This repository contains the client to access the lubw api

## Setup
The projects dependencies are managed with uv. To install and get more information about uv, follow this documentation:
```
https://docs.astral.sh/uv/getting-started/installation/
```

To install the dependencies run

```
uv sync --locked
```

To connect to UrbanAirLabs InfluxDB or Mosquitto (MQTT Broker) the clients need a .env file containing the necessary 
credentials and route information e.g. domain, port, etc. You can get this information from you Supervisor. 

## Run Tests
Tests are base on Pytest - run all tests via command line:

```
uv run pytest
```