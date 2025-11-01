FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:0.9.6 /uv /uvx /bin/

ADD . /app
WORKDIR /app

RUN uv sync --locked


CMD ["uv", "run",  "app/src/fetch_and_publish_lubw_hourly.py"]