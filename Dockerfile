FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:0.9.6 /uv /uvx /bin/

COPY . .
WORKDIR /app

RUN uv sync --locked

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["uv", "run",  "app/src/fetch_and_publish_lubw_hourly.py"]