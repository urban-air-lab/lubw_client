FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["python", "app/src/fetch_and_publish_lubw_hourly.py"]