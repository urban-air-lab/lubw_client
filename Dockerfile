FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY fetch_and_publish_lubw_data.py .

CMD ["python", "fetch_and_publish_lubw_data.py"]