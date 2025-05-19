FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
COPY app/src/fetch_and_publish_lubw_data.py .

RUN pip3 install --no-cache-dir -r requirements.txt
CMD ["python3", "fetch_and_publish_lubw_data.py"]