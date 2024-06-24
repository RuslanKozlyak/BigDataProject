FROM bitnami/spark:latest

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark-job.py .
WORKDIR ..
