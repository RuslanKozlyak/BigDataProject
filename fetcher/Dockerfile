FROM python:3.9-slim

WORKDIR /app

COPY fetch_data.py /app/fetch_data.py

RUN pip install pymongo requests

CMD ["python", "fetch_data.py"]
