import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import datetime
import time
import json
import time
from pymongo import MongoClient
import os
import requests
from datetime import datetime, date, timedelta
import time

def fetch_weather(lat, lon, start, end):
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Define parameters for API request
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"]
    }

    # Fetch weather data
    responses = openmeteo.weather_api(url, params=params)

    # Process first location
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(2).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(3).ValuesAsNumpy()

    # Create DataFrame from hourly data
    hourly_data = {
        "timestamp": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "temperature_2m": hourly_temperature_2m,
        "relative_humidity_2m": hourly_relative_humidity_2m,
        "precipitation": hourly_precipitation,
        "wind_speed_10m": hourly_wind_speed_10m
    }

    hourly_dataframe = pd.DataFrame(data=hourly_data)

    return json.loads(hourly_dataframe.to_json(orient="records"))


# Function to fetch air pollution data
def fetch_air_pollution_data(lat, lon, start, end, api_key):
    url = f'http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code}")
        return None
    response = response.json()

    # Extract and preprocess historical data
    data = []
    for record in response['list']:
        data.append({
            'timestamp': datetime.utcfromtimestamp(record['dt']),
            'aqi': float(record['main']['aqi']),
            'co': float(record['components']['co']),
            'no': float(record['components']['no']),
            'no2': float(record['components']['no2']),
            'o3': float(record['components']['o3']),
            'so2': float(record['components']['so2']),
            'pm2_5': float(record['components']['pm2_5']),
            'pm10': float(record['components']['pm10']),
            'nh3': float(record['components']['nh3'])
        })

    # Create Pandas DataFrame
    air_quality_df = pd.DataFrame(data)

    air_quality_df['timestamp'] = air_quality_df['timestamp'].dt.tz_localize('UTC')
    return json.loads(air_quality_df.to_json(orient="records"))


# MongoDB configuration
MONGO_URI = 'mongodb://mongodb:27017/'
DB_NAME = 'movies_db'

WETHER_BD = 'wether_db'

WEATHER_TABLE = 'weather'
QUALITY_TABLE = 'quality'

def main():
    # API key and coordinates
    api_key = os.environ.get('API_KEY')
    lat = float(os.environ.get('LAT')) 
    lon = float(os.environ.get('LON'))

    # Dates for the specified period in unix time
    end_date = date.today()  - timedelta(days=5)
    start_date = date.today() - timedelta(days=365*2)

    start_date = datetime(2022, 1, 1)
    end_date = datetime(2024, 6, 20)

    client = MongoClient(MONGO_URI)
    db = client[WETHER_BD]

    print('Downloading weather data from {} to {}'.format(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    
    weather_data = fetch_weather(lat, lon, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

    start_date = datetime.combine(start_date, datetime.min.time())
    end_date = datetime.combine(end_date, datetime.min.time())

    quality_data = fetch_air_pollution_data(lat, lon, int(start_date.timestamp()), int(end_date.timestamp()), api_key)

    collection = db[WEATHER_TABLE]
    for e in weather_data:
        collection.update_one({'id': e['timestamp']}, {'$set': e}, upsert=True)

    collection = db[QUALITY_TABLE]
    for e in quality_data:
        collection.update_one({'id': e['timestamp']}, {'$set': e}, upsert=True)

    print('Downloading weather data complete!')

    # print('Sleeping till next day....')
    # day_seconds = 24 * 60 * 60 * 60
    # time.sleep(day_seconds)

if __name__ == "__main__":
    main()