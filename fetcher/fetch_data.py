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
from io import StringIO

def fetch_aurora(lat, lon, start, end):
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Define parameters for API request
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 52.3794,
        "longitude": 13.0645,
        "start_date": "2020-01-01",
        "end_date": "2024-06-20",
        "hourly": ["shortwave_radiation", "direct_radiation", "diffuse_radiation", "direct_normal_irradiance", "global_tilted_irradiance"],
        "daily": ["temperature_2m_max", "temperature_2m_min", "wind_speed_10m_max"]
    }

    responses = openmeteo.weather_api(url, params=params)

    # Process first location
    response = responses[0]

    # Process hourly data
    hourly = response.Hourly()
    hourly_shortwave_radiation = hourly.Variables(0).ValuesAsNumpy()
    hourly_direct_radiation = hourly.Variables(1).ValuesAsNumpy()
    hourly_diffuse_radiation = hourly.Variables(2).ValuesAsNumpy()
    hourly_direct_normal_irradiance = hourly.Variables(3).ValuesAsNumpy()
    hourly_global_tilted_irradiance = hourly.Variables(4).ValuesAsNumpy()

    hourly_data = {
        "timestamp": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "shortwave_radiation": hourly_shortwave_radiation,
        "direct_radiation": hourly_direct_radiation,
        "diffuse_radiation": hourly_diffuse_radiation,
        "direct_normal_irradiance": hourly_direct_normal_irradiance,
        "global_tilted_irradiance": hourly_global_tilted_irradiance
    }

    hourly_dataframe = pd.DataFrame(data=hourly_data)

    # Aggregate hourly data to daily data
    hourly_dataframe['date'] = hourly_dataframe['timestamp'].dt.date
    daily_solar_data = hourly_dataframe.groupby('date').agg({
        'shortwave_radiation': 'mean',
        'direct_radiation': 'mean',
        'diffuse_radiation': 'mean',
        'direct_normal_irradiance': 'mean',
        'global_tilted_irradiance': 'mean'
    }).reset_index()

    # Process daily data
    daily = response.Daily()
    daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(2).ValuesAsNumpy()

    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        ),
        "temperature_2m_max": daily_temperature_2m_max,
        "temperature_2m_min": daily_temperature_2m_min,
        "wind_speed_10m_max": daily_wind_speed_10m_max
    }

    daily_dataframe = pd.DataFrame(data=daily_data)

    # Ensure both dataframes have the 'date' column as datetime type and without time details
    daily_dataframe['date'] = pd.to_datetime(daily_dataframe['date']).dt.date
    daily_solar_data['date'] = pd.to_datetime(daily_solar_data['date']).dt.date

    # Merge daily weather data with aggregated solar data
    merged_weather_data = pd.merge(daily_dataframe, daily_solar_data, on="date")

    return json.loads(merged_weather_data.to_json(orient="records"))


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

# Function to fetch data from the URL
def fetch_data(startdate, enddate):
    url = f"https://kp.gfz-potsdam.de/kpdata?startdate={startdate}&enddate={enddate}&format=kp1#kpdatadownload-143"

    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    data = response.content.decode('utf-8')

    pandas_df = pd.read_fwf(StringIO(data), header=None)

    pandas_df.columns = [
        "Year", "Month", "Day", "Days", "Days_M", "BSR", "DB",
        "Kp_00_03", "Kp_03_06", "Kp_06_09", "Kp_09_12", "Kp_12_15", "Kp_15_18", "Kp_18_21", "Kp_21_24",
        "ap_00_03", "ap_03_06", "ap_06_09", "ap_09_12", "ap_12_15", "ap_15_18", "ap_18_21", "ap_21_24",
        "Ap", "SN", "F10_7obs", "F10_7adj", "D"
    ]
    return json.loads(pandas_df.to_json(orient="records"))

# MongoDB configuration
MONGO_URI = 'mongodb://mongodb:27017/'
DB_NAME = 'movies_db'

WETHER_BD = 'wether_db'

WEATHER_TABLE = 'weather'
QUALITY_TABLE = 'quality'
AURORA_TABLE = 'aurora_weather'
SOLAR_ACTIVITY_TABLE = 'solar_activity'

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
    aurora_data = fetch_aurora(lat, lon, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    solar_activity = fetch_data(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

    start_date = datetime.combine(start_date, datetime.min.time())
    end_date = datetime.combine(end_date, datetime.min.time())

    quality_data = fetch_air_pollution_data(lat, lon, int(start_date.timestamp()), int(end_date.timestamp()), api_key)

    collection = db[WEATHER_TABLE]
    for e in weather_data:
        collection.update_one({'id': e['timestamp']}, {'$set': e}, upsert=True)

    collection = db[QUALITY_TABLE]
    for e in quality_data:
        collection.update_one({'id': e['timestamp']}, {'$set': e}, upsert=True)

    collection = db[AURORA_TABLE]
    for e in aurora_data:
        collection.update_one({'id': e['date']}, {'$set': e}, upsert=True)
    
    collection = db[SOLAR_ACTIVITY_TABLE]
    for e in solar_activity:
        collection.update_one({'id': e['Year']+e['Month']+e['Day']}, {'$set': e}, upsert=True)

    print('Downloading weather data complete!')

    print('Sleeping till next day....')
    day_seconds = 24 * 60 * 60 * 60
    time.sleep(day_seconds)

if __name__ == "__main__":
    main()