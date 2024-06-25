
import requests
import time
from pymongo import MongoClient

# file: data_loader.py
import requests
import time
from pymongo import MongoClient
import os

# Kinopoisk.dev API configuration
API_KEY = os.environ.get('KINOPOISK_API_KEY')
BASE_URL = 'https://api.kinopoisk.dev/v1.4/movie'
HEADERS = {
    'X-API-KEY': API_KEY,
    'Content-Type': 'application/json',
}

# MongoDB configuration
MONGO_URI = 'mongodb://mongodb:27017/'
DB_NAME = 'movies_db'
COLLECTION_NAME = 'movies'

def fetch_movies(page=1, limit=20):
    params = {
        'page': page,
        'limit': limit,
        'selectFields': ['id', 'name', 'alternativeName', 'year', 'rating', 'description', 'shortDescription', 'poster', 'genres', 'countries'],
        'sortField': 'rating.kp',
        'sortType': '-1'  # Descending order
    }
    
    response = requests.get(BASE_URL, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        print(response.text)
        return None

def insert_movies_to_db(movies):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    for movie in movies['docs']:
        collection.update_one({'id': movie['id']}, {'$set': movie}, upsert=True)
    
    client.close()

def main():
    page = 1
    total_pages = 1
    
    while page <= total_pages:
        print(f"Fetching page {page}...")
        movies_data = fetch_movies(page)
        
        if movies_data:
            total_pages = movies_data['pages']
            insert_movies_to_db(movies_data)
            print(f"Inserted/Updated {len(movies_data['docs'])} movies")
        else:
            break
        
        page += 1
        time.sleep(0.2)  # To avoid hitting API rate limits

    print("Data loading completed!")

if __name__ == "__main__":
    main()