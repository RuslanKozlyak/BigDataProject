import requests
import pymongo
import time

# Конфигурация
API_KEY = 'F1SVK45-E294A8C-NQMH4KK-0HY6S4V'
MONGO_URI = "mongodb://root:example@mongodb:27017"
DB_NAME = "movies"
COLLECTION_NAME = "films"

# Ожидание доступности MongoDB
client = None
for i in range(5):
    try:
        client = pymongo.MongoClient(MONGO_URI)
        break
    except pymongo.errors.ConnectionFailure:
        print("MongoDB недоступна, пробую снова через 5 секунд...")
        time.sleep(5)

if not client:
    raise Exception("Не удалось подключиться к MongoDB")

db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Функция для получения данных из API Кинопоиска
def fetch_movie_data(api_key, page):
    url = f"https://api.kinopoisk.dev/v1.4/movie?token=F1SVK45-E294A8C-NQMH4KK-0HY6S4V&field=rating.kp&search=7-10&page=1&limit=10"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Загрузка данных и сохранение в MongoDB
for page in range(1, 2):  # Пример, загрузка первых 10 страниц
    data = fetch_movie_data(API_KEY, page)
    if data:
        collection.insert_many(data['docs'])
    else:
        print(f"Ошибка при загрузке данных для страницы {page}")

print("Загрузка завершена!")
