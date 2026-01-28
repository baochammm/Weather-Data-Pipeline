import os
import requests
import mysql.connector
import json
from dotenv import load_dotenv

load_dotenv()

# Config connect MySQL from .env
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

# API key and cities to ingest
API_KEY = os.getenv("WEATHER_API_KEY")
CITIES = ["Hanoi", "Ho Chi Minh City"]

def ingest_data():
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        for city in CITIES:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                query = "INSERT INTO stg_weather (city_name, raw_json) VALUES (%s, %s)"
                cursor.execute(query, (city, json.dumps(data)))
                print(f"Ingested: {city}")
            else:
                print(f"API error for {city}: {response.status_code}")
                
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    ingest_data()