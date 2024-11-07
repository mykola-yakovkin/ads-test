from datetime import datetime
import time
from typing import Dict

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

cities = {
    "Kyiv": {"lat": 50.4501, "lon": 30.5234},
    "Kharkiv": {"lat": 49.9935, "lon": 36.2304},
    "Odesa": {"lat": 46.4825, "lon": 30.7233},
    "Lviv": {"lat": 49.8397, "lon": 24.0297},
    "Zhmerynka": {"lat": 49.0391, "lon": 28.1086}
}

default_args = {
    "owner": "airflow",
    "start_date": datetime(2004, 4, 20),
}


def fetch_weather_data(coordinates: Dict[str, float], city_name: str, date: str):
    api_key = Variable.get("OPEN_WEATHER_API_KEY")
    timestamp = int(time.mktime(datetime.strptime(date, "%Y-%m-%d").timetuple()))

    url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine"
    params = {
        'lat': coordinates['lat'],
        'lon': coordinates['lon'],
        'dt': timestamp,
        'appid': api_key,
        'units': 'metric'
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        weather_data = data['data'][0]

        temperature = weather_data.get('temp')
        humidity = weather_data.get('humidity')
        cloudiness = weather_data.get('clouds')
        wind_speed = weather_data.get('wind_speed')

        print(
            f"Weather for {city_name}: Temperature: {temperature}, Humidity: {humidity}, Cloudiness: {cloudiness}"
            f", Wind Speed: {wind_speed}"
        )

    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")


def create_dag():
    dag = DAG(
        "weather_dag_all_cities",
        default_args=default_args,
        description="Weather data extraction for all cities",
        schedule_interval="@daily",
        catchup=True
    )

    with dag:
        for city_name, city_coords in cities.items():
            t1 = PythonOperator(
                task_id=f"fetch_weather_{city_name}",
                python_callable=fetch_weather_data,
                op_kwargs={'coordinates': city_coords, 'city_name': city_name, 'date': "{{ ds }}"},
            )

    return dag


dag_id = "dag_all_cities_scraper"
globals()[dag_id] = create_dag()
