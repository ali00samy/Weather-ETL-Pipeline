import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import os

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
API_KEY = os.getenv('OPENWEATHER_API_KEY')

def extract_weather(**kwargs):
    API_KEY = 'your_openweather_api_key'
    url = f"http://api.openweathermap.org/data/2.5/weather?q=Cairo&appid={API_KEY}"
    response = requests.get(url)
    data = response.json()
    kwargs['ti'].xcom_push(key='weather_data', value=data)

def transform_weather(**kwargs):
    data = kwargs['ti'].xcom_pull(key='weather_data')
    temp = data['main']['temp'] - 273.15  # Convert from Kelvin to Celsius
    cleaned_data = {'city': data['name'], 'temp': temp, 'humidity': data['main']['humidity']}
    kwargs['ti'].xcom_push(key='transformed_data', value=cleaned_data)

def load_to_postgres(**kwargs):
    data = kwargs['ti'].xcom_pull(key='transformed_data')
    conn = psycopg2.connect(
        database=DB_NAME, 
        user=DB_USER,
        password=DB_PASSWORD,
        host="postgres",
        port=5432
    )
    cursor = conn.cursor()
    query = "INSERT INTO weather (city, temp, humidity) VALUES (%s, %s, %s)"
    cursor.execute(query, (data['city'], data['temp'], data['humidity']))
    conn.commit()
    cursor.close()
    conn.close()

default_args = {'owner': 'airflow', 'start_date': datetime(2024, 10, 10)}

with DAG('weather_etl', default_args=default_args, schedule_interval='@daily') as dag:
    extract = PythonOperator(task_id='extract_weather', python_callable=extract_weather, provide_context=True)
    transform = PythonOperator(task_id='transform_weather', python_callable=transform_weather, provide_context=True)
    load = PythonOperator(task_id='load_to_postgres', python_callable=load_to_postgres, provide_context=True)

    extract >> transform >> load
