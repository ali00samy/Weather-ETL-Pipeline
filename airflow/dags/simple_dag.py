from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract_data(**kwargs):
    data = {'name': 'Ali', 'age': 24, 'city': 'Cairo'}
    kwargs['ti'].xcom_push(key='extracted_data', value=data)

def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='extracted_data')
    data['age'] += 1  # Example transformation
    kwargs['ti'].xcom_push(key='transformed_data', value=data)

def load_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='transformed_data')
    print(f"Loaded Data: {data}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 10),
}

with DAG('simple_etl', default_args=default_args, schedule_interval=None) as dag:
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    extract >> transform >> load  # Task dependencies
