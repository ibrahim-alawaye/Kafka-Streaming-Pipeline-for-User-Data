from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import logging
import time
from kafka import KafkaProducer

# Default settings applied to all tasks
default_args = {
    'owner': 'Ibrahim Alawaye',
    'start_date': datetime(2024, 5, 9, 11, 00),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_data():
    import json
   # import pandas
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res


def format_data(res):
    import uuid
    data = {}
    location = res['location']
    #data['id'] = str(uuid.uuid4())  # Generate a new UUID
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    return data


def stream_data():
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

# Define the DAG
dag = DAG(
    'Kafka_API_Stream',
    default_args=default_args,
    description='End to End Stream of User information using Kafka',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Define the tasks
stream_api_data = PythonOperator(
    task_id='stream_api_data',
    python_callable=stream_data,
    dag=dag,
)

# Set task dependencies
# task1

stream_api_data