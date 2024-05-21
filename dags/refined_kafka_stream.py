from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import logging
import time
from kafka import KafkaProducer
import requests
import uuid

# Default settings applied to all tasks
default_args = {
    'owner': 'Ibrahim Alawaye',
    'start_date': datetime(2024, 5, 9, 11, 0),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_data():
    """
    Fetches random user data from the API.
    """
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    return res['results'][0]

def format_data(res):
    """
    Formats the user data into a dictionary.
    """
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())  # Generate a new UUID
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, " \
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
    """
    Streams formatted user data to a Kafka topic.
    """
    # Initialize logging
    logging.basicConfig(level=logging.ERROR)

    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    curr_time = time.time()

    while True:
        if time.time() > curr_time + 120:
            break  # 2 minutes

        try:
            # Fetch and format data
            res = get_data()
            formatted_res = format_data(res)

            # Send data to Kafka
            producer.send('new_user_created', value=formatted_res)
            producer.flush()
        except Exception as e:
            logging.error(f'Error occurred: {e}')

        # Sleep for a short interval to avoid spamming
        time.sleep(1)

# Define the DAG
dag = DAG(
    'Kafka_API_StreamV2',
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