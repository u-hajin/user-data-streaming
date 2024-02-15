from pendulum import datetime

from airflow.decorators import dag, task
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator


def get_data():
    """
    Get data from random-user API
    """

    import requests

    response = requests.get('https://randomuser.me/api/')
    response = response.json()
    response = response['results'][0]

    return response


def format_data(response):
    data = {}

    data['id'] = response['login']['uuid']
    data['gender'] = response['gender']
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']

    location = response['location']
    data['address'] = (f"{str(location['street']['number'])} {location['street']['name']}"
                       f"{location['city']} {location['state']} {location['country']}")
    data['postcode'] = f"{str(location['postcode'])}"
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data


def stream_data():
    import json
    import logging
    import time

    current_time = time.time()
    i = 0

    while True:
        if time.time() > current_time + 60:
            break
        try:
            response = get_data()
            data = format_data(response)
            data = json.dumps(data, ensure_ascii=False).encode('utf-8').decode()

            yield (
                json.dumps(i),
                data
            )
            i += 1
        except Exception as e:
            logging.error(f'An error occurred while streaming data to kafka: {e}')


@dag(
    start_date=datetime(2024, 2, 7, tz='Asia/Seoul'),
    schedule='*/30 * * * *',
    catchup=False
)
def kafka_stream():
    produce_user_data = ProduceToTopicOperator(
        task_id='produce_user_data',
        kafka_config_id='kafka_default',
        topic='users',
        producer_function=stream_data
    )

    produce_user_data


kafka_stream()
