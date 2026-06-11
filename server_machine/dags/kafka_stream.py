
import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {"owner": "FD",
                "start_date": datetime(2026, 6, 9, 8, 10)}


def extract_api_data():

    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res["results"][0]

    return res

def transform_data(res):

    data = {}
    location = res["location"]

    data["id"] = uuid.uuid4().hex

    # extracting information
    data["first_name"] = res["name"]["first"]
    data["last_name"] = res["name"]["last"]
    data["gender"] = res["gender"]
    data["address"] = (
        f"{str(location['street']['number'])} {location['street']['name']}, "
        f"{location['city']}, {location['state']}, {location['country']}"
    )
    data["post_code"] = location["postcode"]
    data["email"] = res["email"]
    data["username"] = res["login"]["username"]
    data["dob"] = res["dob"]["date"]
    data["registered_date"] = res["registered"]["date"]
    data["phone"] = res["phone"]
    data["picture"] = res["picture"]["medium"]

    return data


def data_streaming():

    import json
    from kafka import KafkaProducer
    import time
    import logging

    try:

        # connection with Kafka
        producer = KafkaProducer(bootstrap_servers = ["broker:29092"], max_block_ms = 5000)

        # 5s before start streaming
        time.sleep(5)
        logging.info("Kafka producer successfully connected.")

    except Exception as e:

        logging.error(f"Failed to connect to Kafka broker: {e}")
        return

    curr_time = time.time()

    # the streaming will run for 1 minute
    while True:

        if time.time() > curr_time + 60:  # 1 minute
            break
        try:

            res = extract_api_data()
            res = transform_data(res)

            producer.send("kafka_topic", json.dumps(res).encode("utf-8"))

        except Exception as e:

            logging.error(f"An error occurred: {e}")
            continue

# Airflow DAG
with DAG("real-time-etl-stack",
         default_args=default_args,
         schedule=timedelta(days=1),
         catchup=False,
) as dag:
    streaming_task = PythonOperator(task_id="stream_from_api", 
                                    python_callable=data_streaming)




