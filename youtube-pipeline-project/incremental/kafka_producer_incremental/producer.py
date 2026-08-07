import json
import time
import logging
import os
import requests  # type: ignore
from kafka import KafkaProducer  # type: ignore
from kafka.errors import KafkaConnectionError  # type: ignore
from requests.exceptions import RequestException  # type: ignore

KAFKA_BROKER = os.getenv("KAFKA_BROKERS", "kafka:9092")
DS_API_URL = os.getenv("DS_API_URL", "http://ds_api:8000")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "300"))
TOPIC = "youtube_incremental"
MAX_RETRIES = 10
RETRY_DELAY = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def create_kafka_producer(retries=MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Pokušaj {attempt}/{retries} - Povezivanje na Kafka broker...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                request_timeout_ms=30000,
                retry_backoff_ms=1000,
                api_version=(0, 10, 1)
            )
            logging.info("Uspešno povezano na Kafka!")
            return producer
        except (KafkaConnectionError, Exception) as e:
            logging.warning(f"Neuspešan pokušaj {attempt}/{retries}: {e}")
            if attempt < retries:
                time.sleep(RETRY_DELAY)
            else:
                raise


def fetch_ds_videos():
    try:
        response = requests.get(f"{DS_API_URL}/ds-videos", timeout=10)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logging.error(f"Greška pri pozivu DS' API-ja: {e}")
        return []


def main():
    producer = create_kafka_producer()
    logging.info(f"DS' inkrementalni producer pokrenut, topic: {TOPIC}")

    while True:
        try:
            videos = fetch_ds_videos()
            for video in videos:
                producer.send(TOPIC, value=video)
            producer.flush()
            logging.info(f"Poslato {len(videos)} DS' redova u topic '{TOPIC}'")
        except Exception as e:
            logging.error(f"Greška u glavnoj petlji: {e}")

        logging.info(f"Čekam {FETCH_INTERVAL} sekundi...")
        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    main()
