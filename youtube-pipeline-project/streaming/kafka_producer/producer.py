import json
import time
import logging
import requests
from kafka import KafkaProducer # type: ignore
from kafka.errors import NoBrokersAvailable, KafkaConnectionError # type: ignore
from requests.exceptions import RequestException

# ---------------- CONFIG ----------------
RAPIDAPI_KEY = "78b700219bmshbcb74a76fc2570bp10d1b1jsne61961cc2f3d"
API_URL = "https://yt-api.p.rapidapi.com/trending"
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "youtube_realtime"
FETCH_INTERVAL = 60  # sekundi
MAX_RETRIES = 10
RETRY_DELAY = 30  # sekundi između pokušaja
# ----------------------------------------

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def create_kafka_producer(retries=MAX_RETRIES):
    """Kreira Kafka producer sa retry logikom."""
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Pokušaj {attempt}/{retries} - Povezivanje na Kafka broker...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                request_timeout_ms=30000,  # 30 sekundi timeout
                retry_backoff_ms=1000,     # 1 sekunda između retry-jeva
                api_version=(0, 10, 1)     # eksplicitna API verzija
            )
            logging.info("✅ Uspešno povezano na Kafka!")
            return producer
        except (NoBrokersAvailable, KafkaConnectionError) as e:
            logging.warning(f"❌ Neuspešan pokušaj {attempt}/{retries}: {e}")
            if attempt < retries:
                logging.info(f"Čekam {RETRY_DELAY} sekundi pre sledećeg pokušaja...")
                time.sleep(RETRY_DELAY)
            else:
                logging.error("❌ Ne mogu da se povežem na Kafka nakon svih pokušaja!")
                raise

def fetch_youtube_data():
    """Povlači trending video podatke sa RapidAPI-ja."""
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "yt-api.p.rapidapi.com"
    }
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logging.error(f"Greška pri pozivu API-ja: {e}")
        return None

def send_to_kafka(producer, videos):
    """Šalje listu videa kao pojedinačne Kafka evente."""
    for video in videos:
        producer.send(KAFKA_TOPIC, value=video)
    producer.flush()
    logging.info(f"✅ Poslato {len(videos)} videa u Kafka topic '{KAFKA_TOPIC}'")

def main():
    # Kreiraj producer sa retry logikom
    producer = create_kafka_producer()
    
    while True:
        try:
            data = fetch_youtube_data()
            if data and "data" in data:
                send_to_kafka(producer, data["data"])
            else:
                logging.warning("⚠️ Nema novih podataka")
        except Exception as e:
            logging.error(f"Greška u glavnoj petlji: {e}")
        
        time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    logging.info("🚀 YouTube Kafka producer pokrenut")
    main()