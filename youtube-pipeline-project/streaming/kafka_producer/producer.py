import json
import time
import logging
import requests  # type: ignore
import os
from kafka import KafkaProducer  # type: ignore
from kafka.errors import NoBrokersAvailable, KafkaConnectionError  # type: ignore
from requests.exceptions import RequestException  # type: ignore

RAPIDAPI_KEY = "78b700219bmshbcb74a76fc2570bp10d1b1jsne61961cc2f3d"
BASE_API_URL = "https://yt-api.p.rapidapi.com"
KAFKA_BROKER = os.getenv("KAFKA_BROKERS", "kafka:9092")
FETCH_INTERVAL = 900  # sekundi
MAX_RETRIES = 10
RETRY_DELAY = 30  # sekundi između pokušaja

TOPICS = {
    "trending": "youtube_trending",
    "comments": "youtube_comments", 
    "video_details": "youtube_video_details"
    # "channel_videos": "youtube_channel_videos" 
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


class YouTubeKafkaProducer:
    def __init__(self):
        self.producer = self.create_kafka_producer()
        self.headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": "yt-api.p.rapidapi.com"
        }



    def create_kafka_producer(self, retries=MAX_RETRIES):
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
                logging.info("Uspešno povezano na Kafka!")
                return producer
            except (NoBrokersAvailable, KafkaConnectionError) as e:
                logging.warning(f"Neuspešan pokušaj {attempt}/{retries}: {e}")
                if attempt < retries:
                    logging.info(f"Čekam {RETRY_DELAY} sekundi pre sledećeg pokušaja...")
                    time.sleep(RETRY_DELAY)
                else:
                    logging.error("Ne mogu da se povežem na Kafka nakon svih pokušaja!")
                    raise


    def make_api_request(self, endpoint, params=None, max_retries=5, base_delay=60):
        """Univerzalna funkcija za API pozive sa backoff logikom za 429."""
        url = f"{BASE_API_URL}/{endpoint}"
        delay = base_delay

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=10)
                if response.status_code == 429:
                    logging.warning(f"Rate limit na {endpoint}. Čekam {delay}s (pokušaj {attempt}/{max_retries})...")
                    time.sleep(delay)
                    delay = min(delay * 2, 3600)
                    continue

                response.raise_for_status()
                return response.json()

            except RequestException as e:
                logging.error(f"Greška pri pozivu {endpoint}: {e}")
                if attempt < max_retries:
                    logging.info(f"Čekam {delay}s i pokušavam ponovo...")
                    time.sleep(delay)
                    delay = min(delay * 2, 3600)
                else:
                    logging.error(f"Odustajem od {endpoint} nakon {max_retries} pokušaja.")
                    return None

        return None


    def send_to_kafka(self, topic, data, data_key="data"):
        """Šalje podatke u specifičan Kafka topic."""
        if not data or data_key not in data:
            logging.warning(f"Nema podataka za topic {topic}")
            return
        
        items = data[data_key] if isinstance(data[data_key], list) else [data[data_key]]
        
        for item in items:
            # Dodaj metadata
            enriched_item = {
                "timestamp": int(time.time()),
                "source": "youtube_api",
                "data": item
            }
            self.producer.send(topic, value=enriched_item)
        
        self.producer.flush()
        logging.info(f"Poslato {len(items)} stavki u topic '{topic}'")



    # 1. TRENDING VIDEOS
    def fetch_trending_videos(self):
        """Povlači trending video podatke."""
        data = self.make_api_request("trending")
        if data:
            self.send_to_kafka(TOPICS["trending"], data)
    
    # 2. VIDEO COMMENTS
    def fetch_video_comments(self, video_id):
        """Povlači komentare za specifičan video."""
        data = self.make_api_request("comments", {"id": video_id})
        if data:
            # Dodaj video_id u svaki komentar
            if "data" in data:
                for comment in data["data"]:
                    comment["video_id"] = video_id
            self.send_to_kafka(TOPICS["comments"], data)
    
    # 3. VIDEO DETAILS
    def fetch_video_details(self, video_id):
        """Povlači detaljne informacije o videu."""
        data = self.make_api_request("video/info", {"id": video_id})
        if data:
            self.send_to_kafka(TOPICS["video_details"], {"data": [data]})

    # def fetch_channel_videos(self, channel_id):
    #     """Povlači osnovne informacije o kanalu i listu videa."""
    #     data = self.make_api_request("channel/videos", {"id": channel_id})
    #     if data:
    #         self.send_to_kafka(TOPICS["channel_videos"], {"data": [data]})

 


class YouTubeDataOrchestrator:
    """Orkestrator koji koordinira različite tokove podataka."""
    
    def __init__(self):
        self.youtube_producer = YouTubeKafkaProducer()
        self.processed_videos = set()  # Cache za obrađene videe
    
    def collect_trending_data(self):
        """Osnovni trending tok - pokreće ostale tokove."""
        logging.info("Prikupljanje trending podataka...")
        
        # 1. Dobij trending videe
        trending_data = self.youtube_producer.make_api_request("trending")
        if not trending_data or "data" not in trending_data:
            return
        
        # 2. Pošalji trending podatke
        self.youtube_producer.send_to_kafka(TOPICS["trending"], trending_data)
        
        # 3. Za svaki trending video, prikupi dodatne podatke
        for video in trending_data["data"][:5]:  # Ograniči na prvih 5 zbog rate limita
            video_id = video.get("videoId")
            
            if video_id and video_id not in self.processed_videos:
                # Detalji videa
                self.youtube_producer.fetch_video_details(video_id)
                time.sleep(1)  # Rate limit zaštita
                
                # Komentari videa  
                self.youtube_producer.fetch_video_comments(video_id)
                time.sleep(1)
                

                # Dodaj i kanal
                # channel_id = video.get("channelId")
                # if channel_id:
                #     self.youtube_producer.fetch_channel_videos(channel_id)
                #     time.sleep(1)

                self.processed_videos.add(video_id)

        
        # Čisti cache periodično
        if len(self.processed_videos) > 1000:
            self.processed_videos.clear()



def main():
    orchestrator = YouTubeDataOrchestrator()
    
    logging.info("YouTube Data Pipeline pokrenut")
    logging.info(f"Aktivni topics: {list(TOPICS.values())}")
    
    while True:
        try:
            orchestrator.collect_trending_data()
        except Exception as e:
            logging.error(f"Greška u glavnoj petlji: {e}")
        
        logging.info(f"Čekam {FETCH_INTERVAL} sekundi...")
        time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    main()
