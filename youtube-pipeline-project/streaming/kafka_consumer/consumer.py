import json
import logging
from kafka import KafkaConsumer # type: ignore
from kafka.errors import NoBrokersAvailable # type: ignore
import pandas as pd # type: ignore
from datetime import datetime
import time

# ---------------- CONFIG ----------------
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "youtube_realtime"
CONSUMER_GROUP = "youtube_analytics_group"
# ----------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def create_kafka_consumer(retries=5):
    """Kreira Kafka consumer sa retry logikom."""
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Pokušaj {attempt}/{retries} - Kreiranje Kafka consumer...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=CONSUMER_GROUP,
                auto_offset_reset='earliest',  # čita od početka
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=30000,  # timeout za polling
                enable_auto_commit=True
            )
            logging.info("✅ Kafka consumer kreiran uspešno!")
            return consumer
        except NoBrokersAvailable as e:
            logging.warning(f"❌ Neuspešan pokušaj {attempt}/{retries}: {e}")
            if attempt < retries:
                time.sleep(10)
            else:
                raise

def process_video_data(video_data):
    """Obrađuje pojedinačan video zapis."""
    try:
        # Ekstraktuj ključne metrike
        processed = {
            'video_id': video_data.get('videoId'),
            'title': video_data.get('title'),
            'channel_title': video_data.get('channelTitle'),
            'channel_id': video_data.get('channelId'),
            'channel_thumbnail': video_data.get('thumbnail', [{}])[0].get('url') if video_data.get('channelThumbnail') else None,
            'view_count': int(video_data.get('viewCount', 0)) if video_data.get('viewCount') else 0,
            'published_time_text': video_data.get('publishedTimeText'),
            'published_date': video_data.get('publishDate'),
            'published_at': datetime(video_data.get('publishedAt')).isoformat(),
            'length_text': video_data.get('lengthText'),
            'description_preview': video_data.get('description', '')[:100] + '...' if video_data.get('description') else '',
            'thumbnail_url': video_data.get('thumbnail', [{}])[0].get('url') if video_data.get('thumbnail') else None,
            'processed_at': datetime.now().isoformat()
        }
        return processed
    except Exception as e:
        logging.error(f"Greška pri procesiranju video podataka: {e}")
        return None

def analyze_trending_data(videos_batch):
    """Analizira batch trendujućih videa."""
    if not videos_batch:
        return
    
    df = pd.DataFrame(videos_batch)
    
    # Osnovne statistike
    total_videos = len(df)
    total_views = df['view_count'].sum()
    avg_views = df['view_count'].mean()
    
    # Top 10 kanala po broju videa
    top_channels = df['channel_title'].value_counts().head(10)
    
    # Top 10 videa po views
    top_videos = df.nlargest(10, 'view_count')[['title', 'channel_title', 'view_count']]
    
    logging.info("📊 TRENDING ANALIZA:")
    logging.info(f"   Total videa: {total_videos}")
    logging.info(f"   Ukupno pregleda: {total_views:,}")
    logging.info(f"   Prosečno pregleda: {avg_views:,.0f}")
    logging.info("📈 Top 5 kanala:")
    for channel, count in top_channels.head(5).items():
        logging.info(f"   {channel}: {count} videa")
    
    logging.info("🔥 Top 3 najgledanija:")
    for _, video in top_videos.head(3).iterrows():
        logging.info(f"   {video['title'][:50]}... ({video['view_count']:,} views)")

def main():
    consumer = create_kafka_consumer()
    
    logging.info("🔄 Počinje konzumiranje YouTube streaming podataka...")
    
    videos_batch = []
    batch_size = 50  # Obradi svakih 50 videa
    
    try:
        for message in consumer:
            video_data = message.value
            processed_video = process_video_data(video_data)
            
            if processed_video:
                videos_batch.append(processed_video)
                logging.info(f"📺 Procesiran: {processed_video['title'][:50]}... "
                           f"({processed_video['view_count']:,} views)")
                
                # Kada prikupiš batch, analiziraj
                if len(videos_batch) >= batch_size:
                    analyze_trending_data(videos_batch)
                    videos_batch = []  # Resetuj batch
                    
    except KeyboardInterrupt:
        logging.info("🛑 Consumer zaustavljen od strane korisnika")
    except Exception as e:
        logging.error(f"❌ Greška u consumer-u: {e}")
    finally:
        consumer.close()
        logging.info("✅ Kafka consumer zatvoren")

if __name__ == "__main__":
    logging.info("🚀 YouTube Kafka consumer pokrenut")
    main()