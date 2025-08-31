import json
import logging
import os
from kafka import KafkaConsumer 
from kafka.errors import NoBrokersAvailable 
import pandas as pd  # type: ignore
from datetime import datetime
import time
import re

# ---------------- CONFIG ----------------
KAFKA_BROKER = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "youtube_realtime")
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
                consumer_timeout_ms=120000,  # timeout za polling
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

def safe_int_conversion(value):
    """Bezbedno konvertuje vrednost u integer."""
    if value is None:
        return 0
    
    if isinstance(value, int):
        return value
    
    if isinstance(value, str):
        # Ukloni sva slova i specijalne karaktere, ostavi samo brojevi
        numeric_str = re.sub(r'[^\d]', '', value)
        if numeric_str:
            try:
                return int(numeric_str)
            except ValueError:
                return 0
        return 0
    
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0

def safe_datetime_conversion(date_value):
    """Bezbedno konvertuje datum u ISO format."""
    if not date_value:
        return None
    
    try:
        # Ako je već datetime objekat
        if isinstance(date_value, datetime):
            return date_value.isoformat()
        
        # Ako je string, pokušaj različite formate
        if isinstance(date_value, str):
            # Pokušaj ISO format
            try:
                dt = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                return dt.isoformat()
            except ValueError:
                pass
            
            # Pokušaj različite formate datuma
            date_formats = [
                '%Y-%m-%d',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%d %H:%M:%S'
            ]
            
            for fmt in date_formats:
                try:
                    dt = datetime.strptime(date_value, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
        
        # Ako je broj (timestamp)
        if isinstance(date_value, (int, float)):
            dt = datetime.fromtimestamp(date_value)
            return dt.isoformat()
            
    except Exception as e:
        logging.warning(f"Ne mogu da konvertujem datum {date_value}: {e}")
    
    return None

def safe_get_nested(data, *keys):
    """Bezbedno pristupa ugniježđenim vrednostima."""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list) and isinstance(key, int) and 0 <= key < len(current):
            current = current[key]
        else:
            return None
    return current

def process_video_data(video_data):
    """Obrađuje pojedinačan video zapis - potpuno bezbedan pristup."""
    try:
        # Loguj raw podatke za debugging (samo prvi put)
        if not hasattr(process_video_data, '_logged_sample'):
            logging.info(f"📋 Sample video data keys: {list(video_data.keys())}")
            logging.info(f"📋 Sample viewCount value: {repr(video_data.get('viewCount'))}")
            logging.info(f"📋 Sample publishedAt value: {repr(video_data.get('publishedAt'))}")
            process_video_data._logged_sample = True
        
        # Potpuno bezbedan pristup - sve vrednosti su bezbedne
        processed = {}
        
        # Osnovni identifikatori - nikad neće baciti grešku
        processed['video_id'] = str(video_data.get('videoId', video_data.get('id', 'N/A')))
        processed['title'] = str(video_data.get('title', 'N/A'))
        processed['channel_title'] = str(video_data.get('channelTitle', video_data.get('channelName', 'N/A')))
        processed['channel_id'] = str(video_data.get('channelId', 'N/A'))
        
        # Bezbedna konverzija view_count - koristi helper funkciju
        processed['view_count'] = safe_int_conversion(video_data.get('viewCount'))
        
        # String polja - direktno iz podataka
        processed['published_time_text'] = str(video_data.get('publishedTimeText', 'N/A'))
        processed['published_date'] = str(video_data.get('publishDate', 'N/A'))
        processed['length_text'] = str(video_data.get('lengthText', video_data.get('duration', 'N/A')))
        
        # Bezbedna konverzija datuma
        processed['published_at'] = safe_datetime_conversion(video_data.get('publishedAt'))
        
        # Opis sa ograničenjem
        description = video_data.get('description', '')
        if description and len(str(description)) > 100:
            processed['description_preview'] = str(description)[:100] + '...'
        else:
            processed['description_preview'] = str(description) if description else 'N/A'
        
        # Trenutni timestamp
        processed['processed_at'] = datetime.now().isoformat()
        
        # Bezbedno izvlačenje thumbnail URL-ova
        processed['thumbnail_url'] = safe_get_thumbnail_url(video_data)
        processed['channel_thumbnail'] = safe_get_channel_thumbnail(video_data)
        
        return processed
        
    except Exception as e:
        # Čak i ako sve ovo ne uspe, vrati osnovni objekat
        logging.error(f"KRITIČNA GREŠKA pri procesiranju: {e}")
        return {
            'video_id': 'ERROR',
            'title': 'Processing Error',
            'channel_title': 'Unknown',
            'channel_id': 'Unknown',
            'view_count': 0,
            'published_time_text': 'N/A',
            'published_date': 'N/A',
            'published_at': None,
            'length_text': 'N/A',
            'description_preview': 'N/A',
            'processed_at': datetime.now().isoformat(),
            'thumbnail_url': None,
            'channel_thumbnail': None
        }

def safe_get_thumbnail_url(video_data):
    """Bezbedno izvlači thumbnail URL."""
    try:
        # Pokušaj thumbnail kao lista
        thumbnail = video_data.get('thumbnail')
        if isinstance(thumbnail, list) and len(thumbnail) > 0:
            if isinstance(thumbnail[0], dict):
                return thumbnail[0].get('url')
        
        # Pokušaj thumbnail kao dict
        if isinstance(thumbnail, dict):
            return thumbnail.get('url')
        
        # Pokušaj thumbnail kao string
        if isinstance(thumbnail, str):
            return thumbnail
            
        # Pokušaj thumbnails objekat
        thumbnails = video_data.get('thumbnails', {})
        if isinstance(thumbnails, dict):
            for resolution in ['maxres', 'standard', 'high', 'medium', 'default']:
                if resolution in thumbnails and isinstance(thumbnails[resolution], dict):
                    url = thumbnails[resolution].get('url')
                    if url:
                        return url
        
        return None
    except:
        return None

def safe_get_channel_thumbnail(video_data):
    """Bezbedno izvlači channel thumbnail URL."""
    try:
        channel_thumb = video_data.get('channelThumbnail')
        if isinstance(channel_thumb, list) and len(channel_thumb) > 0:
            if isinstance(channel_thumb[0], dict):
                return channel_thumb[0].get('url')
        return None
    except:
        return None

def analyze_trending_data(videos_batch):
    """Analizira batch trendujućih videa."""
    if not videos_batch:
        return
    
    try:
        df = pd.DataFrame(videos_batch)
        
        # Osnovne statistike
        total_videos = len(df)
        total_views = df['view_count'].sum()
        avg_views = df['view_count'].mean() if total_videos > 0 else 0
        
        # Top kanali po broju videa (filtruj N/A)
        valid_channels = df[df['channel_title'] != 'N/A']['channel_title']
        top_channels = valid_channels.value_counts().head(10) if not valid_channels.empty else pd.Series()
        
        # Top videa po views (filtruj one sa 0 views)
        valid_videos = df[df['view_count'] > 0]
        top_videos = valid_videos.nlargest(10, 'view_count')[['title', 'channel_title', 'view_count']] if not valid_videos.empty else pd.DataFrame()
        
        logging.info("📊 TRENDING ANALIZA:")
        logging.info(f"   Total videa: {total_videos}")
        logging.info(f"   Ukupno pregleda: {total_views:,}")
        logging.info(f"   Prosečno pregleda: {avg_views:,.0f}")
        
        if not top_channels.empty:
            logging.info("📈 Top 5 kanala:")
            for channel, count in top_channels.head(5).items():
                logging.info(f"   {channel}: {count} videa")
        
        if not top_videos.empty:
            logging.info("🔥 Top 3 najgledanija:")
            for _, video in top_videos.head(3).iterrows():
                title = video['title'][:50] + "..." if len(video['title']) > 50 else video['title']
                logging.info(f"   {title} ({video['view_count']:,} views)")
        
        # Dodatne statistike
        videos_with_views = df[df['view_count'] > 0]
        if not videos_with_views.empty:
            logging.info(f"📈 Videa sa pregledi > 0: {len(videos_with_views)}")
            logging.info(f"📈 Max pregleda: {videos_with_views['view_count'].max():,}")
            logging.info(f"📈 Min pregleda: {videos_with_views['view_count'].min():,}")
            
    except Exception as e:
        logging.error(f"Greška u analizi podataka: {e}")

def main():
    consumer = create_kafka_consumer()
    
    logging.info("🔄 Počinje konzumiranje YouTube streaming podataka...")
    
    videos_batch = []
    batch_size = 50  # Obradi svakih 50 videa
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            video_data = message.value
            processed_video = process_video_data(video_data)
            
            if processed_video:
                videos_batch.append(processed_video)
                title = processed_video['title'][:50] + "..." if len(processed_video['title']) > 50 else processed_video['title']
                logging.info(f"📺 [{message_count}] Procesiran: {title} "
                           f"({processed_video['view_count']:,} views)")
                
                # Kada prikupiš batch, analiziraj
                if len(videos_batch) >= batch_size:
                    analyze_trending_data(videos_batch)
                    videos_batch = []  # Resetuj batch
            else:
                logging.warning(f"⚠️ [{message_count}] Neuspešno procesiran video")
                    
    except KeyboardInterrupt:
        logging.info("🛑 Consumer zaustavljen od strane korisnika")
    except Exception as e:
        logging.error(f"❌ Greška u consumer-u: {e}")
        # Analiziraj poslednji batch pre zatvaranja
        if videos_batch:
            analyze_trending_data(videos_batch)
    finally:
        consumer.close()
        logging.info("✅ Kafka consumer zatvoren")

if __name__ == "__main__":
    logging.info("🚀 YouTube Kafka consumer pokrenut")
    main()