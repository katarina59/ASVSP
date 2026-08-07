import json
import logging
import os
import time

import psycopg2  # type: ignore
from kafka import KafkaConsumer  # type: ignore
from kafka.errors import KafkaConnectionError  # type: ignore

KAFKA_BROKER = os.getenv("KAFKA_BROKERS", "kafka:9092")
TOPIC = "youtube_incremental"
MAX_RETRIES = 10
RETRY_DELAY = 30

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT_INTERNAL", "5432")
PG_DB = os.getenv("POSTGRES_DB", "airflow")
PG_USER = os.getenv("POSTGRES_USER", "airflow")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

CREATE_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS sink_data;

CREATE TABLE IF NOT EXISTS sink_data.sink_videos (
    video_id TEXT NOT NULL,
    trending_date TEXT NOT NULL,
    title TEXT,
    channel_title TEXT,
    category_id INT,
    category_title TEXT,
    assignable BOOLEAN,
    publish_time TIMESTAMPTZ,
    tags TEXT,
    views BIGINT,
    likes BIGINT,
    dislikes BIGINT,
    comment_count BIGINT,
    thumbnail_link TEXT,
    comments_disabled BOOLEAN,
    ratings_disabled BOOLEAN,
    video_error_or_removed BOOLEAN,
    description TEXT,
    region TEXT,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (video_id, trending_date)
);
"""

INSERT_SQL = """
INSERT INTO sink_data.sink_videos (
    video_id, trending_date, title, channel_title, category_id, category_title,
    assignable, publish_time, tags, views, likes, dislikes, comment_count,
    thumbnail_link, comments_disabled, ratings_disabled, video_error_or_removed,
    description, region
) VALUES (
    %(video_id)s, %(trending_date)s, %(title)s, %(channel_title)s, %(category_id)s, %(category_title)s,
    %(assignable)s, %(publish_time)s, %(tags)s, %(views)s, %(likes)s, %(dislikes)s, %(comment_count)s,
    %(thumbnail_link)s, %(comments_disabled)s, %(ratings_disabled)s, %(video_error_or_removed)s,
    %(description)s, %(region)s
)
ON CONFLICT (video_id, trending_date) DO NOTHING;
"""


def to_bool(value):
    return str(value).strip().lower() == "true"


def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def connect_postgres(retries=MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASSWORD
            )
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            logging.warning(f"Postgres nije spreman ({attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(RETRY_DELAY)
            else:
                raise


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    logging.info("sink_data.sink_videos šema/tabela spremna")


def create_kafka_consumer(retries=MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id="sink_consumer_group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                api_version=(0, 10, 1),
            )
        except (KafkaConnectionError, Exception) as e:
            logging.warning(f"Kafka nije spreman ({attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(RETRY_DELAY)
            else:
                raise


def video_to_row(video):
    return {
        "video_id": video.get("video_id"),
        "trending_date": video.get("trending_date"),
        "title": video.get("title"),
        "channel_title": video.get("channel_title"),
        "category_id": to_int(video.get("category_id")),
        "category_title": video.get("category_title"),
        "assignable": to_bool(video.get("assignable")),
        "publish_time": video.get("publish_time"),
        "tags": video.get("tags"),
        "views": to_int(video.get("views")),
        "likes": to_int(video.get("likes")),
        "dislikes": to_int(video.get("dislikes")),
        "comment_count": to_int(video.get("comment_count")),
        "thumbnail_link": video.get("thumbnail_link"),
        "comments_disabled": to_bool(video.get("comments_disabled")),
        "ratings_disabled": to_bool(video.get("ratings_disabled")),
        "video_error_or_removed": to_bool(video.get("video_error_or_removed")),
        "description": video.get("description"),
        "region": video.get("region"),
    }


def main():
    conn = connect_postgres()
    ensure_schema(conn)
    consumer = create_kafka_consumer()
    logging.info(f"Sink consumer pokrenut, sluša topic '{TOPIC}'")

    for message in consumer:
        row = video_to_row(message.value)
        if not row["video_id"] or not row["trending_date"]:
            continue
        try:
            with conn.cursor() as cur:
                cur.execute(INSERT_SQL, row)
        except Exception as e:
            logging.error(f"Greška pri upisu reda {row.get('video_id')}: {e}")


if __name__ == "__main__":
    main()
