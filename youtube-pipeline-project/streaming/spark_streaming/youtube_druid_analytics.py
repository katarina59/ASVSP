from array import ArrayType
import os
import json
import requests
from datetime import datetime, timedelta
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col, count, window, from_json,current_timestamp, unix_timestamp, when, avg, countDistinct, lag, greatest, log10, lit, StringType, StructField# type: ignore
from pyspark.sql.types import IntegerType # type: ignore
from pyspark.sql.window import Window # type: ignore

# ===============================
# KONFIGURACIJA ZA DRUID
# ===============================
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = "youtube_realtime"
DRUID_BROKER_HOST = os.getenv("DRUID_BROKER_HOST", "druid-broker")
DRUID_BROKER_PORT = os.getenv("DRUID_BROKER_PORT", "8082")
DRUID_URL = f"http://{DRUID_BROKER_HOST}:{DRUID_BROKER_PORT}"

# ===============================
# DRUID HTTP CLIENT
# ===============================
class DruidClient:
    def __init__(self, broker_url):
        self.broker_url = broker_url
    
    def send_to_druid(self, datasource, data_batch):
        """Šalje batch podataka u Druid putem HTTP POST"""
        url = f"{self.broker_url}/druid/indexer/v1/task"
        
        # Kreiraj Druid ingestion spec
        ingestion_spec = {
            "type": "index_parallel",
            "spec": {
                "ioConfig": {
                    "type": "index_parallel",
                    "inputSource": {
                        "type": "inline",
                        "data": "\n".join([json.dumps(row) for row in data_batch])
                    },
                    "inputFormat": {
                        "type": "json"
                    }
                },
                "tuningConfig": {
                    "type": "index_parallel",
                    "maxRowsPerSegment": 5000000
                },
                "dataSchema": {
                    "dataSource": datasource,
                    "timestampSpec": {
                        "column": "__time",
                        "format": "iso"
                    },
                    "dimensionsSpec": {
                        "dimensions": self._get_dimensions_for_datasource(datasource)
                    },
                    "metricsSpec": self._get_metrics_for_datasource(datasource),
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "HOUR",
                        "queryGranularity": "MINUTE",
                        "rollup": False
                    }
                }
            }
        }
        
        try:
            response = requests.post(url, json=ingestion_spec, timeout=30)
            if response.status_code == 200:
                print(f"✅ Poslato {len(data_batch)} redova u Druid datasource '{datasource}'")
                return response.json()
            else:
                print(f"❌ Greška pri slanju u Druid: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"❌ Druid connection error: {e}")
            return None
    
    def _get_dimensions_for_datasource(self, datasource):
        """Definiši dimenzije za različite datasource-ove"""
        dimension_configs = {
            "trending_videos": [
                "video_id", "title", "channel_title", "channel_id", 
                "published_date", "thumbnail_url", "duration_category"
            ],
            "channel_analytics": [
                "channel_id", "channel_title"
            ],
            "view_growth": [
                "video_id", "title", "channel_title"
            ],
            "content_by_duration": [
                "duration_category"
            ],
            "engagement_metrics": [
                "video_id", "title", "channel_title"
            ]
        }
        return dimension_configs.get(datasource, [])
    
    def _get_metrics_for_datasource(self, datasource):
        """Definiši metrike za različite datasource-ove"""
        metrics_configs = {
            "trending_videos": [
                {"type": "longSum", "name": "view_count", "fieldName": "view_count"},
                {"type": "doubleSum", "name": "trend_score", "fieldName": "trend_score"}
            ],
            "channel_analytics": [
                {"type": "longSum", "name": "total_videos", "fieldName": "total_videos"},
                {"type": "longSum", "name": "total_views", "fieldName": "total_views"},
                {"type": "doubleSum", "name": "avg_views_per_video", "fieldName": "avg_views_per_video"}
            ],
            "view_growth": [
                {"type": "longSum", "name": "previous_views", "fieldName": "previous_views"},
                {"type": "longSum", "name": "current_views", "fieldName": "current_views"},
                {"type": "doubleSum", "name": "growth_rate", "fieldName": "growth_rate"}
            ],
            "content_by_duration": [
                {"type": "longSum", "name": "video_count", "fieldName": "video_count"},
                {"type": "doubleSum", "name": "avg_views", "fieldName": "avg_views"},
                {"type": "longSum", "name": "total_views", "fieldName": "total_views"}
            ],
            "engagement_metrics": [
                {"type": "doubleSum", "name": "views_per_hour", "fieldName": "views_per_hour"},
                {"type": "doubleSum", "name": "engagement_velocity", "fieldName": "engagement_velocity"},
                {"type": "doubleSum", "name": "viral_score", "fieldName": "viral_score"}
            ]
        }
        return metrics_configs.get(datasource, [])

# Global Druid client
druid_client = DruidClient(DRUID_URL)

# ===============================
# KREIRANJE SPARK SESIJE
# ===============================
def create_spark_session():
    return SparkSession.builder \
        .appName("YouTube-Druid-RealTime-Analytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/opt/spark_apps/checkpoint") \
        .getOrCreate()

# ===============================
# SCHEMA ZA YOUTUBE PODATKE
# ===============================
youtube_schema = StringType([
    StructField("type", StringType(), True),
    StructField("videoId", StringType(), True),
    StructField("title", StringType(), True),
    StructField("channelTitle", StringType(), True),
    StructField("channelId", StringType(), True),
    StructField("viewCount", StringType(), True),
    StructField("publishedTimeText", StringType(), True),
    StructField("publishDate", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("lengthText", StringType(), True),
    StructField("description", StringType(), True),
    StructField("thumbnail", ArrayType(StringType([
        StructField("url", StringType(), True),
        StructField("width", IntegerType(), True),
        StructField("height", IntegerType(), True)
    ])), True)
])

# ===============================
# DRUID WRITER FUNKCIJA
# ===============================
def write_to_druid(df, datasource_name, epoch_id):
    """Piše DataFrame batch u Druid"""
    # Konvertuj DataFrame u liste rečnika
    data_batch = []
    for row in df.collect():
        row_dict = row.asDict()
        # Dodaj timestamp
        row_dict["__time"] = datetime.now().isoformat()
        data_batch.append(row_dict)
    
    if data_batch:
        druid_client.send_to_druid(datasource_name, data_batch)

# ===============================
# UČITAVANJE PODATAKA IZ KAFKE
# ===============================
def read_kafka_stream(spark):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

# ===============================
# TRANSFORMACIJA OSNOVNIH PODATAKA
# ===============================
def transform_youtube_data(kafka_df):
    parsed_df = kafka_df \
        .select(
            from_json(col("value").cast("string"), youtube_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .filter(col("videoId").isNotNull()) \
        .withColumn("view_count_int", col("viewCount").cast("long")) \
        .withColumn("thumbnail_url", col("thumbnail").getItem(0).getField("url")) \
        .withColumn("processed_time", current_timestamp()) \
        .filter(col("view_count_int") > 0)
    
    # Dodaj kategorije po dužini videa
    parsed_df = parsed_df.withColumn(
        "duration_category",
        when(col("lengthText").rlike("^[0-9]:[0-9]{2}$"), "short")
        .when(col("lengthText").rlike("^[1-5][0-9]:[0-9]{2}$"), "medium")
        .otherwise("long")
    )
    
    # Računaj recency (sati od publishedAt)
    parsed_df = parsed_df.withColumn(
        "publish_recency_hours",
        (unix_timestamp(col("processed_time")) - unix_timestamp(col("publishedAt"))) / 3600.0
    )
    
    return parsed_df

# ===============================
# UPIT 1: TRENDING VIDEOS → DRUID
# ===============================
def streaming_query_1_trending_videos(df):
    trending_df = df \
        .withWatermark("processed_time", "10 minutes") \
        .groupBy(
            window(col("processed_time"), "5 minutes", "1 minute"),
            col("videoId"), col("title"), col("channelTitle"), 
            col("channelId"), col("thumbnail_url"), col("duration_category")
        ) \
        .agg(
            max(col("view_count_int")).alias("view_count"),
            count("*").alias("update_count")
        ) \
        .withColumn(
            "trend_score",
            col("view_count") * col("update_count") / 1000000.0
        ) \
        .select(
            col("videoId").alias("video_id"),
            col("title"),
            col("channelTitle").alias("channel_title"),
            col("channelId").alias("channel_id"),
            col("view_count"),
            col("thumbnail_url"),
            col("duration_category"),
            col("trend_score")
        )
    
    return trending_df.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, epoch: write_to_druid(df, "trending_videos", epoch)) \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", "/opt/spark_apps/checkpoint/trending_druid") \
        .start()

# ===============================
# UPIT 2: CHANNEL ANALYTICS → DRUID
# ===============================
def streaming_query_2_channel_analytics(df):
    channel_analytics = df \
        .withWatermark("processed_time", "15 minutes") \
        .groupBy(
            window(col("processed_time"), "5 minutes", "5 minutes"),
            col("channelId"), col("channelTitle")
        ) \
        .agg(
            countDistinct(col("videoId")).alias("total_videos"),
            sum(col("view_count_int")).alias("total_views"),
            avg(col("view_count_int")).alias("avg_views_per_video")
        ) \
        .select(
            col("channelId").alias("channel_id"),
            col("channelTitle").alias("channel_title"),
            col("total_videos"),
            col("total_views"),
            col("avg_views_per_video")
        )
    
    return channel_analytics.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch: write_to_druid(df, "channel_analytics", epoch)) \
        .trigger(processingTime="5 minutes") \
        .option("checkpointLocation", "/opt/spark_apps/checkpoint/channel_druid") \
        .start()

# ===============================
# UPIT 3: VIEW GROWTH → DRUID
# ===============================
def streaming_query_3_view_growth(df):
    growth_df = df \
        .withWatermark("processed_time", "20 minutes") \
        .withColumn(
            "previous_views",
            lag(col("view_count_int"), 1).over(
                Window.partitionBy("videoId").orderBy("processed_time")
            )
        ) \
        .filter(col("previous_views").isNotNull()) \
        .withColumn(
            "growth_rate",
            when(col("previous_views") > 0, 
                 (col("view_count_int") - col("previous_views")) / col("previous_views") * 100.0)
            .otherwise(0.0)
        ) \
        .filter(col("view_count_int") > col("previous_views")) \
        .select(
            col("videoId").alias("video_id"),
            col("title"),
            col("channelTitle").alias("channel_title"),
            col("previous_views"),
            col("view_count_int").alias("current_views"),
            col("growth_rate")
        )
    
    return growth_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch: write_to_druid(df, "view_growth", epoch)) \
        .trigger(processingTime="2 minutes") \
        .option("checkpointLocation", "/opt/spark_apps/checkpoint/growth_druid") \
        .start()

# ===============================
# UPIT 4: CONTENT BY DURATION → DRUID
# ===============================
def streaming_query_4_content_by_duration(df):
    duration_analytics = df \
        .withWatermark("processed_time", "10 minutes") \
        .groupBy(
            window(col("processed_time"), "10 minutes", "5 minutes"),
            col("duration_category")
        ) \
        .agg(
            countDistinct(col("videoId")).alias("video_count"),
            avg(col("view_count_int")).alias("avg_views"),
            sum(col("view_count_int")).alias("total_views")
        ) \
        .select(
            col("duration_category"),
            col("video_count"),
            col("avg_views"),
            col("total_views")
        )
    
    return duration_analytics.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch: write_to_druid(df, "content_by_duration", epoch)) \
        .trigger(processingTime="5 minutes") \
        .option("checkpointLocation", "/opt/spark_apps/checkpoint/duration_druid") \
        .start()

# ===============================
# UPIT 5: ENGAGEMENT METRICS → DRUID
# ===============================
def streaming_query_5_engagement_metrics(df):
    engagement_df = df \
        .withWatermark("processed_time", "5 minutes") \
        .withColumn(
            "views_per_hour",
            when(col("publish_recency_hours") > 0,
                 col("view_count_int") / col("publish_recency_hours"))
            .otherwise(col("view_count_int"))
        ) \
        .withColumn(
            "engagement_velocity",
            col("view_count_int") / greatest(col("publish_recency_hours") * 60, lit(1.0))
        ) \
        .withColumn(
            "viral_score",
            log10(greatest(col("view_count_int"), lit(1))) * 
            log10(greatest(col("engagement_velocity"), lit(1)))
        ) \
        .select(
            col("videoId").alias("video_id"),
            col("title"),
            col("channelTitle").alias("channel_title"),
            col("views_per_hour"),
            col("engagement_velocity"),
            col("viral_score")
        ) \
        .filter(col("viral_score") > 0)
    
    return engagement_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch: write_to_druid(df, "engagement_metrics", epoch)) \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", "/opt/spark_apps/checkpoint/engagement_druid") \
        .start()

# ===============================
# GLAVNA FUNKCIJA
# ===============================
def main():
    print("🚀 Pokretanje YouTube Real-Time Analytics sa Spark Streaming + Druid...")
    
    # Kreiraj Spark sesiju
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Učitaj Kafka stream
    kafka_df = read_kafka_stream(spark)
    
    # Transformiši podatke
    youtube_df = transform_youtube_data(kafka_df)
    
    print("📊 Pokretam 5 streaming upita ka Druid-u...")
    
    # Pokreni sve streaming upite
    query1 = streaming_query_1_trending_videos(youtube_df)
    query2 = streaming_query_2_channel_analytics(youtube_df)  
    query3 = streaming_query_3_view_growth(youtube_df)
    query4 = streaming_query_4_content_by_duration(youtube_df)
    query5 = streaming_query_5_engagement_metrics(youtube_df)
    
    print("✅ Svih 5 upita pokrenuto!")
    print(f"🌐 Druid Web Console: http://localhost:8085")
    print("🔄 Čekam streaming podatke...")
    
    # Čekaj da se završe svi upiti
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("🛑 Zaustavljanje aplikacije...")
        spark.stop()

if __name__ == "__main__":
    main()