# channel_videos_schema = StructType([
#     StructField("timestamp", LongType(), True),
#     StructField("source", StringType(), True),
#     StructField("data", StructType([
#         StructField("channelId", StringType(), True),
#         StructField("title", StringType(), True),
#         StructField("description", StringType(), True),

#         StructField("channelHandle", StringType(), True),
#         StructField("subscriberCountText", StringType(), True),
#         StructField("subscriberCount", StringType(), True),
#         StructField("videosCountText", StringType(), True),
#         StructField("videosCount", StringType(), True),

#         StructField("keywords", ArrayType(StringType()), True),
#         StructField("isFamilySafe", BooleanType(), True),
#         StructField("isUnlisted", BooleanType(), True),
#         StructField("availableCountries", ArrayType(StringType()), True),

#         StructField("avatar", ArrayType(StructType([
#             StructField("url", StringType(), True),
#             StructField("width", IntegerType(), True),
#             StructField("height", IntegerType(), True),
#         ])), True),

#         StructField("banner", ArrayType(StructType([
#             StructField("url", StringType(), True),
#             StructField("width", IntegerType(), True),
#             StructField("height", IntegerType(), True),
#         ])), True),

#         StructField("tabs", ArrayType(StringType()), True),
#         StructField("continuation", StringType(), True)
#     ]), True)
# ])

# channel_videos_stream = create_kafka_stream(spark, KAFKA_TOPICS["channel_videos"], channel_videos_schema)


   # channel_videos_basic = channel_videos_stream.select(
    #     col("timestamp").alias("kafka_timestamp"),
    #     col("source"),
    #     col("data.channelId").alias("channel_id"),
    #     col("data.title").alias("channel_title"),
    #     col("data.description").alias("description"),
    #     col("data.channelHandle").alias("channel_handle"),
    #     col("data.subscriberCount").alias("subscriber_count"),
    #     col("data.subscriberCountText").alias("subscriber_count_text"),
    #     col("data.videosCount").alias("videos_count"),
    #     col("data.videosCountText").alias("videos_count_text"),
    #     col("data.keywords").alias("keywords"),
    #     col("data.isFamilySafe").alias("is_family_safe"),
    #     col("data.isUnlisted").alias("is_unlisted"),
    #     col("data.availableCountries").alias("available_countries"),
    #     col("data.avatar").alias("avatar"),
    #     col("data.banner").alias("banner"),
    #     col("data.tabs").alias("tabs"),
    #     col("data.continuation").alias("continuation"),
    #     col("processing_time")
    # )


    # channel_videos_query = channel_videos_basic.writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(lambda df, epoch: print_stream_data(df, epoch, "CHANNEL VIDEOS")) \
    #     .trigger(processingTime='10 seconds') \
    #     .start()



import os
from pyspark.sql import SparkSession # type: ignore
import pyspark.sql.functions as F # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType, BooleanType # type: ignore
import datetime 


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")

pg_url = "jdbc:postgresql://postgres:5432/airflow"
pg_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}


# Kafka topics iz producer-a
KAFKA_TOPICS = {
    "trending": "youtube_trending",
    "comments": "youtube_comments", 
    "video_details": "youtube_video_details"
    # "channel_videos": "youtube_channel_videos" 
}


def create_spark_session():
    return SparkSession.builder \
        .appName("YouTube-Advanced-Analytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

trending_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("source", StringType(), True),
    StructField("data", StructType([
        StructField("type", StringType(), True),
        StructField("videoId", StringType(), True),
        StructField("title", StringType(), True),


        StructField("channelId", StringType(), True),
        StructField("channelTitle", StringType(), True),
        StructField("channelHandle", ArrayType(StructType([
            StructField("url", StringType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True),
        ])), True),
        StructField("channelThumbnail", ArrayType(StructType([
            StructField("url", StringType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True),
        ])), True),
        StructField("channelAvatar", ArrayType(StructType([
            StructField("url", StringType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True),
        ])), True),

        StructField("description", StringType(), True),
        StructField("viewCountText", StringType(), True),
        StructField("viewCount", StringType(), True),
        StructField("publishedTimeText", StringType(), True),
        StructField("publishDate", StringType(), True),
        StructField("publishedAt", StringType(), True),
        StructField("lengthText", StringType(), True),
        StructField("thumbnail", ArrayType(StructType([
            StructField("url", StringType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True)
        ])), True),
    ]), True)
])

video_details_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("source", StringType(), True),
    StructField("data", StructType([
        # OSNOVNE INFORMACIJE
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("lengthSeconds", StringType(), True),  
        
        
        # KANAL
        StructField("channelId", StringType(), True),
        StructField("channelTitle", StringType(), True),
        
        # SADRŽAJ
        StructField("description", StringType(), True),
        
        # THUMBNAIL ARRAY - može biti null
        StructField("thumbnail", ArrayType(StructType([
            StructField("url", StringType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True),
        ])), True),
        
        # BOOLEAN FLAGS
        StructField("allowRatings", BooleanType(), True),
        StructField("isPrivate", BooleanType(), True),
        StructField("isUnpluggedCorpus", BooleanType(), True),
        StructField("isLiveContent", BooleanType(), True),
        StructField("isCrawlable", BooleanType(), True),
        StructField("isFamilySafe", BooleanType(), True),
        StructField("isUnlisted", BooleanType(), True),
        StructField("isShortsEligible", BooleanType(), True),
        
        # METRICS - Stringovi jer mogu biti veliki brojevi
        StructField("viewCount", StringType(), True),
        StructField("likeCount", StringType(), True),
        
        # GEO I DOSTUPNOST
        StructField("availableCountries", ArrayType(StringType()), True),
        
        # KATEGORIJA
        StructField("category", StringType(), True),
        
        # DATUMI - Stringovi u ISO formatu
        StructField("publishDate", StringType(), True),
        StructField("publishedAt", StringType(), True),
        StructField("uploadDate", StringType(), True),
        
        # TITLOVI
        StructField("hasCaption", BooleanType(), True),
        StructField("subtitles", ArrayType(StructType([
            StructField("languageName", StringType(), True),
            StructField("languageCode", StringType(), True),
            StructField("isTranslatable", BooleanType(), True),
            StructField("url", StringType(), True),
        ])), True),
        
    ]), True)
])

comments_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("source", StringType(), True),
    StructField("data", StructType([   
        StructField("video_id", StringType(), True),
        StructField("commentId", StringType(), True),
        StructField("authorText", StringType(), True),
        StructField("authorChannelId", StringType(), True),
        StructField("authorThumbnail", ArrayType(StructType([
            StructField("url", StringType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True),
        ])), True),
        StructField("textDisplay", StringType(), True),
        StructField("publishedTimeText", StringType(), True),
        StructField("publishDate", StringType(), True),
        StructField("publishedAt", StringType(), True),
        StructField("likesCount", StringType(), True),
        StructField("replyCount", StringType(), True),
        StructField("replyToken", StringType(), True),
        StructField("authorIsChannelOwner", BooleanType(), True),
        StructField("isVerified", BooleanType(), True),
        StructField("isArtist", BooleanType(), True),
        StructField("isCreator", BooleanType(), True)
    ]), True)
])




def parse_duration_to_seconds(duration_text):
    """Konvertuje duration text u sekunde."""
    if not duration_text:
        return 0
    
    parts = duration_text.split(':')
    if len(parts) == 2:  # MM:SS
        return int(parts[0]) * 60 + int(parts[1])
    elif len(parts) == 3:  # HH:MM:SS
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    return 0

# ---------------- STREAM READERS ----------------
def create_kafka_stream(spark, topic, schema):
    """Kreira Kafka stream sa error handling-om."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .select(F.from_json(F.col("value").cast("string"), schema).alias("parsed_data")) \
        .select("parsed_data.*") \
        .withColumn("processing_time", F.current_timestamp())


def print_stream_data(df, epoch_id, stream_name):
    """Funkcija za prikaz streaming podataka."""
    print(f"\n{'='*50}")
    print(f"EPOCH {epoch_id} - {stream_name}")
    print(f"{'='*50}")
    
    if df.count() > 0:
        print(f"Broj novih redova: {df.count()}")
        print("Schema:")
        df.printSchema()
        print("Podaci:")
        df.show(10, truncate=False)
    else:
        print("Nema novih podataka u ovom batch-u")

#upit 1

def create_channel_performance_stream(video_details_basic):
    channel_performance = video_details_basic \
        .withWatermark("details_timestamp", "30 seconds") \
        .groupBy(
            F.window(F.col("details_timestamp"), "1 minute"),
            "channel_title", "category"
        ) \
        .agg(
            F.count("*").alias("videos_count"),
            F.sum("view_count").alias("total_views"),
            F.avg("view_count").alias("avg_views_per_video"),
            F.sum("like_count").alias("total_likes"),
            F.avg("like_count").alias("avg_likes_per_video")
        ) \
        .withColumn("engagement_rate", 
                   F.col("total_likes") / F.greatest(F.col("total_views"), F.lit(1)) * 100)
    
    channel_query = channel_performance.writeStream \
        .outputMode("update") \
        .trigger(processingTime='20 seconds') \
        .foreachBatch(lambda df, epoch_id: 
            print(f"\n TOP CHANNELS PERFORMANCE - Epoch {epoch_id}") or
            df.orderBy(F.desc("total_views")) \
              .select("window.start", "channel_title", "category", 
                     "videos_count", "total_views", "avg_views_per_video", "engagement_rate") \
              .show(10, truncate=False) or
            print("="*120)
        ) \
        .start()
    
    return channel_query


#upit 2
def create_category_trends_stream(video_details_basic):
    category_trends = video_details_basic \
        .withWatermark("details_timestamp", "30 seconds") \
        .groupBy(
            F.window(F.col("details_timestamp"), "1 minute"),
            "category"
        ) \
        .agg(
            F.count("*").alias("videos_count"),
            F.avg("view_count").alias("avg_views"),
            F.avg("like_count").alias("avg_likes"),
            F.avg("length_seconds").alias("avg_duration"),
            F.approx_count_distinct("channel_id").alias("unique_channels")
        ) \
        .withColumn("popularity_score", 
                   (F.col("avg_views") + F.col("avg_likes") * 10) / F.greatest(F.col("videos_count"), F.lit(1)))
    
    category_query = category_trends.writeStream \
        .outputMode("update") \
        .trigger(processingTime='25 seconds') \
        .foreachBatch(lambda df, epoch_id: 
            print(f"\n CATEGORY TRENDS - Epoch {epoch_id}") or
            df.orderBy(F.desc("popularity_score")) \
              .select("window.start", "category", "videos_count", 
                     "avg_views", "avg_duration", "unique_channels", "popularity_score") \
              .show(10, truncate=False) or
            print("="*100)
        ) \
        .start()
    
    return category_query


#upit 3
def create_viral_potential_stream(video_details_basic):
    viral_potential = video_details_basic \
        .withWatermark("details_timestamp", "30 seconds") \
        .filter(F.col("view_count") > 10000) \
        .withColumn("views_per_minute", F.col("view_count") / F.greatest(F.col("length_seconds") / 60, F.lit(1))) \
        .withColumn("like_ratio", F.col("like_count") / F.greatest(F.col("view_count"), F.lit(1)) * 100) \
        .withColumn("viral_score", 
                   F.col("views_per_minute") * 0.3 + 
                   F.col("like_ratio") * 20 + 
                   F.col("countries_count") * 0.1) \
        .filter(F.col("viral_score") > 50)
    
    viral_query = viral_potential.writeStream \
        .outputMode("append") \
        .trigger(processingTime='15 seconds') \
        .foreachBatch(lambda df, epoch_id: 
            print(f"\n VIRAL POTENTIAL DETECTED - Epoch {epoch_id}") or
            df.orderBy(F.desc("viral_score")) \
              .select("title", "channel_title", "view_count", "like_count", 
                     "views_per_minute", "like_ratio", "viral_score") \
              .show(5, truncate=False) or
            print("="*100)
        ) \
        .start()
    
    return viral_query


#uput 4
def create_geo_analysis_stream(video_details_basic):
    geo_analysis = video_details_basic \
        .withWatermark("details_timestamp", "30 seconds") \
        .groupBy(
            F.window(F.col("details_timestamp"), "2 minutes"),
            "countries_count"
        ) \
        .agg(
            F.count("*").alias("videos_count"),
            F.avg("view_count").alias("avg_views"),
            F.collect_list("title").alias("video_titles")
        ) \
        .withColumn("global_reach", 
                   F.when(F.col("countries_count") > 200, "Global")
                    .when(F.col("countries_count") > 100, "Wide")
                    .when(F.col("countries_count") > 50, "Regional")
                    .otherwise("Local"))
    
    geo_query = geo_analysis.writeStream \
        .outputMode("update") \
        .trigger(processingTime='30 seconds') \
        .foreachBatch(lambda df, epoch_id: 
            print(f"\n GEO DISTRIBUTION - Epoch {epoch_id}") or
            df.orderBy(F.desc("countries_count")) \
              .select("window.start", "global_reach", "countries_count", 
                     "videos_count", "avg_views") \
              .show(10, truncate=False) or
            print("="*100)
        ) \
        .start()
    
    return geo_query

def load_batch_context_data(spark):
    """Učitava kontekstualne podatke iz batch obrade"""
    
    # UPIT 1: Regionalne i kategorijske performanse
    regional_performance = spark.read.jdbc(
        pg_url, "query1_category_region_analysis", properties=pg_properties
    ).select(
        "category_title", "region", 
        F.avg("avg_views").alias("historical_avg_views"),
        F.avg("avg_comments").alias("historical_avg_comments"),
        F.avg("moving_avg_views_3d").alias("trend_3d")
    ).groupBy("category_title", "region").agg(
        F.first("historical_avg_views").alias("hist_avg_views"),
        F.first("historical_avg_comments").alias("hist_avg_comments"), 
        F.first("trend_3d").alias("trend_3d")
    )
    
    # UPIT 2: Top performing kanali po kategorijama
    top_channels = spark.read.jdbc(
        pg_url, "query2_channel_engagement", properties=pg_properties
    ).filter(F.col("rank_in_category") <= 5).select(
        "category_title", "channel_title",
        "engagement_score", "avg_engagement_per_video", "rank_in_category"
    )
    
    # BONUS UPIT 1: Regionalne baseline statistike
    regional_baselines = spark.read.jdbc(
        pg_url, "bonus1_regional_executive_summary", properties=pg_properties
    ).select(
        "region", "avg_views_per_video", "avg_likes_per_video",
        "total_videos", "unique_channels"
    )
    
    return regional_performance, top_channels, regional_baselines



#upit 1 -trending
def prepare_trending_data(trending_basic):
    return trending_basic.select(
        F.col("kafka_timestamp"),
        F.col("source"),
        F.col("trending_timestamp"),
        F.col("video_id"),
        F.col("title"),
        F.col("channel_id"),
        F.col("channel_title"),
        
        # Parse view_count iz string u long
        F.regexp_replace(F.col("view_count"), "[^0-9]", "").cast("long").alias("view_count_parsed"),
        F.col("view_count").alias("view_count_raw"),
        F.col("view_count_text"),
        
        # Parse duration iz text u sekunde
        F.when(F.col("length_text").rlike("^\\d+:\\d+$"), 
               F.split(F.col("length_text"), ":")[0].cast("int") * 60 + 
               F.split(F.col("length_text"), ":")[1].cast("int"))
        .when(F.col("length_text").rlike("^\\d+:\\d+:\\d+$"),
              F.split(F.col("length_text"), ":")[0].cast("int") * 3600 +
              F.split(F.col("length_text"), ":")[1].cast("int") * 60 +
              F.split(F.col("length_text"), ":")[2].cast("int"))
        .otherwise(0).alias("duration_seconds"),
        
        F.col("length_text"),
        F.col("description"),
        F.length(F.col("description")).alias("description_length"),
        F.col("publish_date"),
        F.col("published_at"),
        F.col("content_type")
    ).filter(F.col("view_count_parsed").isNotNull() & (F.col("view_count_parsed") > 0))

# UPIT 1. Koji YouTube kanali imaju najbolje performanse u trending sekciji u okviru svakog 15-minutnog vremenskog perioda?
def create_trending_channels_analysis(trending_prepared):
    trending_channels = trending_prepared \
        .withWatermark("trending_timestamp", "20 minutes") \
        .groupBy(
            F.window(F.col("trending_timestamp"), "15 minutes"),  
            "channel_title"
        ) \
        .agg(
            F.count("*").alias("trending_videos_count"),
            F.sum("view_count_parsed").alias("total_trending_views"),
            F.avg("view_count_parsed").alias("avg_views_per_video"),
            F.avg("duration_seconds").alias("avg_duration"),
            F.avg("description_length").alias("avg_description_length")
        ) \
        .withColumn("trending_power", 
                   F.col("trending_videos_count") * F.log10(F.greatest(F.col("avg_views_per_video"), F.lit(1))))
    
    def smart_processor(df, epoch_id):
        count = df.count()
        if count > 0:
            current_time = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"\n TRENDING CHANNELS - Epoch {epoch_id} at {current_time} ({count} windows)")
            df.orderBy(F.desc("trending_power")) \
              .select("window.start", "channel_title", "trending_videos_count", 
                     "total_trending_views", "avg_views_per_video", "trending_power") \
              .show(15, truncate=False)
            print("="*120)
            
            # Dodatno: prikaži timestamp kada su podaci stigli
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f" Processed at: {current_time}")
        else:
            # Kratka poruka umesto prazne tabele
            print(f" Epoch {epoch_id}: Waiting for new trending data...")
    
    channels_query = trending_channels.writeStream \
        .outputMode("update") \
        .trigger(processingTime='5 minutes') \
        .foreachBatch(smart_processor) \
        .start()
    
    return channels_query

#upit 2 - trending
def create_content_type_analysis(trending_prepared):
    content_analysis = trending_prepared \
        .withWatermark("trending_timestamp", "20 minutes") \
        .groupBy(
            F.window(F.col("trending_timestamp"), "15 minutes"),
            "content_type"
        ) \
        .agg(
            F.count("*").alias("videos_count"),
            F.avg("view_count_parsed").alias("avg_views"),
            F.avg("duration_seconds").alias("avg_duration_seconds"),
            F.max("view_count_parsed").alias("max_views"),
            F.min("view_count_parsed").alias("min_views")
        ) \
        .withColumn("duration_category",
                   F.when(F.col("avg_duration_seconds") < 60, "Short (<1min)")
                    .when(F.col("avg_duration_seconds") < 300, "Medium (1-5min)")
                    .when(F.col("avg_duration_seconds") < 1800, "Long (5-30min)")
                    .otherwise("Very Long (30min+)"))
    
    content_query = content_analysis.writeStream \
        .outputMode("update") \
        .trigger(processingTime='25 seconds') \
        .foreachBatch(lambda df, epoch_id: 
            print(f"\n🎬 CONTENT TYPE ANALYSIS - Epoch {epoch_id}") or
            df.orderBy(F.desc("avg_views")) \
              .select("window.start", "content_type", "videos_count", "avg_views", 
                     "duration_category", "max_views") \
              .show(10, truncate=False) or
            print("="*100)
        ) \
        .start()
    
    return content_query


#upit 3 - trending
def create_trending_momentum_detector(trending_prepared):
    momentum_analysis = trending_prepared \
        .withWatermark("trending_timestamp", "30 seconds") \
        .withColumn("views_per_second", 
                   F.col("view_count_parsed") / F.greatest(F.col("duration_seconds"), F.lit(1))) \
        .withColumn("popularity_score",
                   F.log10(F.greatest(F.col("view_count_parsed"), F.lit(1))) * 
                   F.sqrt(F.greatest(F.col("views_per_second"), F.lit(0.1)))) \
        .filter(F.col("popularity_score") > 5)  # Filter for significant momentum
    
    momentum_query = momentum_analysis.writeStream \
        .outputMode("append") \
        .trigger(processingTime='15 seconds') \
        .foreachBatch(lambda df, epoch_id: 
            print(f"\n TRENDING MOMENTUM - Epoch {epoch_id}") or
            df.orderBy(F.desc("popularity_score")) \
              .select("title", "channel_title", "view_count_parsed", "duration_seconds",
                     "views_per_second", "popularity_score") \
              .show(5, truncate=False) or
            print("="*100)
        ) \
        .start()
    
    return momentum_query


#upit 4 - trending
def create_description_insights(trending_prepared):
    description_analysis = trending_prepared \
        .withWatermark("trending_timestamp", "30 seconds") \
        .withColumn("description_category",
                   F.when(F.col("description_length") == 0, "No Description")
                    .when(F.col("description_length") < 100, "Short")
                    .when(F.col("description_length") < 500, "Medium")
                    .otherwise("Long")) \
        .groupBy(
            F.window(F.col("trending_timestamp"), "3 minutes"),
            "description_category"
        ) \
        .agg(
            F.count("*").alias("videos_count"),
            F.avg("view_count_parsed").alias("avg_views"),
            F.avg("description_length").alias("avg_desc_length")
        ) \
        .withColumn("views_per_char",
                   F.col("avg_views") / F.greatest(F.col("avg_desc_length"), F.lit(1)))
    
    desc_query = description_analysis.writeStream \
        .outputMode("update") \
        .trigger(processingTime='30 seconds') \
        .foreachBatch(lambda df, epoch_id: 
            print(f"\n DESCRIPTION INSIGHTS - Epoch {epoch_id}") or
            df.orderBy(F.desc("avg_views")) \
              .select("window.start", "description_category", "videos_count", 
                     "avg_views", "avg_desc_length", "views_per_char") \
              .show(10, truncate=False) or
            print("="*100)
        ) \
        .start()
    
    return desc_query

def create_simple_windowed_analysis(video_details_basic):
    print("=== KREIRANJE SIMPLE WINDOWED ANALYSIS ===")
    
    trending_windowed = video_details_basic \
        .withWatermark("details_timestamp", "30 seconds") \
        .groupBy(
            F.window(F.col("details_timestamp"), "30 seconds"),  # Manji window
            "video_id", "title"
        ) \
        .agg(
            F.count("*").alias("updates_count"),
            F.max("view_count").alias("max_views"),
            F.min("view_count").alias("min_views")
        ) \
        .withColumn("views_diff", F.col("max_views") - F.col("min_views"))
    
    simple_query = trending_windowed.writeStream \
        .outputMode("update") \
        .trigger(processingTime='15 seconds') \
        .foreachBatch(lambda df, epoch_id: 
            print(f"\n=== SIMPLE WINDOW RESULTS - Epoch {epoch_id} ===") or
            print(f"Window results count: {df.count()}") or
            df.show(10, truncate=False) or
            print("="*80)
        ) \
        .start()
    
    return simple_query

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Pokretanje osnovnog YouTube streaming-a...")
    print(f"Kafka brokers: {KAFKA_BROKERS}")
    
    print("\n" + "="*60)
    print("KREIRANJE KAFKA STREAMOVA")
    print("="*60)

    
    print("Povezujem se na Kafka topice...")
    trending_stream = create_kafka_stream(spark, KAFKA_TOPICS["trending"], trending_schema)
    video_details_stream = create_kafka_stream(spark, KAFKA_TOPICS["video_details"], video_details_schema)
    comments_stream = create_kafka_stream(spark, KAFKA_TOPICS["comments"], comments_schema)
    
    print("Kafka streamovi kreirani!")
    
    trending_basic = trending_stream.select(
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("source"),
        F.col("data.type").alias("content_type"),
        F.col("data.videoId").alias("video_id"),
        F.col("data.title").alias("title"),

        F.col("data.channelId").alias("channel_id"),
        F.col("data.channelTitle").alias("channel_title"),
        F.col("data.channelHandle").alias("channel_handle"),
        F.col("data.channelThumbnail").alias("channel_thumbnail"),
        F.col("data.channelAvatar").alias("channel_avatar"),

        F.col("data.viewCount").alias("view_count"),
        F.col("data.viewCountText").alias("view_count_text"),
        F.col("data.publishDate").alias("publish_date"),
        F.col("data.publishedAt").alias("published_at"),
        F.col("data.lengthText").alias("length_text"),
        F.col("data.description").alias("description"),
        F.col("data.thumbnail").alias("thumbnail"),

        F.to_timestamp(F.col("processing_time")).alias("trending_timestamp")
    )
    
    video_details_basic = video_details_stream.select(
    F.col("timestamp").alias("kafka_timestamp"),
    F.col("source"),
    F.col("data").alias("video"),
    F.to_timestamp(F.col("processing_time")).alias("details_timestamp")
    ).select(
        # KAFKA META
        F.col("kafka_timestamp"),
        F.col("source"),
        F.col("details_timestamp"),
        
        # OSNOVNE INFO
        F.col("video.id").alias("video_id"),
        F.col("video.title").alias("title"),
        F.col("video.lengthSeconds").cast("int").alias("length_seconds"),  # Cast to int
        
        # KANAL
        F.col("video.channelTitle").alias("channel_title"),
        F.col("video.channelId").alias("channel_id"),
        
        # METRICS - Cast to long za brojeve
        F.col("video.viewCount").cast("long").alias("view_count"),
        F.col("video.likeCount").cast("long").alias("like_count"),
        
        # KATEGORIJA I FLAGS
        F.col("video.category").alias("category"),
        F.col("video.hasCaption").alias("has_caption"),
        F.col("video.isPrivate").alias("is_private"),
        F.col("video.isLiveContent").alias("is_live_content"),
        F.col("video.isShortsEligible").alias("is_shorts_eligible"),
        F.col("video.isFamilySafe").alias("is_family_safe"),
        
        # DATUMI - Konvertuj u timestamp format
        F.to_timestamp(F.col("video.publishDate")).alias("publish_date"),
        F.to_timestamp(F.col("video.uploadDate")).alias("upload_date"),
        
        F.col("video.availableCountries").alias("available_countries"),
        F.size(F.col("video.availableCountries")).alias("countries_count"),
        
        # THUMBNAIL INFO (ako postoji)
        F.col("video.thumbnail").alias("thumbnails"),
        F.size(F.col("video.thumbnail")).alias("thumbnail_count"),
        
        # DESCRIPTION LENGTH (korisno za analizu)
        F.length(F.col("video.description")).alias("description_length"),
        
        # SUBTITLES INFO
        F.col("video.subtitles").alias("subtitles"),
        F.size(F.col("video.subtitles")).alias("subtitles_count")
    )

    
    comments_basic = comments_stream.select(
    F.col("timestamp").alias("kafka_timestamp"),
    F.col("source"),
    F.col("data").alias("item"),
    F.to_timestamp(F.col("processing_time")).alias("comment_timestamp")
    ).select(
        F.col("kafka_timestamp"),
        F.col("source"),
        F.col("item.video_id").alias("video_id"),
        F.col("item.commentId").alias("comment_id"),
        F.col("item.authorText").alias("author_name"),
        F.col("item.authorChannelId").alias("author_channel_id"),
        F.col("item.authorThumbnail").alias("author_thumbnail"),
        F.col("item.textDisplay").alias("text_display"),
        F.col("item.publishedTimeText").alias("published_time_text"),
        F.col("item.publishDate").alias("publish_date"),
        F.col("item.publishedAt").alias("published_at"),
        F.col("item.likesCount").alias("likes_count"),
        F.col("item.replyCount").alias("reply_count"),
        F.col("item.replyToken").alias("reply_token"),
        F.col("item.authorIsChannelOwner").alias("author_is_channel_owner"),
        F.col("item.isVerified").alias("is_verified"),
        F.col("item.isArtist").alias("is_artist"),
        F.col("item.isCreator").alias("is_creator"),
        F.col("comment_timestamp")
    )


    print("Podaci normalizovani!")


    print("\n" + "="*60)
    print("POKRETANJE STREAMING QUERIES")
    print("="*60)
    
    # Query za trending podatke
    # trending_query = trending_basic.writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(lambda df, epoch: print_stream_data(df, epoch, "TRENDING VIDEOS")) \
    #     .trigger(processingTime='10 seconds') \
    #     .start()
    
    # video_details_query = video_details_basic.writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(lambda df, epoch: print_stream_data(df, epoch, "VIDEO DETAILS")) \
    #     .trigger(processingTime='10 seconds') \
    #     .start()
    
    # comments_query = comments_basic.writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(lambda df, epoch: print_stream_data(df, epoch, "COMMENTS")) \
    #     .trigger(processingTime='10 seconds') \
    #     .start()

        # 1. PRVO PROVĚRI OSNOVNE PODATKE
    # debug_basic_query = video_details_basic.writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(lambda df, epoch_id: 
    #         print(f"\n=== BASIC DATA DEBUG - Epoch {epoch_id} ===") or
    #         print(f"Count: {df.count()}") or
    #         df.select("video_id", "title", "details_timestamp", "view_count", "like_count") \
    #         .show(5, truncate=False) or
    #         print("="*60)
    #     ) \
    #     .trigger(processingTime='10 seconds') \
    #     .start()

    # # 2. PROVERAVA PRE-WINDOW PODATKE
    # pre_window_debug = video_details_basic \
    #     .withWatermark("details_timestamp", "30 seconds") \
    #     .writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(lambda df, epoch_id: 
    #         print(f"\n=== PRE-WINDOW DEBUG - Epoch {epoch_id} ===") or
    #         print(f"Rows with watermark: {df.count()}") or
    #         df.select("video_id", "details_timestamp", "view_count") \
    #         .show(3, truncate=False)
    #     ) \
    #     .trigger(processingTime='10 seconds') \
    #     .start()


    # print("=== ŠEMA VALIDACIJA ===")
    # video_details_stream_with_schema = spark.readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("subscribe", "video-details") \
    #     .load() \
    #     .select(
    #         F.col("timestamp").alias("kafka_timestamp"),
    #         F.from_json(F.col("value").cast("string"), video_details_schema).alias("parsed_data")
    #     )

    # # 2. NULL CHECK QUERY
    # null_check_query = video_details_basic.writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(lambda df, epoch_id: 
    #         df.select([
    #             F.count(F.when(F.col(c).isNull(), c)).alias(f"null_{c}") 
    #             for c in df.columns
    #         ]).show()
    #     ) \
    #     .start()

    # # 3. DATA TYPE VALIDATION
    # type_validation_query = video_details_basic.writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(lambda df, epoch_id:
    #         print(f"Batch {epoch_id}:") or
    #         df.printSchema() or
    #         df.select("video_id", "length_seconds", "view_count", "publish_date").show(5)
    #     ) \
    #     .start()
    
   


    #probni upit
    # simple_windowed_query = create_simple_windowed_analysis(video_details_basic)

    #upit 1 - video_details
    # channel_perf_query = create_channel_performance_stream(video_details_basic)

    #upit 2 - video_details
    # category_trends_query = create_category_trends_stream(video_details_basic)

    #upit 3 - video_details
    # viral_potential_query = create_viral_potential_stream(video_details_basic)

    #upit 4 - video_details
    # geo_analysis_query = create_geo_analysis_stream(video_details_basic)

    
    prepared_data = prepare_trending_data(trending_basic)


    #upit 1 - trending
    channels_query = create_trending_channels_analysis(prepared_data)

    #upit 2 - trending
    # content_query = create_content_type_analysis(prepared_data)

    #upit 3 - trending
    # momentum_query = create_trending_momentum_detector(prepared_data)

    #upit 4 - trending
    # desc_query = create_description_insights(prepared_data)
    
    print("Windowed processors pokrenuti! Čekam podatke...")
    
    print("Svi streamovi pokrenuti!")
    print("Čekam podatke iz Kafka...")
    
    spark.streams.awaitAnyTermination()




if __name__ == "__main__":
    main()




# POBOLJŠANA verzija glavnog upita
def create_intelligent_trending_analysis(trending_prepared, top_channels):
    """Poboljšana analiza trending kanala - JEDNOSTAVNIJA verzija"""
    
    trending_with_metrics = trending_prepared \
        .withWatermark("trending_timestamp", "20 minutes") \
        .groupBy(
            F.window(F.col("trending_timestamp"), "15 minutes"),
            "channel_title", "popularity_tier", "description_category"
        ) \
        .agg(
            F.count("*").alias("trending_videos_count"),
            F.sum("view_count_parsed").alias("total_trending_views"),
            F.avg("view_count_parsed").alias("avg_views_per_video"),
            F.avg("duration_seconds").alias("avg_duration"),
            F.avg("description_length").alias("avg_description_length"),
            F.max("view_count_parsed").alias("peak_video_views"),
            F.approx_count_distinct("video_id").alias("unique_videos")
        ) \
        .withColumn("trending_power", 
                   F.col("trending_videos_count") * F.log10(F.greatest(F.col("avg_views_per_video"), F.lit(1))))
    
    # Conditionally join sa batch podacima SAMO ako nisu prazni
    def try_enrich_with_batch(df, top_channels):
        try:
            if top_channels.count() > 0:
                return df.join(
                    F.broadcast(top_channels), 
                    ["channel_title"], 
                    "left"
                ).withColumn(
                    "channel_status",
                    F.when(F.col("rank_in_category").isNotNull(), 
                           F.concat(F.lit("TOP "), F.col("rank_in_category").cast("string")))
                    .otherwise("OTHER")
                ).withColumn(
                    "performance_multiplier",
                    F.when(F.col("rank_in_category") <= 2, 2.0)
                    .when(F.col("rank_in_category") <= 5, 1.5)
                    .otherwise(1.0)
                ).withColumn(
                    "intelligent_score",
                    F.col("trending_power") * F.col("performance_multiplier")
                )

                
            else:
                return df.withColumn("channel_status", F.lit("NO_BATCH_DATA")) \
                        .withColumn("intelligent_score", F.col("trending_power"))
        except Exception as e:
            print(f"WARNING: Could not join with batch data: {e}")
            return df.withColumn("channel_status", F.lit("BATCH_ERROR")) \
                    .withColumn("intelligent_score", F.col("trending_power"))
    
    enriched_trending = try_enrich_with_batch(trending_with_metrics, top_channels)
    
    def intelligent_processor(df, epoch_id):
        count = df.count()
        if count > 0:
            current_time = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"\nINTELLIGENT TRENDING ANALYSIS - Epoch {epoch_id} at {current_time} ({count} windows)")
            print("="*120)
            
            # Prikaži top performere
            print("TOP TRENDING CHANNELS:")
            df.filter(F.col("trending_videos_count") >= 1) \
              .orderBy(F.desc("intelligent_score")) \
              .select(
                  "window.start", "channel_title", "channel_status",
                  "trending_videos_count", "total_trending_views", 
                  "avg_views_per_video", "peak_video_views", "intelligent_score"
              ).show(10, truncate=False)
              
            # Content trends analiza
            print("\nTRENDING BY CONTENT TYPE:")
            content_trends = df.groupBy("description_category", "popularity_tier") \
                .agg(
                    F.sum("trending_videos_count").alias("total_videos"),
                    F.avg("avg_views_per_video").alias("avg_performance")
                ) \
                .orderBy(F.desc("total_videos"))
                
            content_trends.show(10, truncate=False)
            
            print("="*120)
            
        else:
            print(f"Epoch {epoch_id}: Waiting for trending data...")
    
    channels_query = enriched_trending.writeStream \
        .outputMode("update") \
        .trigger(processingTime='5 minutes') \
        .foreachBatch(intelligent_processor) \
        .start()
    
    return channels_query






#upit 3 - trending
def create_trending_momentum_detector(trending_prepared):
    momentum_analysis = trending_prepared \
        .withWatermark("trending_timestamp", "30 seconds") \
        .withColumn("views_per_second", 
                   F.col("view_count_parsed") / F.greatest(F.col("duration_seconds"), F.lit(1))) \
        .withColumn("popularity_score",
                   F.log10(F.greatest(F.col("view_count_parsed"), F.lit(1))) * 
                   F.sqrt(F.greatest(F.col("views_per_second"), F.lit(0.1)))) \
        .filter(F.col("popularity_score") > 5)  # Filter for significant momentum
    
    momentum_query = momentum_analysis.writeStream \
        .outputMode("append") \
        .trigger(processingTime='15 seconds') \
        .foreachBatch(lambda df, epoch_id: 
            print(f"\n TRENDING MOMENTUM - Epoch {epoch_id}") or
            df.orderBy(F.desc("popularity_score")) \
              .select("title", "channel_title", "view_count_parsed", "duration_seconds",
                     "views_per_second", "popularity_score") \
              .show(5, truncate=False) or
            print("="*100)
        ) \
        .start()
    
    return momentum_query


#upit 4 - trending
def create_description_insights(trending_prepared):
    description_analysis = trending_prepared \
        .withWatermark("trending_timestamp", "30 seconds") \
        .withColumn("description_category",
                   F.when(F.col("description_length") == 0, "No Description")
                    .when(F.col("description_length") < 100, "Short")
                    .when(F.col("description_length") < 500, "Medium")
                    .otherwise("Long")) \
        .groupBy(
            F.window(F.col("trending_timestamp"), "3 minutes"),
            "description_category"
        ) \
        .agg(
            F.count("*").alias("videos_count"),
            F.avg("view_count_parsed").alias("avg_views"),
            F.avg("description_length").alias("avg_desc_length")
        ) \
        .withColumn("views_per_char",
                   F.col("avg_views") / F.greatest(F.col("avg_desc_length"), F.lit(1)))
    
    desc_query = description_analysis.writeStream \
        .outputMode("update") \
        .trigger(processingTime='30 seconds') \
        .foreachBatch(lambda df, epoch_id: 
            print(f"\n DESCRIPTION INSIGHTS - Epoch {epoch_id}") or
            df.orderBy(F.desc("avg_views")) \
              .select("window.start", "description_category", "videos_count", 
                     "avg_views", "avg_desc_length", "views_per_char") \
              .show(10, truncate=False) or
            print("="*100)
        ) \
        .start()
    
    return desc_query

def create_simple_windowed_analysis(video_details_basic):
    print("=== KREIRANJE SIMPLE WINDOWED ANALYSIS ===")
    
    trending_windowed = video_details_basic \
        .withWatermark("details_timestamp", "30 seconds") \
        .groupBy(
            F.window(F.col("details_timestamp"), "30 seconds"),  # Manji window
            "video_id", "title"
        ) \
        .agg(
            F.count("*").alias("updates_count"),
            F.max("view_count").alias("max_views"),
            F.min("view_count").alias("min_views")
        ) \
        .withColumn("views_diff", F.col("max_views") - F.col("min_views"))
    
    simple_query = trending_windowed.writeStream \
        .outputMode("update") \
        .trigger(processingTime='15 seconds') \
        .foreachBatch(lambda df, epoch_id: 
            print(f"\n=== SIMPLE WINDOW RESULTS - Epoch {epoch_id} ===") or
            print(f"Window results count: {df.count()}") or
            df.show(10, truncate=False) or
            print("="*80)
        ) \
        .start()
    
    return simple_query
