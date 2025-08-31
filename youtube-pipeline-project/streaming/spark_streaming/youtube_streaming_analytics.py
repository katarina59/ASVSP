import os
from pyspark.sql import SparkSession # type: ignore
import pyspark.sql.functions as F # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType, BooleanType, DoubleType # type: ignore
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

# def create_channel_performance_stream(video_details_basic):
#     channel_performance = video_details_basic \
#         .withWatermark("details_timestamp", "30 seconds") \
#         .groupBy(
#             F.window(F.col("details_timestamp"), "1 minute"),
#             "channel_title", "category"
#         ) \
#         .agg(
#             F.count("*").alias("videos_count"),
#             F.sum("view_count").alias("total_views"),
#             F.avg("view_count").alias("avg_views_per_video"),
#             F.sum("like_count").alias("total_likes"),
#             F.avg("like_count").alias("avg_likes_per_video")
#         ) \
#         .withColumn("engagement_rate", 
#                    F.col("total_likes") / F.greatest(F.col("total_views"), F.lit(1)) * 100)
    
#     channel_query = channel_performance.writeStream \
#         .outputMode("update") \
#         .trigger(processingTime='20 seconds') \
#         .foreachBatch(lambda df, epoch_id: 
#             print(f"\n TOP CHANNELS PERFORMANCE - Epoch {epoch_id}") or
#             df.orderBy(F.desc("total_views")) \
#               .select("window.start", "channel_title", "category", 
#                      "videos_count", "total_views", "avg_views_per_video", "engagement_rate") \
#               .show(10, truncate=False) or
#             print("="*120)
#         ) \
#         .start()
    
#     return channel_query



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

# def load_batch_context_data(spark):
#     """Učitava kontekstualne podatke iz batch obrade"""
    
#     # UPIT 1: Regionalne i kategorijske performanse
#     regional_performance = spark.read.jdbc(
#         pg_url, "query1_category_region_analysis", properties=pg_properties
#     ).select(
#         "category_title", "region", 
#         F.avg("avg_views").alias("historical_avg_views"),
#         F.avg("avg_comments").alias("historical_avg_comments"),
#         F.avg("moving_avg_views_3d").alias("trend_3d")
#     ).groupBy("category_title", "region").agg(
#         F.first("historical_avg_views").alias("hist_avg_views"),
#         F.first("historical_avg_comments").alias("hist_avg_comments"), 
#         F.first("trend_3d").alias("trend_3d")
#     )
    
#     # UPIT 2: Top performing kanali po kategorijama
#     top_channels = spark.read.jdbc(
#         pg_url, "query2_channel_engagement", properties=pg_properties
#     ).filter(F.col("rank_in_category") <= 5).select(
#         "category_title", "channel_title",
#         "engagement_score", "avg_engagement_per_video", "rank_in_category"
#     )
    
#     # BONUS UPIT 1: Regionalne baseline statistike
#     regional_baselines = spark.read.jdbc(
#         pg_url, "bonus1_regional_executive_summary", properties=pg_properties
#     ).select(
#         "region", "avg_views_per_video", "avg_likes_per_video",
#         "total_videos", "unique_channels"
#     )
    
#     return regional_performance, top_channels, regional_baselines


def load_batch_context_data(spark):
    """Učitava kontekstualne podatke iz batch obrade - ISPRAVLJENA verzija"""
    
    try:
        # UPIT 1: Regionalne i kategorijske performanse - ISPRAVKA
        regional_performance = spark.read.jdbc(
            pg_url, "query1_category_region_analysis", properties=pg_properties
        ).groupBy("category_title", "region").agg(
            F.avg("avg_views").alias("hist_avg_views"),
            F.avg("avg_comments").alias("hist_avg_comments"),
            F.avg("moving_avg_views_3d").alias("trend_3d")
        )
        
        # UPIT 2: Top performing kanali po kategorijama
        top_channels = spark.read.jdbc(
            pg_url, "query2_channel_engagement", properties=pg_properties
        ).filter(F.col("rank_in_category") <= 5).select(
            "category_title", "channel_title",
            "engagement_score", "avg_engagement_per_video", "rank_in_category"
        )
        
        # BONUS UPIT 1: Regionalne baseline statistike - PROVERI DA LI POSTOJI
        try:
            regional_baselines = spark.read.jdbc(
                pg_url, "bonus1_regional_executive_summary", properties=pg_properties
            ).select(
                "region", "avg_views_per_video", "avg_likes_per_video",
                "total_videos", "unique_channels"
            )
        except Exception as e:
            print(f"WARNING: Could not load regional baselines: {e}")
            # Kreiraj prazan DataFrame sa potrebnim kolonama
            
            schema = StructType([
                StructField("region", StringType(), True),
                StructField("avg_views_per_video", DoubleType(), True),
                StructField("avg_likes_per_video", DoubleType(), True),
                StructField("total_videos", LongType(), True),
                StructField("unique_channels", LongType(), True)
            ])
            regional_baselines = spark.createDataFrame([], schema)
        
        print(" Successfully loaded batch context data:")
        print(f"   - Regional performance: {regional_performance.count()} records")
        print(f"   - Top channels: {top_channels.count()} records") 
        print(f"   - Regional baselines: {regional_baselines.count()} records")
        
        return regional_performance, top_channels, regional_baselines
        
    except Exception as e:
        print(f"ERROR loading batch context data: {e}")

        rp_schema = StructType([
            StructField("category_title", StringType(), True),
            StructField("region", StringType(), True),
            StructField("hist_avg_views", DoubleType(), True),
            StructField("hist_avg_comments", DoubleType(), True),
            StructField("trend_3d", DoubleType(), True)
        ])
        regional_performance = spark.createDataFrame([], rp_schema)
        
        tc_schema = StructType([
            StructField("category_title", StringType(), True),
            StructField("channel_title", StringType(), True),
            StructField("engagement_score", DoubleType(), True),
            StructField("avg_engagement_per_video", DoubleType(), True),
            StructField("rank_in_category", LongType(), True)
        ])
        top_channels = spark.createDataFrame([], tc_schema)
        
        rb_schema = StructType([
            StructField("region", StringType(), True),
            StructField("avg_views_per_video", DoubleType(), True),
            StructField("avg_likes_per_video", DoubleType(), True),
            StructField("total_videos", LongType(), True),
            StructField("unique_channels", LongType(), True)
        ])
        regional_baselines = spark.createDataFrame([], rb_schema)
        
        return regional_performance, top_channels, regional_baselines

# prepare_trending_data bez batch join-a
def prepare_trending_data_enhanced(trending_basic):
    """Priprema trending podatke sa dodatnim metrikama - BEZ batch join-a"""
    return trending_basic.select(
        F.col("kafka_timestamp"),
        F.col("source"),  
        F.col("trending_timestamp"),
        F.col("video_id"),
        F.col("title"),
        F.col("channel_id"),
        F.col("channel_title"),
        
        F.regexp_replace(F.col("view_count"), "[^0-9]", "").cast("long").alias("view_count_parsed"),
        F.col("view_count").alias("view_count_raw"),
        
        F.when(F.col("length_text").rlike("^\\d+:\\d+$"), 
               F.split(F.col("length_text"), ":")[0].cast("int") * 60 + 
               F.split(F.col("length_text"), ":")[1].cast("int"))
        .when(F.col("length_text").rlike("^\\d+:\\d+:\\d+$"),
              F.split(F.col("length_text"), ":")[0].cast("int") * 3600 +
              F.split(F.col("length_text"), ":")[1].cast("int") * 60 +
              F.split(F.col("length_text"), ":")[2].cast("int"))
        .otherwise(0).alias("duration_seconds"),
        
        F.col("description"),
        F.length(F.col("description")).alias("description_length"),
        F.col("publish_date"),
        F.col("published_at"),
        
        F.when(F.length(F.col("description")) == 0, "No Description")
        .when(F.length(F.col("description")) < 100, "Very Short")
        .when(F.length(F.col("description")) < 300, "Short")
        .when(F.length(F.col("description")) < 1000, "Medium") 
        .when(F.length(F.col("description")) < 2000, "Long")
        .otherwise("Very Long").alias("description_category"),
        
        F.when(F.col("view_count_parsed") >= 10000000, "Mega Hit (10M+)")
        .when(F.col("view_count_parsed") >= 1000000, "Viral (1M+)")
        .when(F.col("view_count_parsed") >= 100000, "Popular (100K+)")
        .when(F.col("view_count_parsed") >= 10000, "Rising (10K+)")
        .otherwise("New/Small").alias("popularity_tier"),
        
        F.col("content_type")
    ).filter(F.col("view_count_parsed").isNotNull() & (F.col("view_count_parsed") > 0))


# UPIT 1: Koji kanali trenutno dominiraju YouTube trending listom u poslednje 15 minuta, 
#         kada se uporede sa njihovim istorijskim performansama iz batch analize, i koji od njih predstavljaju neočekivane viral 
#         fenomene koji nisu bili u top performerima?
def create_intelligent_trending_analysis_v2(trending_prepared, regional_performance, top_channels, regional_baselines):
    """NAPREDNA verzija sa pravim kontekstualnim obogaćivanjem"""
    
    # 1. KREIRANJE BASELINE METRIKA iz batch podataka
    category_baselines = regional_performance.groupBy("category_title").agg(
        F.avg("hist_avg_views").alias("category_avg_views"),
        F.avg("hist_avg_comments").alias("category_avg_comments"),
        F.avg("trend_3d").alias("category_trend")
    )
    
    # 2. REGIONALNE PERFORMANCE THRESHOLDS
    regional_thresholds = regional_baselines.select(
        "region",
        F.col("avg_views_per_video").alias("regional_baseline_views"),
        F.col("avg_likes_per_video").alias("regional_baseline_likes")
    )
    
    # 3. OSNOVNI REAL-TIME AGREGATI
    trending_with_metrics = trending_prepared \
        .withWatermark("trending_timestamp", "20 minutes") \
        .groupBy(
            F.window(F.col("trending_timestamp"), "15 minutes"),
            "channel_title", "popularity_tier", "description_category", "content_type"
        ) \
        .agg(
            F.count("*").alias("trending_videos_count"),
            F.sum("view_count_parsed").alias("total_trending_views"),
            F.avg("view_count_parsed").alias("avg_views_per_video"),
            F.avg("duration_seconds").alias("avg_duration"),
            F.max("view_count_parsed").alias("peak_video_views"),
            F.approx_count_distinct("video_id").alias("unique_videos"),
            # DODATNE METRIKE
            F.stddev("view_count_parsed").alias("view_consistency"),
            F.min("view_count_parsed").alias("min_views")
        )
    
    def intelligent_enrichment(df):
        
        enriched = df.join(F.broadcast(top_channels), ["channel_title"], "left") \
            .withColumn("is_established_channel", 
                       F.when(F.col("rank_in_category").isNotNull(), True).otherwise(False))
        
        enriched = enriched.withColumn(
            "viral_anomaly_score",
            F.when(
                (F.col("popularity_tier") == "Viral (1M+)") & 
                (F.col("is_established_channel") == False), 
                F.col("avg_views_per_video") / 1000000  # Score za neočekivane virale
            ).otherwise(0)
        )
        
        enriched = enriched.withColumn(
            "content_velocity",
            F.col("trending_videos_count") / F.greatest(F.col("avg_duration") / 60, F.lit(1))  # Videos per minute of content
        )
        
        enriched = enriched.withColumn(
            "engagement_consistency", 
            F.when(F.col("view_consistency").isNull(), 1.0)
            .otherwise(1.0 / (1.0 + F.col("view_consistency") / F.col("avg_views_per_video")))
        )
        
        enriched = enriched.withColumn(
            "intelligent_score_v2",
            (F.col("trending_videos_count") * F.log10(F.greatest(F.col("avg_views_per_video"), F.lit(1)))) * 
            
            (1.0 + F.col("viral_anomaly_score") * 0.5) *  # Bonus za neočekivane virale
            (1.0 + F.col("content_velocity") * 0.3) *     # Bonus za visoku produktivnost  
            F.col("engagement_consistency") *              # Penalizuj inconsistent content
            
            F.when(F.col("rank_in_category") <= 2, 1.2)
            .when(F.col("rank_in_category") <= 5, 1.1)
            .otherwise(1.0)
        )
        
        return enriched
    
    enriched_trending = intelligent_enrichment(trending_with_metrics)
    
    def advanced_processor(df, epoch_id):
        count = df.count()
        if count > 0:
            current_time = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"\n ADVANCED TRENDING INTELLIGENCE - Epoch {epoch_id} at {current_time}")
            print("="*140)
            
            # TOP TRENDING sa više konteksta
            print(" TOP TRENDING CHANNELS (Multi-dimensional scoring):")
            top_trending = df.filter(F.col("trending_videos_count") >= 1) \
                .orderBy(F.desc("intelligent_score_v2")) \
                .select(
                    "window.start", "channel_title", 
                    F.when(F.col("is_established_channel"), " ESTABLISHED").otherwise(" EMERGING").alias("status"),
                    "trending_videos_count", 
                    F.round("avg_views_per_video", 0).alias("avg_views"),
                    F.round("viral_anomaly_score", 2).alias("viral_score"),
                    F.round("content_velocity", 2).alias("velocity"),
                    F.round("engagement_consistency", 2).alias("consistency"),
                    F.round("intelligent_score_v2", 1).alias("intelligent_score")
                )
            top_trending.show(15, truncate=False)
            
            print("\n VIRAL ANOMALY DETECTION (Unexpected breakouts):")
            viral_anomalies = df.filter(F.col("viral_anomaly_score") > 0.5) \
                .orderBy(F.desc("viral_anomaly_score")) \
                .select(
                    "channel_title", "trending_videos_count", 
                    F.round("avg_views_per_video", 0).alias("avg_views"),
                    F.round("viral_anomaly_score", 2).alias("anomaly_score"),
                    "popularity_tier"
                )
            
            if viral_anomalies.count() > 0:
                viral_anomalies.show(10, truncate=False)
            else:
                print("   No significant viral anomalies detected.")
            
            # CONTENT STRATEGY INSIGHTS - ovo mislim da nema potrebe da se prikazuje
            # print("\n CONTENT STRATEGY INSIGHTS:")
            # strategy_insights = df.groupBy("description_category", "popularity_tier") \
            #     .agg(
            #         F.sum("trending_videos_count").alias("total_videos"),
            #         F.round(F.avg("avg_views_per_video"), 0).alias("avg_performance"),
            #         F.round(F.avg("content_velocity"), 2).alias("avg_velocity"),
            #         F.round(F.avg("engagement_consistency"), 2).alias("avg_consistency")
            #     ) \
            #     .orderBy(F.desc("total_videos"))
                
            # strategy_insights.show(10, truncate=False)
            
            print("="*140)
            
        else:
            print(f"Epoch {epoch_id}: Waiting for trending data...")
    
    query = enriched_trending.writeStream \
        .outputMode("update") \
        .trigger(processingTime='3 minutes') \
        .foreachBatch(advanced_processor) \
        .start()
    
    return query


# UPIT 2. Koje su performanse YouTube kategorija (broj videa, pregledi i engagement) u realnom vremenu, 
#         kako se one upoređuju sa istorijskim vrednostima i koji su ukupni trendovi, 
#         uključujući kategorije koje rastu ili opadaju i top dobitnici i gubitnici?
def create_category_summary_stream(video_details_basic, spark):
    """
    Streaming upit koji sumira po kategorijama i poredi sa batch rezultatima
    """
    
    # Učitaj istorijske kategorial podatke iz batch upita
    historical_categories = spark.read.jdbc(
        pg_url,
        "query1_category_region_analysis", 
        properties=pg_properties
    ).groupBy("category_title") \
        .agg(
            F.sum("video_count").alias("historical_total_videos"),
            F.avg("avg_views").alias("historical_avg_views"),
            F.avg("avg_comments").alias("historical_avg_comments"),
            F.approx_count_distinct("region").alias("historical_regions_count")
        )
    
    # Engagement po kategorijama iz batch-a
    historical_engagement_by_cat = spark.read.jdbc(
        pg_url,
        "query2_channel_engagement",
        properties=pg_properties
    ).groupBy("category_title") \
        .agg(
            F.sum("engagement_score").alias("historical_total_engagement"),
            F.avg("avg_engagement_per_video").alias("historical_avg_engagement_per_video"),
            F.approx_count_distinct("channel_title").alias("historical_unique_channels")
        )
    
    # Real-time kategorial analiza
    category_performance = video_details_basic \
        .withWatermark("details_timestamp", "30 seconds") \
        .groupBy(
            F.window(F.col("details_timestamp"), "2 minutes"),
            "category"
        ) \
        .agg(
            F.count("*").alias("current_videos_count"),
            F.approx_count_distinct("channel_title").alias("current_unique_channels"),
            F.sum("view_count").alias("current_total_views"),
            F.avg("view_count").alias("current_avg_views"),
            F.sum("like_count").alias("current_total_likes"),
            F.avg("like_count").alias("current_avg_likes"),
            F.max("view_count").alias("current_max_views"),
            F.min("view_count").alias("current_min_views")
        ) \
        .withColumn("current_engagement_rate",
            F.round(F.col("current_total_likes") / F.greatest(F.col("current_total_views"), F.lit(1)) * 100, 2)
        ) \
        .withColumn("current_avg_engagement_per_video",
            F.round(F.col("current_total_likes") / F.greatest(F.col("current_videos_count"), F.lit(1)), 2)
        )
    
    # Dodaj debug korak da vidimo koje kategorije imamo
    def debug_categories(df, epoch_id):
        print(f"\n DEBUG - Current streaming categories in epoch {epoch_id}:")
        df.select("category").distinct().show(truncate=False)
        
    # Prvo prikaži trenutne kategorije za debug
    debug_stream = category_performance.writeStream \
        .outputMode("update") \
        .trigger(processingTime='60 seconds') \
        .foreachBatch(debug_categories) \
        .start()
    
    # Spoji sa istorijskim podacima - dodaj case-insensitive matching
    enriched_categories = category_performance.alias("current") \
        .join(
            historical_categories.alias("hist_cat"),
            F.upper(F.trim(F.col("current.category"))) == F.upper(F.trim(F.col("hist_cat.category_title"))),
            "left"
        ) \
        .join(
            historical_engagement_by_cat.alias("hist_eng"),
            F.upper(F.trim(F.col("current.category"))) == F.upper(F.trim(F.col("hist_eng.category_title"))),
            "left"
        ) \
        .select(
            "current.*",
            "hist_cat.historical_total_videos",
            "hist_cat.historical_avg_views",
            "hist_cat.historical_regions_count",
            "hist_eng.historical_total_engagement",
            "hist_eng.historical_avg_engagement_per_video",
            "hist_eng.historical_unique_channels"
        ) \
        .withColumn("views_performance_vs_historical",
            F.when(F.col("historical_avg_views").isNotNull() & (F.col("historical_avg_views") > 0),
                F.round(((F.col("current_avg_views") - F.col("historical_avg_views")) / 
                        F.col("historical_avg_views")) * 100, 2)
            ).otherwise(F.lit(None))
        ) \
        .withColumn("engagement_performance_vs_historical",
            F.when(F.col("historical_avg_engagement_per_video").isNotNull() & (F.col("historical_avg_engagement_per_video") > 0),
                F.round(((F.col("current_avg_engagement_per_video") - F.col("historical_avg_engagement_per_video")) / 
                        F.col("historical_avg_engagement_per_video")) * 100, 2)
            ).otherwise(F.lit(None))
        ) \
        .withColumn("channel_diversity_vs_historical",
            F.when(F.col("historical_unique_channels").isNotNull() & (F.col("historical_unique_channels") > 0),
                F.round((F.col("current_unique_channels").cast("double") / F.col("historical_unique_channels").cast("double")) * 100, 2)
            ).otherwise(F.lit(None))
        ) \
        .withColumn("category_trend_indicator",
            F.when(F.col("views_performance_vs_historical") > 25, "HOT_CATEGORY")
            .when(F.col("views_performance_vs_historical") > 0, "GROWING_CATEGORY") 
            .when(F.col("views_performance_vs_historical") > -25, "STABLE_CATEGORY")
            .when(F.col("views_performance_vs_historical").isNotNull(), "DECLINING_CATEGORY")
            .otherwise("NEW_CATEGORY")
        ) \
        .withColumn("market_position",
            F.when(F.col("current_total_views") > 10000000, "DOMINANT")
            .when(F.col("current_total_views") > 5000000, "STRONG")
            .when(F.col("current_total_views") > 1000000, "MODERATE")
            .otherwise("EMERGING")
        )

    # Streaming query sa unified comparison view
    category_query = enriched_categories.writeStream \
        .outputMode("update") \
        .trigger(processingTime='25 seconds') \
        .foreachBatch(lambda df, epoch_id:
            print(f"\n UNIFIED CATEGORY COMPARISON: CURRENT vs HISTORICAL - Epoch {epoch_id}") or
            print("="*180) or
            
            # GLAVNA TABELA - sve metrike u jednoj tabeli
            df.orderBy(F.desc("current_total_views")) \
                .select(
                    "window.start",
                    "category",
                    
                    # VIDEO COUNT COMPARISON
                    "current_videos_count", 
                    F.coalesce(F.round("historical_total_videos", 0), F.lit(0)).alias("hist_videos"),
                    
                    # CHANNEL COUNT COMPARISON  
                    "current_unique_channels",
                    F.coalesce("historical_unique_channels", F.lit(0)).alias("hist_channels"),
                    
                    # VIEWS COMPARISON
                    F.round("current_total_views", 0).alias("current_views"),
                    F.round("current_avg_views", 0).alias("current_avg"),
                    F.coalesce(F.round("historical_avg_views", 0), F.lit(0)).alias("hist_avg"),
                    F.coalesce("views_performance_vs_historical", F.lit(0)).alias("views_change_%"),
                    
                    # ENGAGEMENT COMPARISON  
                    F.round("current_engagement_rate", 1).alias("curr_eng_%"),
                    F.coalesce(F.round("historical_avg_engagement_per_video", 1), F.lit(0)).alias("hist_eng"),
                    F.coalesce("engagement_performance_vs_historical", F.lit(0)).alias("eng_change_%"),
                    
                    # TREND INDICATORS
                    "category_trend_indicator",
                    "market_position"
                ) \
                .show(15, truncate=False) or
            print("="*180) or
            
            # SUMMARY STATISTICS TABLE
            print("\n CATEGORY PERFORMANCE SUMMARY:") or
            df.agg(
                F.sum("current_videos_count").alias("total_current_videos"),
                F.sum(F.coalesce("historical_total_videos", F.lit(0))).alias("total_historical_videos"), 
                F.avg("views_performance_vs_historical").alias("avg_views_change"),
                F.avg("engagement_performance_vs_historical").alias("avg_engagement_change"),
                F.count("*").alias("categories_analyzed"),
                F.sum(F.when(F.col("views_performance_vs_historical").isNull(), 1).otherwise(0)).alias("new_categories"),
                F.sum(F.when(F.col("views_performance_vs_historical") > 0, 1).otherwise(0)).alias("growing_categories"),
                F.sum(F.when(F.col("views_performance_vs_historical") < 0, 1).otherwise(0)).alias("declining_categories")
            ).select(
                "total_current_videos",
                "total_historical_videos",
                F.round("avg_views_change", 2).alias("avg_views_change_%"),
                F.round("avg_engagement_change", 2).alias("avg_eng_change_%"),
                "categories_analyzed", 
                "new_categories",
                "growing_categories",
                "declining_categories"
            ).show(truncate=False) or
            
            # TOP MOVERS TABLE
            print("\n TOP CATEGORY MOVERS (Best/Worst Performance):") or
            df.filter(F.col("views_performance_vs_historical").isNotNull()) \
                .select(
                    "category",
                    "views_performance_vs_historical",
                    "engagement_performance_vs_historical", 
                    "current_total_views",
                    "category_trend_indicator"
                ) \
                .orderBy(F.desc("views_performance_vs_historical")) \
                .show(8, truncate=False) or
            print("="*180)
        ) \
        .start()
    
    return category_query



# UPIT 3
def create_regional_performance_stream(video_details_basic, spark):
    """
    Kreira real-time stream koji poredi trenutne performanse regiona 
    sa istorijskim batch podacima
    """
    
    # Učitaj regionalne baseline podatke iz batch analize
    regional_baseline = spark.read.jdbc(
        pg_url, 
        "query1_category_region_analysis", 
        properties=pg_properties
    ).groupBy("region").agg(
        F.avg("avg_views").alias("historical_avg_views"),
        F.avg("avg_comments").alias("historical_avg_comments"),
        F.count("*").alias("historical_video_count"),
        F.max("avg_views").alias("historical_peak_views")
    )
    
    # KORAK 2: Mapiranje zemalja na regione (simplifikovano)
    country_to_region_map = {
        # Severna Amerika
        "US": "North America", "CA": "North America", "MX": "North America",
        # Evropa  
        "GB": "Europe", "DE": "Europe", "FR": "Europe", "IT": "Europe", 
        "ES": "Europe", "NL": "Europe", "PL": "Europe", "RU": "Europe",
        "SE": "Europe", "NO": "Europe", "FI": "Europe", "DK": "Europe",
        # Azija
        "JP": "Asia", "KR": "Asia", "CN": "Asia", "IN": "Asia", 
        "TH": "Asia", "VN": "Asia", "PH": "Asia", "ID": "Asia",
        # Južna Amerika
        "BR": "South America", "AR": "South America", "CL": "South America",
        "CO": "South America", "PE": "South America",
        # Ostalo
        "AU": "Oceania", "NZ": "Oceania",
        "ZA": "Africa", "NG": "Africa", "EG": "Africa"
    }
    
    # Registruj UDF za mapiranje zemalja u regione
    def map_countries_to_regions(countries_array):
        if not countries_array:
            return []
        regions = set()
        for country in countries_array:
            if country in country_to_region_map:
                regions.add(country_to_region_map[country])
        return list(regions)
    
    map_countries_udf = F.udf(map_countries_to_regions, ArrayType(StringType()))
    
    # KORAK 3: Transformiši real-time podatke
    regional_stream = video_details_basic \
        .withWatermark("details_timestamp", "1 minute") \
        .withColumn("detected_regions", map_countries_udf(F.col("available_countries"))) \
        .withColumn("region", F.explode(F.col("detected_regions"))) \
        .groupBy(
            F.window(F.col("details_timestamp"), "3 minutes", "1 minute"),
            "region"
        ) \
        .agg(
            F.count("*").alias("current_videos"),
            F.avg("view_count").alias("current_avg_views"),
            F.avg("like_count").alias("current_avg_likes"),
            F.avg("countries_count").alias("avg_global_reach"),
            F.approx_count_distinct("category").alias("category_diversity"),
            F.collect_list("title").alias("recent_titles"),
            F.max("view_count").alias("peak_views_now"),
            F.stddev("view_count").alias("views_volatility")
        )
    
    # KORAK 4: Stream sa poređenjem protiv baseline-a
    def process_regional_comparison(df, epoch_id):
        print(f"\n REGIONAL PERFORMANCE COMPARISON - Epoch {epoch_id}")
        print("="*100)
        
        if df.count() == 0:
            print("No data in this batch")
            return
            
        # Join sa baseline podacima
        comparison_df = df.join(
            regional_baseline.hint("broadcast"), 
            on="region", 
            how="left"
        ).withColumn(
            "performance_vs_history",
            F.when(F.col("historical_avg_views").isNull(), "NEW_REGION")
            .when(F.col("current_avg_views") > F.col("historical_avg_views") * 1.5, "SURGING")
            .when(F.col("current_avg_views") > F.col("historical_avg_views") * 1.2, "GROWING") 
            .when(F.col("current_avg_views") > F.col("historical_avg_views") * 0.8, "STABLE")
            .when(F.col("current_avg_views") > F.col("historical_avg_views") * 0.5, "DECLINING")
            .otherwise("STRUGGLING")
        ).withColumn(
            "growth_percentage",
            F.when(F.col("historical_avg_views") > 0,
                F.round(((F.col("current_avg_views") - F.col("historical_avg_views")) 
                        / F.col("historical_avg_views")) * 100, 1)
            ).otherwise(None)
        ).withColumn(
            "market_position",
            F.when(F.col("current_avg_views") > 1000000, "PREMIUM")
            .when(F.col("current_avg_views") > 500000, "HIGH_VALUE") 
            .when(F.col("current_avg_views") > 100000, "SOLID")
            .when(F.col("current_avg_views") > 50000, "EMERGING")
            .otherwise("DEVELOPING")
        )
        
        # Prikaz rezultata
        print("\n REGIONAL PERFORMANCE DASHBOARD:")
        comparison_df.select(
            "region",
            "performance_vs_history",
            "market_position", 
            F.col("current_videos").alias("videos_now"),
            F.col("current_avg_views").alias("avg_views_now"),
            F.col("historical_avg_views").alias("historical_avg"),
            "growth_percentage",
            F.col("category_diversity").alias("categories"),
            F.col("avg_global_reach").alias("global_reach")
        ).orderBy(F.desc("current_avg_views")).show(15, truncate=False)
        
        # Detaljne insights
        print("\n KEY INSIGHTS:")
        
        # Top performers
        top_regions = comparison_df.filter(
            F.col("performance_vs_history").isin("SURGING", "GROWING")
        ).count()
        
        struggling_regions = comparison_df.filter(
            F.col("performance_vs_history").isin("STRUGGLING", "DECLINING")  
        ).count()
        
        new_regions = comparison_df.filter(
            F.col("performance_vs_history") == "NEW_REGION"
        ).count()
        
        print(f"  • Regions outperforming history: {top_regions}")
        print(f"  • Regions underperforming: {struggling_regions}") 
        print(f"  • New regions detected: {new_regions}")
        
        # Content diversity analysis
        diversity_stats = comparison_df.agg(
            F.avg("category_diversity").alias("avg_diversity"),
            F.max("category_diversity").alias("max_diversity")
        ).collect()[0]
        
        print(f"  • Average category diversity: {diversity_stats['avg_diversity']:.1f}")
        print(f"  • Highest category diversity: {diversity_stats['max_diversity']}")
        
        # Global reach insights
        global_reach_stats = comparison_df.agg(
            F.avg("avg_global_reach").alias("avg_reach"),
            F.max("avg_global_reach").alias("max_reach")
        ).collect()[0]
        
        print(f"  • Average global reach: {global_reach_stats['avg_reach']:.0f} countries")
        print(f"  • Maximum global reach: {global_reach_stats['max_reach']:.0f} countries")
        
        print("\n" + "="*100)
    
    # KORAK 5: Kreiraj stream
    regional_query = regional_stream.writeStream \
        .outputMode("update") \
        .trigger(processingTime='45 seconds') \
        .foreachBatch(process_regional_comparison) \
        .start()
    
    return regional_query


def create_trend_analysis_stream(video_details_basic, spark):
    """
    Dodatni stream za dublje analize trendova po regionima
    """
    
    # Mapiranje kao gore (možeš izdvojiti u pomocnu funkciju)
    country_to_region_map = {
        "US": "North America", "CA": "North America", "MX": "North America",
        "GB": "Europe", "DE": "Europe", "FR": "Europe", "IT": "Europe", 
        "ES": "Europe", "NL": "Europe", "PL": "Europe", "RU": "Europe",
        "JP": "Asia", "KR": "Asia", "CN": "Asia", "IN": "Asia",
        "BR": "South America", "AR": "South America", "CL": "South America",
        "AU": "Oceania", "NZ": "Oceania"
    }
    
    def map_countries_to_regions(countries_array):
        if not countries_array:
            return []
        regions = set()
        for country in countries_array:
            if country in country_to_region_map:
                regions.add(country_to_region_map[country])
        return list(regions)
    
    map_countries_udf = F.udf(map_countries_to_regions, ArrayType(StringType()))
    
    trend_analysis = video_details_basic \
        .withWatermark("details_timestamp", "2 minutes") \
        .withColumn("detected_regions", map_countries_udf(F.col("available_countries"))) \
        .withColumn("region", F.explode(F.col("detected_regions"))) \
        .withColumn("hour", F.hour("details_timestamp")) \
        .groupBy(
            F.window(F.col("details_timestamp"), "5 minutes", "2 minutes"),
            "region",
            "hour"
        ) \
        .agg(
            F.count("*").alias("hourly_videos"),
            F.avg("view_count").alias("hourly_avg_views"),
            F.sum("like_count").alias("total_likes"),
            F.approx_count_distinct("channel_title").alias("unique_channels"),
            F.avg("length_seconds").alias("avg_duration"),
            F.sum(F.when(F.col("is_shorts_eligible"), 1).otherwise(0)).alias("shorts_count"),
            F.sum(F.when(F.col("is_live_content"), 1).otherwise(0)).alias("live_count")
        ) \
        .withColumn("shorts_ratio", 
                   F.round(F.col("shorts_count") / F.col("hourly_videos") * 100, 1))
    
    def process_trend_analysis(df, epoch_id):
        print(f"\n HOURLY TREND ANALYSIS - Epoch {epoch_id}")
        print("="*80)
        
        if df.count() == 0:
            print("No trend data available")
            return
            
        # Analiza po satima
        hourly_summary = df.groupBy("hour").agg(
            F.sum("hourly_videos").alias("total_videos"),
            F.avg("hourly_avg_views").alias("avg_views_per_hour"),
            F.sum("unique_channels").alias("total_channels")
        ).orderBy("hour")
        
        print("\n HOURLY ACTIVITY PATTERN:")
        hourly_summary.show(24, truncate=False)
        
        # Top performing regioni u ovom trenutku  
        print("\n TOP PERFORMING REGIONS THIS PERIOD:")
        df.select("region", "hourly_videos", "hourly_avg_views", 
                 "unique_channels", "shorts_ratio") \
          .orderBy(F.desc("hourly_avg_views")) \
          .show(10, truncate=False)
    
    trend_query = trend_analysis.writeStream \
        .outputMode("update") \
        .trigger(processingTime='60 seconds') \
        .foreachBatch(process_trend_analysis) \
        .start()
    
    return trend_query



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
            print(f"\n CONTENT TYPE ANALYSIS - Epoch {epoch_id}") or
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


    #upit 1 - video_details
    # channel_perf_query = create_channel_performance_stream(video_details_basic)


    #upit 2 - video_details
    # category_trends_query = create_category_trends_stream(video_details_basic)

    #upit 3 - video_details
    # viral_potential_query = create_viral_potential_stream(video_details_basic)

    #upit 4 - video_details
    geo_analysis_query = create_geo_analysis_stream(video_details_basic)

    
    #priprema podataka
    # prepared_data = prepare_trending_data_enhanced(trending_basic)


    

    #upit 2 - trending
    # content_query = create_content_type_analysis(prepared_data)

    #upit 3 - trending
    # momentum_query = create_trending_momentum_detector(prepared_data)

    #upit 4 - trending
    # desc_query = create_description_insights(prepared_data)


    #UPIT1
    # print("Loading batch context data...")
    # regional_performance, top_channels, regional_baselines = load_batch_context_data(spark)
        
    # print("Preparing streaming data...")
    # trending_prepared = prepare_trending_data_enhanced(trending_basic)
    
    # print("Starting intelligent trending analysis...")
    # enhanced_query = create_intelligent_trending_analysis_v2(
    #         trending_prepared, regional_performance, top_channels, regional_baselines
    #     )


    #UPIT2
    # channel_perf_query = create_category_summary_stream(video_details_basic, spark)

    #UPIT3
    regional_query = create_regional_performance_stream(video_details_basic, spark)
    
    # Pokreni trend analizu
    trend_query = create_trend_analysis_stream(video_details_basic, spark)
    
    print("Windowed processors pokrenuti! Čekam podatke...")
    
    print("Svi streamovi pokrenuti!")
    print("Čekam podatke iz Kafka...")
    
    spark.streams.awaitAnyTermination()




if __name__ == "__main__":
    main()
