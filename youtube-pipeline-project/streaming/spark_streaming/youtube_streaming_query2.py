import os
from pyspark.sql import SparkSession # type: ignore
import pyspark.sql.functions as F # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType, BooleanType # type: ignore


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")

pg_url = "jdbc:postgresql://postgres:5432/airflow"
pg_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}


KAFKA_TOPICS = {
    "video_details": "youtube_video_details"
}


def create_spark_session():
    return SparkSession.builder \
        .appName("YouTube-Advanced-Analytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


video_details_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("source", StringType(), True),
    StructField("data", StructType([
        
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("lengthSeconds", StringType(), True),  
        
        
        StructField("channelId", StringType(), True),
        StructField("channelTitle", StringType(), True),
        
        StructField("description", StringType(), True),
        
        StructField("thumbnail", ArrayType(StructType([
            StructField("url", StringType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True),
        ])), True),
        
        StructField("allowRatings", BooleanType(), True),
        StructField("isPrivate", BooleanType(), True),
        StructField("isUnpluggedCorpus", BooleanType(), True),
        StructField("isLiveContent", BooleanType(), True),
        StructField("isCrawlable", BooleanType(), True),
        StructField("isFamilySafe", BooleanType(), True),
        StructField("isUnlisted", BooleanType(), True),
        StructField("isShortsEligible", BooleanType(), True),
        
        StructField("viewCount", StringType(), True),
        StructField("likeCount", StringType(), True),
        
        StructField("availableCountries", ArrayType(StringType()), True),
        
        StructField("category", StringType(), True),
        
        StructField("publishDate", StringType(), True),
        StructField("publishedAt", StringType(), True),
        StructField("uploadDate", StringType(), True),
        
        StructField("hasCaption", BooleanType(), True),
        StructField("subtitles", ArrayType(StructType([
            StructField("languageName", StringType(), True),
            StructField("languageCode", StringType(), True),
            StructField("isTranslatable", BooleanType(), True),
            StructField("url", StringType(), True),
        ])), True),
        
    ]), True)
])


def parse_duration_to_seconds(duration_text):
    """Konvertuje duration text u sekunde."""
    if not duration_text:
        return 0
    
    parts = duration_text.split(':')
    if len(parts) == 2:  
        return int(parts[0]) * 60 + int(parts[1])
    elif len(parts) == 3:  
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    return 0

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


# UPIT 2. Koje su performanse YouTube kategorija (trenutni broj videa, pregledi i engagement po kategoriji) u poslednjih 5 minuta, 
#         kako se one upoređuju sa istorijskim vrednostima, uključujući kategorije koje rastu ili opadaju i lista top dobitnici i gubitnici
#         (najveće promene u pregledima i engagementu)?


def load_historical_categories(spark):
    return spark.read.jdbc(
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


def load_historical_engagement(spark):
    return spark.read.jdbc(
        pg_url,
        "query2_channel_engagement",
        properties=pg_properties
    ).groupBy("category_title") \
        .agg(
            F.sum("engagement_score").alias("historical_total_engagement"),
            F.avg("avg_engagement_per_video").alias("historical_avg_engagement_per_video"),
            F.approx_count_distinct("channel_title").alias("historical_unique_channels")
        )

def calculate_current_category_performance(video_details_basic):
    return video_details_basic \
        .withWatermark("details_timestamp", "10 seconds") \
        .groupBy(
            F.window(F.col("details_timestamp"), "5 minutes", "2 minutes"),
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


def start_debug_stream(category_performance):
    def debug_categories(df, epoch_id):
        df.select("category").distinct().show(truncate=False)

    return category_performance.writeStream \
        .outputMode("update") \
        .trigger(processingTime='60 seconds') \
        .foreachBatch(debug_categories) \
        .option("checkpointLocation", "hdfs://namenode:9000/storage/hdfs/checkpoint/query2") \
        .start()

def enrich_with_historical(category_performance, historical_categories, historical_engagement_by_cat):
    return category_performance.alias("current") \
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

def start_category_query(enriched_categories):
    return (
        enriched_categories.writeStream
        .outputMode("update")
        .trigger(processingTime="25 seconds")
        .foreachBatch(lambda df, epoch_id: (
            # Priprema prvog view-a
            lambda top_trending, perf_trending: (
                # Ispis na konzolu
                top_trending.show(15, truncate=False),
                perf_trending.show(8, truncate=False),

                # Čuvanje rezultata na HDFS
                top_trending.write
                    .mode("append")
                    .parquet(f"hdfs://namenode:9000/storage/hdfs/results/query2/category_trending/stream_{epoch_id}"),

                perf_trending.write
                    .mode("append")
                    .parquet(f"hdfs://namenode:9000/storage/hdfs/results/query2/performance_vs_historical/stream_{epoch_id}")
            )
        )(
            # top_trending DF
            df.orderBy(F.desc("current_total_views"))
              .select(
                  "window.start",
                  "category",
                  "current_videos_count", 
                  F.coalesce(F.round("historical_total_videos", 0), F.lit(0)).alias("hist_videos"),
                  "current_unique_channels",
                  F.coalesce("historical_unique_channels", F.lit(0)).alias("hist_channels"),
                  F.round("current_total_views", 0).alias("current_views"),
                  F.round("current_avg_views", 0).alias("current_avg"),
                  F.coalesce(F.round("historical_avg_views", 0), F.lit(0)).alias("hist_avg"),
                  F.coalesce("views_performance_vs_historical", F.lit(0)).alias("views_change_%"),
                  F.round("current_engagement_rate", 1).alias("curr_eng_%"),
                  F.coalesce(F.round("historical_avg_engagement_per_video", 1), F.lit(0)).alias("hist_eng"),
                  F.coalesce("engagement_performance_vs_historical", F.lit(0)).alias("eng_change_%"),
                  "category_trend_indicator",
                  "market_position"
              ),

            # perf_trending DF
            df.filter(F.col("views_performance_vs_historical").isNotNull())
              .select(
                  "category",
                  "views_performance_vs_historical",
                  "engagement_performance_vs_historical", 
                  "current_total_views",
                  "category_trend_indicator"
              )
              .orderBy(F.desc("views_performance_vs_historical"))
        ))
        .start()
    )


def create_category_summary_stream(video_details_basic, spark):
    historical_categories = load_historical_categories(spark)
    historical_engagement_by_cat = load_historical_engagement(spark)

    category_performance = calculate_current_category_performance(video_details_basic)

    enriched_categories = enrich_with_historical(category_performance, historical_categories, historical_engagement_by_cat)

    category_query = start_category_query(enriched_categories)
    return category_query



def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    print(f"Kafka brokers: {KAFKA_BROKERS}")

    video_details_stream = create_kafka_stream(spark, KAFKA_TOPICS["video_details"], video_details_schema)

    video_details_basic = video_details_stream.select(
    F.col("timestamp").alias("kafka_timestamp"),
    F.col("source"),
    F.col("data").alias("video"),
    F.to_timestamp(F.col("processing_time")).alias("details_timestamp")
    ).select(
        
        F.col("kafka_timestamp"),
        F.col("source"),
        F.col("details_timestamp"),
        
        F.col("video.id").alias("video_id"),
        F.col("video.title").alias("title"),
        F.col("video.lengthSeconds").cast("int").alias("length_seconds"), 
        
        F.col("video.channelTitle").alias("channel_title"),
        F.col("video.channelId").alias("channel_id"),
        
        F.col("video.viewCount").cast("long").alias("view_count"),
        F.col("video.likeCount").cast("long").alias("like_count"),
        
        F.col("video.category").alias("category"),
        F.col("video.hasCaption").alias("has_caption"),
        F.col("video.isPrivate").alias("is_private"),
        F.col("video.isLiveContent").alias("is_live_content"),
        F.col("video.isShortsEligible").alias("is_shorts_eligible"),
        F.col("video.isFamilySafe").alias("is_family_safe"),
        
        F.to_timestamp(F.col("video.publishDate")).alias("publish_date"),
        F.to_timestamp(F.col("video.uploadDate")).alias("upload_date"),
        
        F.col("video.availableCountries").alias("available_countries"),
        F.size(F.col("video.availableCountries")).alias("countries_count"),
        
        F.col("video.thumbnail").alias("thumbnails"),
        F.size(F.col("video.thumbnail")).alias("thumbnail_count"),
        
        F.length(F.col("video.description")).alias("description_length"),
        
        F.col("video.subtitles").alias("subtitles"),
        F.size(F.col("video.subtitles")).alias("subtitles_count")
    )

    channel_perf_query = create_category_summary_stream(video_details_basic, spark)

    spark.streams.awaitAnyTermination()




if __name__ == "__main__":
    main()