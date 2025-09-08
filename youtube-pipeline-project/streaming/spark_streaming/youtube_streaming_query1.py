import os
from pyspark.sql import SparkSession # type: ignore
import pyspark.sql.functions as F # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType, DoubleType # type: ignore
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
    "trending": "youtube_trending"
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



def parse_duration_to_seconds(duration_text):
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


# UPIT 1: Koji kanali trenutno dominiraju YouTube trending listom u poslednje 15 minuta, 
#         kada se uporede sa njihovim istorijskim performansama iz batch analize, i koji od njih predstavljaju neočekivane viral 
#         fenomene koji nisu bili u top performerima?


def load_batch_context_data(spark):
    
    try:
        regional_performance = spark.read.jdbc(
            pg_url, "query1_category_region_analysis", properties=pg_properties
        ).groupBy("category_title", "region").agg(
            F.avg("avg_views").alias("hist_avg_views"),
            F.avg("avg_comments").alias("hist_avg_comments"),
            F.avg("moving_avg_views_3d").alias("trend_3d")
        )
        
        top_channels = spark.read.jdbc(
            pg_url, "query2_channel_engagement", properties=pg_properties
        ).filter(F.col("rank_in_category") <= 5).select(
            "category_title", "channel_title",
            "engagement_score", "avg_engagement_per_video", "rank_in_category"
        )

        try:
            regional_baselines = spark.read.jdbc(
                pg_url, "bonus1_regional_executive_summary", properties=pg_properties
            ).select(
                "region", "avg_views_per_video", "avg_likes_per_video",
                "total_videos", "unique_channels"
            )
        except Exception as e:
            
            schema = StructType([
                StructField("region", StringType(), True),
                StructField("avg_views_per_video", DoubleType(), True),
                StructField("avg_likes_per_video", DoubleType(), True),
                StructField("total_videos", LongType(), True),
                StructField("unique_channels", LongType(), True)
            ])
            regional_baselines = spark.createDataFrame([], schema)
        
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



def create_intelligent_trending_analysis_v2(trending_prepared, regional_performance, top_channels, regional_baselines):
    
    category_baselines = regional_performance.groupBy("category_title").agg(
        F.avg("hist_avg_views").alias("category_avg_views"),
        F.avg("hist_avg_comments").alias("category_avg_comments"),
        F.avg("trend_3d").alias("category_trend")
    )
    
    regional_thresholds = regional_baselines.select(
        "region",
        F.col("avg_views_per_video").alias("regional_baseline_views"),
        F.col("avg_likes_per_video").alias("regional_baseline_likes")
    )
    
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
                F.col("avg_views_per_video") / 1000000  
            ).otherwise(0)
        )
        
        enriched = enriched.withColumn(
            "content_velocity",
            F.col("trending_videos_count") / F.greatest(F.col("avg_duration") / 60, F.lit(1)) 
        )
        
        enriched = enriched.withColumn(
            "engagement_consistency", 
            F.when(F.col("view_consistency").isNull(), 1.0)
            .otherwise(1.0 / (1.0 + F.col("view_consistency") / F.col("avg_views_per_video")))
        )
        
        enriched = enriched.withColumn(
            "intelligent_score_v2",
            (F.col("trending_videos_count") * F.log10(F.greatest(F.col("avg_views_per_video"), F.lit(1)))) * 
            
            (1.0 + F.col("viral_anomaly_score") * 0.5) *  
            (1.0 + F.col("content_velocity") * 0.3) *     
            F.col("engagement_consistency") *              
            
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
            
            
            print("="*140)
            
        else:
            print(f"Epoch {epoch_id}: Waiting for trending data...")
    
    query = enriched_trending.writeStream \
        .outputMode("update") \
        .trigger(processingTime='3 minutes') \
        .foreachBatch(advanced_processor) \
        .option("checkpointLocation", "hdfs://namenode:9000/storage/hdfs/checkpoint/query1") \
        .start()
    
    return query


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



    regional_performance, top_channels, regional_baselines = load_batch_context_data(spark)
        
    trending_prepared = prepare_trending_data_enhanced(trending_basic)
    
    enhanced_query = create_intelligent_trending_analysis_v2(
            trending_prepared, regional_performance, top_channels, regional_baselines
        )
    
    
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
