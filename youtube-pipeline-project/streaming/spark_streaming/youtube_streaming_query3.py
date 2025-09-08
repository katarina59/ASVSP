import os
from pyspark.sql import SparkSession # type: ignore
import pyspark.sql.functions as F # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType, DoubleType # type: ignore
import datetime 


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")

# Kafka topics iz producer-a
KAFKA_TOPICS = {
    "trending": "youtube_trending"
}

def create_spark_session():
    return SparkSession.builder \
        .appName("YouTube-Stream-Batch-Analytics") \
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

def load_golden_dataset_analytics(spark):
    """Učitava golden dataset i pravi analytical views."""
    try:
        golden_df = spark.read.format("parquet").load("hdfs://namenode:9000/storage/hdfs/processed/golden_dataset")
        
        channel_analytics = golden_df.groupBy("channel_title", "category_title", "region").agg(
            F.count("*").alias("total_historical_videos"),
            F.avg("views").alias("avg_historical_views"),
            F.max("views").alias("best_video_views"),
            F.avg("likes").alias("avg_historical_likes"),
            F.avg("comment_count").alias("avg_historical_comments"),
            F.stddev("views").alias("view_consistency"),
            F.countDistinct("trending_full_date").alias("trending_days"),
            
            (F.avg("likes") / F.avg("views") * 100).alias("avg_like_rate"),
            (F.avg("comment_count") / F.avg("views") * 100).alias("avg_comment_rate"),
            
            F.when(F.avg("views") >= 10000000, "Mega Channel")
             .when(F.avg("views") >= 1000000, "Popular Channel")
             .when(F.avg("views") >= 100000, "Growing Channel")
             .otherwise("Small Channel").alias("channel_tier")
        )
        
        category_benchmarks = golden_df.groupBy("category_title", "region").agg(
            F.avg("views").alias("category_avg_views"),
            F.percentile_approx("views", 0.9).alias("category_top10_threshold"),
            F.avg("likes").alias("category_avg_likes"),
            F.count("*").alias("category_total_videos")
        )
        
        regional_trends = golden_df.groupBy("region", "trending_month").agg(
            F.avg("views").alias("regional_monthly_avg_views"),
            F.count("*").alias("monthly_trending_count"),
            F.countDistinct("channel_title").alias("unique_channels_monthly")
        )
        
        return channel_analytics, category_benchmarks, regional_trends
        
    except Exception as e:
        empty_channel = spark.createDataFrame([], StructType([
            StructField("channel_title", StringType(), True),
            StructField("category_title", StringType(), True),
            StructField("region", StringType(), True),
            StructField("total_historical_videos", LongType(), True),
            StructField("avg_historical_views", DoubleType(), True),
            StructField("best_video_views", LongType(), True),
            StructField("avg_historical_likes", DoubleType(), True),
            StructField("avg_historical_comments", DoubleType(), True),
            StructField("view_consistency", DoubleType(), True),
            StructField("trending_days", LongType(), True),
            StructField("avg_like_rate", DoubleType(), True),
            StructField("avg_comment_rate", DoubleType(), True),
            StructField("channel_tier", StringType(), True)
        ]))
        
        empty_category = spark.createDataFrame([], StructType([
            StructField("category_title", StringType(), True),
            StructField("region", StringType(), True),
            StructField("category_avg_views", DoubleType(), True),
            StructField("category_top10_threshold", DoubleType(), True),
            StructField("category_avg_likes", DoubleType(), True),
            StructField("category_total_videos", LongType(), True)
        ]))
        
        empty_regional = spark.createDataFrame([], StructType([
            StructField("region", StringType(), True),
            StructField("trending_month", StringType(), True),
            StructField("regional_monthly_avg_views", DoubleType(), True),
            StructField("monthly_trending_count", LongType(), True),
            StructField("unique_channels_monthly", LongType(), True)
        ]))
        
        return empty_channel, empty_category, empty_regional

def prepare_streaming_data(trending_stream):
    return trending_stream.select(
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("source"),
        F.col("data.type").alias("content_type"),
        F.col("data.videoId").alias("video_id"),
        F.col("data.title").alias("title"),
        F.col("data.channelId").alias("channel_id"),
        F.col("data.channelTitle").alias("channel_title"),
        F.col("data.viewCount").alias("view_count_raw"),
        F.col("data.lengthText").alias("length_text"),
        F.col("data.description").alias("description"),
        F.to_timestamp(F.col("processing_time")).alias("stream_timestamp")
    ).withColumn(
        "view_count_parsed", 
        F.regexp_replace(F.col("view_count_raw"), "[^0-9]", "").cast("long")
    ).withColumn(
        "description_length", F.length(F.col("description"))
    ).withColumn(
        "current_popularity_tier",
        F.when(F.col("view_count_parsed") >= 10000000, "Mega Hit (10M+)")
         .when(F.col("view_count_parsed") >= 1000000, "Viral (1M+)")
         .when(F.col("view_count_parsed") >= 100000, "Popular (100K+)")
         .when(F.col("view_count_parsed") >= 10000, "Rising (10K+)")
         .otherwise("New/Small")
    ).filter(
        F.col("view_count_parsed").isNotNull() & (F.col("view_count_parsed") > 0)
    )


# UPIT 3: Koji YouTube kanali trenutno beleže anomalije u performansama u odnosu na svoj istorijski učinak, 
#         koje kategorije trenutno nadmašuju ili zaostaju u odnosu na istorijske kategorijske proseke, 
#         i da li postoje kanali koji u realnom vremenu pokazuju viralni rast u poređenju sa svojim istorijskim performansama?

def create_enriched_stream_batch_analysis(streaming_data, channel_analytics, category_benchmarks, regional_trends):
   
    def enrichment_processor(streaming_df, epoch_id):
        try:
            count = streaming_df.count()
            if count == 0:
                print(f"Epoch {epoch_id}: Čekam streaming podatke...")
                return
                
            
            streaming_aggregated = streaming_df.groupBy("channel_title").agg(
                F.count("*").alias("current_trending_videos"),
                F.avg("view_count_parsed").alias("current_avg_views"),
                F.sum("view_count_parsed").alias("current_total_views"),
                F.max("view_count_parsed").alias("current_best_video"),
                F.first("current_popularity_tier").alias("top_tier"),
                F.avg("description_length").alias("avg_desc_length")
            )
            
            join_test = streaming_aggregated.join(F.broadcast(channel_analytics), ["channel_title"], "inner")
            matches = join_test.count()
            total_streaming = streaming_aggregated.count()
            print(f"    JOIN SUCCESS: {matches}/{total_streaming} streaming channels matched with batch data")
            
            enriched_with_history = streaming_aggregated.join(
                F.broadcast(channel_analytics), 
                ["channel_title"], 
                "left"
            ).withColumn(
                "performance_vs_history",
                F.when(F.col("avg_historical_views").isNull(), "New Channel")
                 .when(F.col("current_avg_views") > F.col("avg_historical_views") * 2, "Breakout Performance")
                 .when(F.col("current_avg_views") > F.col("avg_historical_views") * 1.5, "Above Average") 
                 .when(F.col("current_avg_views") > F.col("avg_historical_views") * 0.8, "Normal Range")
                 .otherwise("Below Average")
            ).withColumn(
                "viral_anomaly_score",
                F.when(F.col("avg_historical_views").isNull(), 0)
                 .otherwise(F.col("current_avg_views") / F.col("avg_historical_views"))
            ).withColumn(
                "growth_indicator",
                F.when(F.col("viral_anomaly_score") >= 3.0, "VIRAL EXPLOSION")
                 .when(F.col("viral_anomaly_score") >= 2.0, "Strong Growth") 
                 .when(F.col("viral_anomaly_score") >= 1.5, "Moderate Growth")
                 .when(F.col("viral_anomaly_score") >= 0.8, "Stable")
                 .otherwise("Declining")
            )
            
            top_performers = enriched_with_history.filter(F.col("current_trending_videos") >= 1) \
                .orderBy(F.desc("current_total_views")) \
                .select(
                    "channel_title",
                    "current_trending_videos",
                    F.format_number("current_avg_views", 0).alias("current_views"),
                    F.format_number("avg_historical_views", 0).alias("historical_avg"),
                    F.round("viral_anomaly_score", 2).alias("anomaly_x"),
                    "performance_vs_history",
                    "growth_indicator",
                    "channel_tier"
                )
            top_performers.show(12, truncate=False)
            
            viral_channels = enriched_with_history.filter(F.col("viral_anomaly_score") >= 1.5) \
                .orderBy(F.desc("viral_anomaly_score")) \
                .select(
                    "channel_title",
                    "current_trending_videos", 
                    F.format_number("current_avg_views", 0).alias("current_views"),
                    F.format_number("avg_historical_views", 0).alias("hist_avg_views"),
                    F.round("viral_anomaly_score", 1).alias("viral_multiplier"),
                    "growth_indicator",
                    F.coalesce("channel_tier", F.lit("New Channel")).alias("tier")
                )
            
            viral_count = viral_channels.count()
            if viral_count > 0:
                viral_channels.show(10, truncate=False)
                print(f"   Detected {viral_count} channels with significant growth anomalies!")
            else:
                print("   No major viral anomalies detected in this window.")
            
            streaming_by_category = streaming_df.groupBy("channel_title").agg(
                F.first("current_popularity_tier").alias("tier"),
                F.avg("view_count_parsed").alias("stream_avg_views")
            ).join(
                F.broadcast(channel_analytics.select("channel_title", "category_title")),
                ["channel_title"], "left"
            ).filter(F.col("category_title").isNotNull()) \
             .groupBy("category_title").agg(
                F.count("*").alias("streaming_channels"),
                F.avg("stream_avg_views").alias("category_stream_avg")
            )
            
            category_comparison = streaming_by_category.join(
                F.broadcast(category_benchmarks),
                ["category_title"], "left"
            ).withColumn(
                "category_performance_vs_batch",
                F.when(F.col("category_avg_views").isNull(), "No Historical Data")
                 .when(F.col("category_stream_avg") > F.col("category_avg_views") * 1.3, "Outperforming")
                 .when(F.col("category_stream_avg") > F.col("category_avg_views") * 0.8, "Normal")
                 .otherwise("Underperforming")
            )
            
            print("\n CATEGORY PERFORMANCE (Stream vs Batch Benchmarks):")
            category_comparison.select(
                "category_title",
                "streaming_channels",
                F.format_number("category_stream_avg", 0).alias("stream_avg"),
                F.format_number("category_avg_views", 0).alias("batch_avg"),
                "category_performance_vs_batch"
            ).orderBy(F.desc("streaming_channels")).show(8, truncate=False)
            
            print("="*150)
            
        except Exception as e:
            print(f" Error in enrichment_processor epoch {epoch_id}: {e}")
    
    streaming_with_watermark = streaming_data.withWatermark("stream_timestamp", "10 minutes")
    
    query = streaming_with_watermark \
        .writeStream \
        .outputMode("update") \
        .trigger(processingTime='2 minutes') \
        .foreachBatch(enrichment_processor) \
        .option("checkpointLocation", "hdfs://namenode:9000/storage/hdfs/checkpoint/query3") \
        .start()
    
    return query

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    
    channel_analytics, category_benchmarks, regional_trends = load_golden_dataset_analytics(spark)
    
    print(" Connecting to Kafka streams...")
    trending_stream = create_kafka_stream(spark, KAFKA_TOPICS["trending"], trending_schema)
    print(" Kafka streams created!")
    
    streaming_prepared = prepare_streaming_data(trending_stream)
    
    enriched_query = create_enriched_stream_batch_analysis(
        streaming_prepared, 
        channel_analytics, 
        category_benchmarks, 
        regional_trends
    )
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()