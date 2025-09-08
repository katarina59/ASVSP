import os
from pyspark.sql import SparkSession # type: ignore
import pyspark.sql.functions as F # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType, DoubleType # type: ignore
import datetime 


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")

KAFKA_TOPICS = {
    "trending": "youtube_trending"
}

def create_spark_session():
    return SparkSession.builder \
        .appName("YouTube-Content-Intelligence-Analytics") \
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

def load_content_intelligence_dataset(spark):
    """Učitava golden dataset i kreira Content Intelligence metrics."""
    try:
        golden_df = spark.read.format("parquet").load("hdfs://namenode:9000/storage/hdfs/processed/golden_dataset")
        
        title_intelligence = golden_df.withColumn(
            "title_length", F.length(F.col("video_title"))
        ).withColumn(
            "title_word_count", F.size(F.split(F.col("video_title"), " "))
        ).withColumn(
            "has_numbers_in_title", F.col("video_title").rlike("[0-9]+")
        ).withColumn(
            "has_caps_lock_words", F.col("video_title").rlike("[A-Z]{3,}")
        ).withColumn(
            "has_question_mark", F.col("video_title").contains("?")
        ).withColumn(
            "has_exclamation", F.col("video_title").contains("!")
        ).groupBy("category_title", "region").agg(
            F.corr("title_length", "views").alias("title_length_views_correlation"),
            F.corr("title_word_count", "views").alias("word_count_views_correlation"),
            
            (F.sum(F.when(F.col("has_numbers_in_title"), F.col("views")).otherwise(0)) / 
             F.sum(F.when(F.col("has_numbers_in_title"), 1).otherwise(0))).alias("avg_views_with_numbers"),
            (F.sum(F.when(~F.col("has_numbers_in_title"), F.col("views")).otherwise(0)) / 
             F.sum(F.when(~F.col("has_numbers_in_title"), 1).otherwise(0))).alias("avg_views_without_numbers"),
            
            (F.sum(F.when(F.col("has_caps_lock_words"), F.col("views")).otherwise(0)) / 
             F.sum(F.when(F.col("has_caps_lock_words"), 1).otherwise(0))).alias("avg_views_with_caps"),
            
            (F.sum(F.when(F.col("has_question_mark"), F.col("views")).otherwise(0)) / 
             F.sum(F.when(F.col("has_question_mark"), 1).otherwise(0))).alias("avg_views_with_question"),
            
            F.avg(F.when(F.col("views") >= 1000000, F.col("title_length"))).alias("optimal_title_length_1M_plus"),
            F.avg(F.when(F.col("views") >= 1000000, F.col("title_word_count"))).alias("optimal_word_count_1M_plus"),
            
            F.avg("views").alias("category_avg_views"),
            F.percentile_approx("views", 0.75).alias("category_75th_percentile")
        )
        
        temporal_intelligence = golden_df.withColumn(
            "publish_hour", F.hour(F.to_timestamp(F.col("publish_date")))
        ).withColumn(
            "publish_day_of_week", F.dayofweek(F.to_timestamp(F.col("publish_date")))
        ).withColumn(
            "trending_delay_days", F.datediff(
                F.to_date(F.col("trending_full_date")), 
                F.to_date(F.col("publish_date"))
            )
        ).groupBy("category_title", "region").agg(
            F.avg(F.when(F.col("views") >= 1000000, F.col("publish_hour"))).alias("optimal_publish_hour"),
            F.mode(F.when(F.col("views") >= 1000000, F.col("publish_day_of_week"))).alias("optimal_publish_day"),
            
            F.avg("trending_delay_days").alias("avg_days_to_trend"),
            F.percentile_approx("trending_delay_days", 0.25).alias("fast_trend_threshold_days"),
            
            (F.avg(F.when(F.col("publish_day_of_week").isin([1, 7]), F.col("views"))) /
             F.avg(F.when(~F.col("publish_day_of_week").isin([1, 7]), F.col("views")))).alias("weekend_vs_weekday_ratio")
        )
        
        return title_intelligence, temporal_intelligence
        
    except Exception as e:
        empty_title = spark.createDataFrame([], StructType([
            StructField("category_title", StringType(), True),
            StructField("region", StringType(), True),
            StructField("title_length_views_correlation", DoubleType(), True),
            StructField("word_count_views_correlation", DoubleType(), True),
            StructField("avg_views_with_numbers", DoubleType(), True),
            StructField("avg_views_without_numbers", DoubleType(), True),
            StructField("avg_views_with_caps", DoubleType(), True),
            StructField("avg_views_with_question", DoubleType(), True),
            StructField("optimal_title_length_1M_plus", DoubleType(), True),
            StructField("optimal_word_count_1M_plus", DoubleType(), True),
            StructField("category_avg_views", DoubleType(), True),
            StructField("category_75th_percentile", DoubleType(), True)
        ]))
        
        empty_temporal = spark.createDataFrame([], StructType([
            StructField("category_title", StringType(), True),
            StructField("region", StringType(), True),
            StructField("optimal_publish_hour", DoubleType(), True),
            StructField("optimal_publish_day", IntegerType(), True),
            StructField("avg_days_to_trend", DoubleType(), True),
            StructField("fast_trend_threshold_days", DoubleType(), True),
            StructField("weekend_vs_weekday_ratio", DoubleType(), True)
        ]))
        
        empty_duration = spark.createDataFrame([], StructType([
            StructField("category_title", StringType(), True),
            StructField("region", StringType(), True),
            StructField("duration_category", StringType(), True),
            StructField("avg_views_by_duration", DoubleType(), True),
            StructField("avg_likes_by_duration", DoubleType(), True),
            StructField("avg_comments_by_duration", DoubleType(), True),
            StructField("video_count_by_duration", LongType(), True),
            StructField("duration_effectiveness_score", DoubleType(), True)
        ]))
        
        return empty_title, empty_temporal, empty_duration

def prepare_streaming_content_features(trending_stream):
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
        F.col("data.publishedTimeText").alias("published_time_text"),
        F.to_timestamp(F.col("processing_time")).alias("stream_timestamp")
    ).withColumn(
        "view_count_parsed", 
        F.regexp_replace(F.col("view_count_raw"), "[^0-9]", "").cast("long")
    ).withColumn(
        # Title feature extraction
        "title_length", F.length(F.col("title"))
    ).withColumn(
        "title_word_count", F.size(F.split(F.col("title"), " "))
    ).withColumn(
        "has_numbers_in_title", F.col("title").rlike("[0-9]+")
    ).withColumn(
        "has_caps_lock_words", F.col("title").rlike("[A-Z]{3,}")
    ).withColumn(
        "has_question_mark", F.col("title").contains("?")
    ).withColumn(
        "has_exclamation", F.col("title").contains("!")
    ).withColumn(
        "has_viral_keywords", 
        F.lower(F.col("title")).rlike("(viral|shocking|amazing|incredible|unbelievable|insane|crazy|epic)")
    ).withColumn(
        "has_urgency_words",
        F.lower(F.col("title")).rlike("(now|today|breaking|urgent|live|happening)")
    ).withColumn(
        "emotional_appeal_score",
        F.when(F.col("has_question_mark"), 1).otherwise(0) +
        F.when(F.col("has_exclamation"), 1).otherwise(0) +
        F.when(F.col("has_caps_lock_words"), 1).otherwise(0) +
        F.when(F.col("has_viral_keywords"), 2).otherwise(0) +
        F.when(F.col("has_urgency_words"), 1).otherwise(0)
    ).withColumn(
        "title_optimization_score",
        F.when(F.col("title_length").between(30, 60), 2)  
         .when(F.col("title_length").between(20, 80), 1)
         .otherwise(0) +
        F.when(F.col("title_word_count").between(5, 10), 1).otherwise(0)
    ).filter(
        F.col("view_count_parsed").isNotNull() & (F.col("view_count_parsed") > 0)
    )

# UPIT 4: Koje YouTube strategije u pisanju naslova trenutno doprinose većoj gledanosti i viralnom potencijalu, 
#         i koji kanali ih najuspešnije koriste u realnom vremenu?
def create_content_intelligence_analysis(streaming_data, title_intelligence, temporal_intelligence):
    """Kreira Content Intelligence analizu kombinujući stream i batch podatke."""
    
    def content_intelligence_processor(streaming_df, epoch_id):
        try:
            count = streaming_df.count()
            if count == 0:
                print(f" Epoch {epoch_id}: Čekam streaming podatke...")
                return
                
            
            
            title_performance = streaming_df.groupBy("channel_title").agg(
                F.count("*").alias("video_count"),
                F.avg("view_count_parsed").alias("avg_views"),
                F.avg("title_length").alias("avg_title_length"),
                F.avg("title_word_count").alias("avg_word_count"),
                F.avg("emotional_appeal_score").alias("avg_emotional_score"),
                F.avg("title_optimization_score").alias("avg_optimization_score"),
                
                (F.sum(F.when(F.col("has_numbers_in_title"), F.col("view_count_parsed"))) /
                 F.sum(F.when(F.col("has_numbers_in_title"), 1))).alias("numbers_avg_views"),
                (F.sum(F.when(F.col("has_viral_keywords"), F.col("view_count_parsed"))) /
                 F.sum(F.when(F.col("has_viral_keywords"), 1))).alias("viral_keywords_avg_views"),
                (F.sum(F.when(F.col("has_question_mark"), F.col("view_count_parsed"))) /
                 F.sum(F.when(F.col("has_question_mark"), 1))).alias("question_avg_views")
            ).withColumn(
                "title_strategy_grade",
                F.when(F.col("avg_optimization_score") >= 2.5, " Excellent")
                 .when(F.col("avg_optimization_score") >= 2.0, " Good")
                 .when(F.col("avg_optimization_score") >= 1.5, " Average")
                 .otherwise(" Needs Optimization")
            ).withColumn(
                "viral_potential_score",
                (F.col("avg_emotional_score") * 0.4 + 
                 F.col("avg_optimization_score") * 0.6) * 
                (F.log10(F.col("avg_views") + 1) / 10)
            )
            
            title_performance.orderBy(F.desc("viral_potential_score")) \
                .select(
                    "channel_title",
                    "video_count",
                    F.format_number("avg_views", 0).alias("avg_views"),
                    F.round("avg_title_length", 1).alias("title_len"),
                    F.round("avg_emotional_score", 1).alias("emotion"),
                    F.round("avg_optimization_score", 1).alias("optim"),
                    F.round("viral_potential_score", 2).alias("viral_pot"),
                    "title_strategy_grade"
                ).show(10, truncate=False)
            
        except Exception as e:
            print(f" Error in content_intelligence_processor epoch {epoch_id}: {e}")
    
    streaming_with_watermark = streaming_data.withWatermark("stream_timestamp", "10 minutes")
    
    query = streaming_with_watermark \
        .writeStream \
        .outputMode("update") \
        .trigger(processingTime='2 minutes') \
        .foreachBatch(content_intelligence_processor) \
        .option("checkpointLocation", "hdfs://namenode:9000/storage/hdfs/checkpoint/query4") \
        .start()
    
    return query

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f" Kafka brokers: {KAFKA_BROKERS}")
    
    
    title_intelligence, temporal_intelligence = load_content_intelligence_dataset(spark)
    
    print(" Connecting to Kafka streams...")
    trending_stream = create_kafka_stream(spark, KAFKA_TOPICS["trending"], trending_schema)
    print(" Kafka streams created!")
    
    streaming_prepared = prepare_streaming_content_features(trending_stream)
    
    intelligence_query = create_content_intelligence_analysis(
        streaming_prepared, 
        title_intelligence, 
        temporal_intelligence
    )
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
