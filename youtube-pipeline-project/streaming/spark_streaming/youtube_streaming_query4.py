import os
from pyspark.sql import SparkSession, Window # type: ignore
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
    try:
        golden_df = spark.read.format("parquet").load("hdfs://namenode:9000/storage/hdfs/processed/golden_dataset")
        
        # Channel Intelligence Analysis
        channel_intelligence = golden_df.groupBy("category_title", "region", "channel_title").agg(
            F.count("*").alias("videos_count"),
            F.avg("views").alias("channel_avg_views"),
            F.sum("views").alias("channel_total_views"),
            F.max("views").alias("channel_best_video_views"),
            F.percentile_approx("views", 0.75).alias("channel_75th_percentile_views"),
            F.avg("likes").alias("channel_avg_likes"),
            F.avg("comment_count").alias("channel_avg_comments"),
            F.avg(F.col("likes") / F.col("views")).alias("channel_avg_like_rate"),
            F.avg(F.col("comment_count") / F.col("views")).alias("channel_avg_comment_rate"),
            F.countDistinct("trending_full_date").alias("trending_days_count")
        )

        return channel_intelligence

    except Exception as e:
        print(f" Error loading content intelligence dataset: {str(e)}")
        return None, None



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

# UPIT 4: Koji YouTube kanali u poslednja 2 minuta beleže najbolje performanse svojih naslova u odnosu na istorijske proseke, 
#         koliko se njihove trenutne prosečne gledanosti razlikuju od istorijskih vrednosti po kategoriji, 
#         i kakav je njihov viralni potencijal i efektivnost strategije pisanja naslova?


def create_content_intelligence_analysis(streaming_data, channel_intelligence):
    
    def content_intelligence_processor(streaming_df, epoch_id):
        try:
            count = streaming_df.count()
            if count == 0:
                print(f"Epoch {epoch_id}: Čekam streaming podatke...")
                return
            
            # Proveravamo da li su batch podaci dostupni
            if channel_intelligence is None:
                
                # Samo real-time analiza bez poređenja
                title_performance = streaming_df.groupBy(
                    F.window("stream_timestamp", "2 minutes"),
                    F.col("channel_title")).agg(
                    F.count("*").alias("video_count"),
                    F.avg("view_count_parsed").alias("avg_views"),
                    F.avg("title_length").alias("avg_title_length"),
                    F.avg("title_word_count").alias("avg_word_count"),
                    F.avg("emotional_appeal_score").alias("avg_emotional_score"),
                    F.avg("title_optimization_score").alias("avg_optimization_score")
                ).withColumn(
                    "title_strategy_grade",
                    F.when(F.col("avg_optimization_score") >= 2.5, "Excellent")
                     .when(F.col("avg_optimization_score") >= 2.0, "Good")
                     .when(F.col("avg_optimization_score") >= 1.5, "Average")
                     .otherwise("Needs Optimization")
                ).withColumn(
                    "viral_potential_score",
                    (F.col("avg_emotional_score") * 0.4 + 
                     F.col("avg_optimization_score") * 0.6) * 
                    (F.log10(F.col("avg_views") + 1) / 10)
                )
                
                print(f"\n REAL-TIME CONTENT ANALYSIS")
                title_performance.orderBy(F.desc("viral_potential_score")).show(10, truncate=False)
                return
                
            # Real-time analiza naslova
            title_performance = streaming_df.groupBy(
                F.window("stream_timestamp", "2 minutes"),
                F.col("channel_title")).agg(
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
                F.when(F.col("avg_optimization_score") >= 2.5, "Excellent")
                 .when(F.col("avg_optimization_score") >= 2.0, "Good")
                 .when(F.col("avg_optimization_score") >= 1.5, "Average")
                 .otherwise("Needs Optimization")
            ).withColumn(
                "viral_potential_score",
                (F.col("avg_emotional_score") * 0.4 + 
                 F.col("avg_optimization_score") * 0.6) * 
                (F.log10(F.col("avg_views") + 1) / 10)
            )
            
            # Priprema batch podataka za poređenje (samo relevantni kanali)
            batch_channels = channel_intelligence.select(
                F.col("channel_title"),
                F.col("channel_avg_views").alias("batch_avg_views"),
                F.col("videos_count").alias("batch_video_count"),
                F.col("channel_avg_like_rate").alias("batch_like_rate"),
                F.col("category_title").alias("batch_category")
            )
            
            # Kombinovanje real-time i batch podataka
            combined_analysis = title_performance.join(
                batch_channels, 
                on="channel_title", 
                how="left"
            ).withColumn(
                "performance_trend",
                F.when(F.col("avg_views") > F.col("batch_avg_views") * 1.2, "Above Historical")
                 .when(F.col("avg_views") > F.col("batch_avg_views") * 0.8, "Consistent")
                 .otherwise("Below Historical")
            ).withColumn(
                "strategy_effectiveness",
                F.when(
                    (F.col("viral_potential_score") > 1.0) & 
                    (F.col("avg_views") > F.col("batch_avg_views")), "Highly Effective"
                ).when(
                    F.col("viral_potential_score") > 0.7, "Effective"
                ).when(
                    F.col("viral_potential_score") > 0.4, "Moderate"
                ).otherwise("Ineffective")
            ).withColumn(
                "improvement_vs_batch",
                F.round(
                    F.when(F.col("batch_avg_views").isNotNull() & (F.col("batch_avg_views") > 0),
                        ((F.col("avg_views") - F.col("batch_avg_views")) / F.col("batch_avg_views")) * 100
                    ).otherwise(0), 1
                )
            )
            
            
            combined_analysis.orderBy(F.desc("viral_potential_score")) \
                .select(
                    F.col("channel_title").alias("Channel"),
                    F.col("batch_category").alias("Category"),
                    F.format_number("avg_views", 0).alias("Current_Views"),
                    F.format_number("batch_avg_views", 0).alias("Historical_Avg"),
                    F.col("improvement_vs_batch").alias("Change_%"),
                    F.col("performance_trend").alias("Trend"),
                    F.round("viral_potential_score", 2).alias("Viral_Score"),
                    F.col("strategy_effectiveness").alias("Strategy"),
                    F.col("title_strategy_grade").alias("Title_Grade")
                ).show(15, truncate=False)
            
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
    spark.sparkContext.setLogLevel("ERROR")
    
    
    
    channel_intelligence = load_content_intelligence_dataset(spark)
    
    trending_stream = create_kafka_stream(spark, KAFKA_TOPICS["trending"], trending_schema)
    
    streaming_prepared = prepare_streaming_content_features(trending_stream)
    
    intelligence_query = create_content_intelligence_analysis(
        streaming_prepared, channel_intelligence
    )
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
