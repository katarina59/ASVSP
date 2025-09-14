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
        .appName("YouTube-Tag-Intelligence-Analytics") \
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

def load_tag_intelligence_dataset(spark):
    try:
        golden_df = spark.read.format("parquet").load("hdfs://namenode:9000/storage/hdfs/processed/golden_dataset")
        
        # Eksplodiranje tagova za analizu
        exploded_tags = golden_df.select("*", F.explode("tags_list").alias("tag")) \
            .filter(F.col("tag") != "no-tags") \
            .filter(F.col("tag") != "unknown")
        
        # Tag Intelligence Analysis - osnovne statistike po tagu
        tag_intelligence = exploded_tags.groupBy("tag", "category_title").agg(
            F.count("*").alias("tag_usage_count"),
            F.avg("views").alias("tag_avg_views"),
            F.avg("likes").alias("tag_avg_likes"),
            F.avg("comment_count").alias("tag_avg_comments"),
            F.avg(F.col("likes") / F.col("views")).alias("tag_avg_like_rate"),
            F.avg(F.col("comment_count") / F.col("views")).alias("tag_avg_comment_rate"),
            F.percentile_approx("views", 0.9).alias("tag_90th_percentile_views"),
            F.countDistinct("channel_title").alias("unique_channels_using_tag"),
            F.countDistinct("trending_full_date").alias("trending_days_with_tag")
        ).filter(F.col("tag_usage_count") >= 10)  # Samo tagovi koji se koriste dovoljno često
        
        # Kombinacije tagova - analiza parova tagova
        tag_combinations = golden_df.filter(F.size("tags_list") >= 2) \
            .withColumn("tag_pair", 
                F.expr("transform(sequence(0, size(tags_list) - 2), i -> " +
                       "struct(tags_list[i] as tag1, tags_list[i+1] as tag2))")
            ).select("*", F.explode("tag_pair").alias("pair")) \
            .select("video_id", "views", "likes", "comment_count", "category_title",
                   "pair.tag1", "pair.tag2") \
            .filter((F.col("tag1") != "no-tags") & (F.col("tag2") != "no-tags")) \
            .filter((F.col("tag1") != "unknown") & (F.col("tag2") != "unknown"))
        
        tag_pair_intelligence = tag_combinations.groupBy("tag1", "tag2", "category_title").agg(
            F.count("*").alias("pair_usage_count"),
            F.avg("views").alias("pair_avg_views"),
            F.avg("likes").alias("pair_avg_likes"),
            F.avg(F.col("likes") / F.col("views")).alias("pair_avg_like_rate")
        ).filter(F.col("pair_usage_count") >= 5)

        return tag_intelligence, tag_pair_intelligence

    except Exception as e:
        print(f" Error loading tag intelligence dataset: {str(e)}")
        return None, None

def prepare_streaming_tag_features(trending_stream):
    return trending_stream.select(
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("source"),
        F.col("data.type").alias("content_type"),
        F.col("data.videoId").alias("video_id"),
        F.col("data.title").alias("title"),
        F.col("data.channelId").alias("channel_id"),
        F.col("data.channelTitle").alias("channel_title"),
        F.col("data.viewCount").alias("view_count_raw"),
        F.col("data.description").alias("description"),
        F.to_timestamp(F.col("processing_time")).alias("stream_timestamp")
    ).withColumn(
        "view_count_parsed", 
        F.regexp_replace(F.col("view_count_raw"), "[^0-9]", "").cast("long")
    ).withColumn(
        # Ekstraktovanje tagova iz title-a i description-a (simulacija)
        "extracted_tags",
        F.array_distinct(F.flatten(F.array(
            # Gaming tagovi
            F.when(F.lower(F.col("title")).rlike("(game|gaming|play|player|gamer)"), F.array(F.lit("gaming"))).otherwise(F.array()),
            F.when(F.lower(F.col("title")).rlike("(minecraft|fortnite|valorant|csgo)"), F.array(F.lit("game-specific"))).otherwise(F.array()),
            
            # Music tagovi
            F.when(F.lower(F.col("title")).rlike("(music|song|audio|sound|beat)"), F.array(F.lit("music"))).otherwise(F.array()),
            F.when(F.lower(F.col("title")).rlike("(cover|remix|live|concert)"), F.array(F.lit("music-performance"))).otherwise(F.array()),
            
            # Entertainment tagovi
            F.when(F.lower(F.col("title")).rlike("(funny|comedy|laugh|humor)"), F.array(F.lit("comedy"))).otherwise(F.array()),
            F.when(F.lower(F.col("title")).rlike("(reaction|react|review)"), F.array(F.lit("reaction"))).otherwise(F.array()),
            
            # Educational tagovi
            F.when(F.lower(F.col("title")).rlike("(tutorial|how to|guide|learn)"), F.array(F.lit("educational"))).otherwise(F.array()),
            F.when(F.lower(F.col("title")).rlike("(science|tech|technology)"), F.array(F.lit("tech"))).otherwise(F.array()),
            
            # Lifestyle tagovi
            F.when(F.lower(F.col("title")).rlike("(vlog|daily|life|lifestyle)"), F.array(F.lit("lifestyle"))).otherwise(F.array()),
            F.when(F.lower(F.col("title")).rlike("(food|cooking|recipe)"), F.array(F.lit("food"))).otherwise(F.array()),
            
            # Trend tagovi
            F.when(F.lower(F.col("title")).rlike("(viral|trending|popular|hot)"), F.array(F.lit("viral"))).otherwise(F.array()),
            F.when(F.lower(F.col("title")).rlike("(challenge|trend|meme)"), F.array(F.lit("challenge"))).otherwise(F.array())
        )))
    ).withColumn(
        "tag_count", F.size(F.col("extracted_tags"))
    ).withColumn(
        "has_gaming_tag", F.array_contains(F.col("extracted_tags"), "gaming")
    ).withColumn(
        "has_music_tag", F.array_contains(F.col("extracted_tags"), "music")
    ).withColumn(
        "has_educational_tag", F.array_contains(F.col("extracted_tags"), "educational")
    ).withColumn(
        "has_viral_tag", F.array_contains(F.col("extracted_tags"), "viral")
    ).withColumn(
        "tag_diversity_score",
        F.when(F.col("tag_count") == 0, 0)
         .when(F.col("tag_count") == 1, 1)
         .when(F.col("tag_count") == 2, 2.5)
         .when(F.col("tag_count") >= 3, 4)
         .otherwise(0)
    ).filter(
        F.col("view_count_parsed").isNotNull() & (F.col("view_count_parsed") > 0)
    )

# UPIT 5: Koje kombinacije tagova u poslednja 3 minuta pokazuju najbolje performanse u odnosu na istorijske proseke,
#         i koji tagovi imaju najveći viralni potencijal?

def create_tag_intelligence_analysis(streaming_data, tag_intelligence, tag_pair_intelligence):
    
    def tag_intelligence_processor(streaming_df, epoch_id):
        try:
            count = streaming_df.count()
            if count == 0:
                print(f"Epoch {epoch_id}: Čekam streaming podatke...")
                return
            
            # Eksplodiranje tagova za real-time analizu
            exploded_streaming = streaming_df.select("*", F.explode("extracted_tags").alias("current_tag")) \
                .filter(F.col("current_tag").isNotNull())
            
            if exploded_streaming.count() == 0:
                print("Nema detektovanih tagova u trenutnom batch-u")
                return
            
            # Real-time tag performanse
            streaming_tag_performance = exploded_streaming.groupBy(
                F.window("stream_timestamp", "3 minutes"),
                F.col("current_tag")
            ).agg(
                F.count("*").alias("current_usage"),
                F.avg("view_count_parsed").alias("current_avg_views"),
                F.sum("view_count_parsed").alias("current_total_views"),
                F.countDistinct("channel_title").alias("channels_using_tag")
            ).withColumn(
                "tag_momentum_score",
                (F.log10(F.col("current_avg_views") + 1) * 
                 F.sqrt(F.col("current_usage")) * 
                 F.col("channels_using_tag")) / 100
            )
            
            if tag_intelligence is not None:
                historical_comparison = streaming_tag_performance.join(
                    tag_intelligence.groupBy("tag").agg(
                        F.avg("tag_avg_views").alias("historical_avg_views"),
                        F.avg("tag_avg_like_rate").alias("historical_like_rate"),
                        F.sum("tag_usage_count").alias("total_historical_usage")
                    ),
                    streaming_tag_performance.current_tag == tag_intelligence.tag,
                    "left"
                ).withColumn(
                    "performance_vs_historical",
                    F.when(F.col("historical_avg_views").isNotNull(),
                        F.round(((F.col("current_avg_views") - F.col("historical_avg_views")) / 
                                F.col("historical_avg_views")) * 100, 1)
                    ).otherwise(F.lit("NEW"))
                ).withColumn(
                    "trend_status",
                    F.when(F.col("performance_vs_historical") == "NEW", "Emerging")
                     .when(F.col("performance_vs_historical") > 50, "Hot Trend")
                     .when(F.col("performance_vs_historical") > 20, "Growing")
                     .when(F.col("performance_vs_historical") > -20, "Stable")
                     .otherwise("Declining")
                )
                
                historical_comparison.orderBy(F.desc("tag_momentum_score")) \
                    .select(
                        F.col("current_tag").alias("Tag"),
                        F.format_number("current_avg_views", 0).alias("Current_Views"),
                        F.format_number("historical_avg_views", 0).alias("Historical_Avg"),
                        F.col("performance_vs_historical").alias("Change_%"),
                        F.col("trend_status").alias("Status"),
                        F.round("tag_momentum_score", 2).alias("Momentum"),
                        F.col("channels_using_tag").alias("Channels")
                    ).show(12, truncate=False)

                historical_comparison.write \
                .mode("append") \
                .parquet(f"hdfs://namenode:9000/storage/hdfs/results/query5/stream_{epoch_id}")
            else:
                streaming_tag_performance.orderBy(F.desc("tag_momentum_score")) \
                    .select(
                        F.col("current_tag").alias("Tag"),
                        F.format_number("current_avg_views", 0).alias("Avg_Views"),
                        F.col("current_usage").alias("Usage"),
                        F.round("tag_momentum_score", 2).alias("Momentum"),
                        F.col("channels_using_tag").alias("Channels")
                    ).show(10, truncate=False)
            
            # # Analiza tag strategija po kanalima
            channel_tag_strategy = streaming_df.groupBy(
                F.window("stream_timestamp", "3 minutes"),
                F.col("channel_title")
            ).agg(
                F.count("*").alias("videos_posted"),
                F.avg("tag_count").alias("avg_tags_per_video"),
                F.avg("tag_diversity_score").alias("tag_strategy_score"),
                F.avg("view_count_parsed").alias("channel_performance"),
                F.sum(F.when(F.col("has_viral_tag"), 1).otherwise(0)).alias("viral_tag_usage"),
                F.sum(F.when(F.col("has_gaming_tag"), 1).otherwise(0)).alias("gaming_tag_usage"),
                F.sum(F.when(F.col("has_music_tag"), 1).otherwise(0)).alias("music_tag_usage")
            ).withColumn(
                "tag_strategy_effectiveness",
                F.when(F.col("tag_strategy_score") >= 3.5, "Expert Tagger")
                 .when(F.col("tag_strategy_score") >= 2.5, "Good Strategy")
                 .when(F.col("tag_strategy_score") >= 1.5, "Basic Tagging")
                 .otherwise("Poor Strategy")
            ).filter(F.col("videos_posted") > 0)
            
            
            # Kombinacije tagova (ako imamo podatke)
            if tag_pair_intelligence is not None:
                tag_combinations_streaming = streaming_df.filter(F.col("tag_count") >= 2) \
                    .withColumn("tag_combinations", 
                        F.expr("transform(sequence(0, size(extracted_tags) - 2), i -> " +
                               "concat(extracted_tags[i], '+', extracted_tags[i+1]))")
                    ).select("*", F.explode("tag_combinations").alias("tag_combo")) \
                    .groupBy(
                        F.window("stream_timestamp", "3 minutes"),
                        F.col("tag_combo")
                    ).agg(
                        F.count("*").alias("combo_usage"),
                        F.avg("view_count_parsed").alias("combo_avg_views")
                    ).filter(F.col("combo_usage") >= 2)
                
                if tag_combinations_streaming.count() > 0:
                    print(f"\n TOP TAG COMBINATIONS:")
                    tag_combinations_streaming.orderBy(F.desc("combo_avg_views")) \
                        .select(
                            F.col("tag_combo").alias("Tag_Combination"),
                            F.format_number("combo_avg_views", 0).alias("Avg_Views"),
                            F.col("combo_usage").alias("Usage")
                        ).show(8, truncate=False)
                    
                    tag_combinations_streaming.write \
                        .mode("append") \
                        .parquet(f"hdfs://namenode:9000/storage/hdfs/results/query5/top_tag_comb/stream_{epoch_id}")
            
            
        except Exception as e:
            print(f" Error in tag_intelligence_processor epoch {epoch_id}: {e}")
    
    streaming_with_watermark = streaming_data.withWatermark("stream_timestamp", "10 minutes")
    
    query = streaming_with_watermark \
        .writeStream \
        .outputMode("update") \
        .trigger(processingTime='3 minutes') \
        .foreachBatch(tag_intelligence_processor) \
        .option("checkpointLocation", "hdfs://namenode:9000/storage/hdfs/checkpoint/query5") \
        .start()
    
    return query

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    tag_intelligence, tag_pair_intelligence = load_tag_intelligence_dataset(spark)
    
    trending_stream = create_kafka_stream(spark, KAFKA_TOPICS["trending"], trending_schema)
    
    streaming_prepared = prepare_streaming_tag_features(trending_stream)
    
    intelligence_query = create_tag_intelligence_analysis(
        streaming_prepared, tag_intelligence, tag_pair_intelligence
    )
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()

