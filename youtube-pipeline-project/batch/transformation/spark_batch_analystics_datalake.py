from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, explode, coalesce, lag, trim, length, lower,
    count, avg, round as spark_round, rank, asc, desc,
    sum as spark_sum, when, min as spark_min, max as spark_max, countDistinct,
    datediff, percent_rank, concat, lit, row_number, monotonically_increasing_id,
    dayofweek, hour, to_timestamp, split as spark_split, size, greatest
)
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder \
    .appName("YouTube curated - Complete Pure Data Lake Processing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/airflow"
pg_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

golden_df = spark.read.parquet("hdfs://namenode:9000/storage/hdfs/processed/golden_dataset") \
    .persist(StorageLevel.MEMORY_AND_DISK)

print(f"Učitano {golden_df.count()} rekorda iz Data Lake")
golden_df.createOrReplaceTempView("youtube_data")

filtered_df = golden_df.filter(col("assignable") == True) \
    .persist(StorageLevel.MEMORY_AND_DISK)

filtered_df.count()


# UPIT 1

print("\n UPIT 1: Prosečna gledanost po kategoriji, regionu i komentarima...")

grouped_df = filtered_df.groupBy(
    "category_title", "region", "trending_full_date", "comments_disabled"
).agg(
    count("*").alias("video_count"),
    spark_round(avg("views"), 2).alias("avg_views"),
    spark_round(avg("comment_count"), 2).alias("avg_comments")
)

views_rank_window = Window.partitionBy("category_title", "region").orderBy(desc("avg_views"))

moving_avg_window = Window.partitionBy(
    "category_title", "region", "comments_disabled"
).orderBy("trending_full_date").rowsBetween(-2, 0)

query1_result = grouped_df.withColumn(
    "views_rank", rank().over(views_rank_window)
).withColumn(
    "moving_avg_views_3d",
    spark_round(avg("avg_views").over(moving_avg_window), 2)
).select(
    "category_title", "region", "trending_full_date", "comments_disabled",
    "video_count", "avg_views", "avg_comments", "views_rank", "moving_avg_views_3d"
).orderBy("category_title", "region", "trending_full_date", "comments_disabled")

print(" TOP 15 rezultata UPIT 1:")
query1_result.show(15, truncate=False)

query1_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query1_category_region_analysis", properties=pg_properties
)


# UPIT 2

print("\n UPIT 2: Top angažman kanala po kategorijama...")

engagement_stats = filtered_df.groupBy("category_title", "channel_title").agg(
    count("*").alias("total_videos"),
    spark_sum("likes").alias("total_likes"),
    spark_sum("dislikes").alias("total_dislikes"),
    spark_sum("comment_count").alias("total_comments"),
    spark_sum(col("likes") + col("comment_count")).alias("engagement_score"),
    spark_round(spark_sum(col("likes") + col("comment_count")) / count("*"), 2).alias("avg_engagement_per_video"),
    when(spark_sum("dislikes") == 0, None).otherwise(
        spark_round(spark_sum("likes") / spark_sum("dislikes"), 2)
    ).alias("like_dislike_ratio")
)

rank_window = Window.partitionBy("category_title").orderBy(desc("engagement_score"))

query2_result = engagement_stats.withColumn(
    "rank_in_category", rank().over(rank_window)
).filter(col("rank_in_category") <= 5).select(
    "category_title", "channel_title", "total_videos", "total_likes",
    "total_dislikes", "total_comments", "engagement_score",
    "avg_engagement_per_video", "like_dislike_ratio", "rank_in_category"
).orderBy("category_title", "rank_in_category")

print(" TOP 25 rezultata UPIT 2:")
query2_result.show(25, truncate=False)

query2_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query2_channel_engagement", properties=pg_properties
)


# UPIT 3

print("\n UPIT 3: Zlatne kombinacije viralnih sadržaja...")

stats = golden_df.filter(
    (col("publish_date").isNotNull()) & (col("trending_full_date").isNotNull())
).groupBy("category_title", "region", "publish_date", "video_id").agg(
    datediff(spark_min("trending_full_date"), col("publish_date")).alias("days_to_trend"),
    countDistinct("trending_full_date").alias("trend_duration_days")
).select(
    col("category_title").alias("category"),
    col("region").alias("region"),
    col("days_to_trend"),
    col("trend_duration_days")
)

aggregated = stats.groupBy("category", "region").agg(
    spark_round(avg("days_to_trend"), 2).alias("avg_days_to_trend"),
    spark_round(avg("trend_duration_days"), 2).alias("avg_trend_days")
)


fastest_window = Window.partitionBy("category").orderBy(asc("avg_days_to_trend"))
longest_window = Window.partitionBy("category").orderBy(desc("avg_trend_days"))

ranked = aggregated.withColumn(
    "pct_fastest_to_trend", percent_rank().over(fastest_window)
).withColumn(
    "pct_longest_trending", percent_rank().over(longest_window)
)

query3_result = ranked.select(
    "category", "region", "avg_days_to_trend", "avg_trend_days",
    spark_round(col("pct_fastest_to_trend") * 100, 2).alias("pct_rank_fastest"),
    spark_round(col("pct_longest_trending") * 100, 2).alias("pct_rank_longest")
).filter(
    (col("pct_fastest_to_trend") <= 0.10) & (col("pct_longest_trending") >= 0.90)
).orderBy("pct_fastest_to_trend", "pct_longest_trending")

print(" Rezultati UPIT 3 - Zlatne kombinacije:")
query3_result.show(20, truncate=False)

query3_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query3_viral_golden_combinations_proba", properties=pg_properties
)


# UPIT 4

print("\n UPIT 4: Najčešći tip problema po kombinaciji kategorije i regiona...")

problem_stats = golden_df.filter(
    col("category_title").isNotNull() & (col("category_title") != "")
).select(
    "video_id", "category_title", "region",
    when(col("video_error_or_removed"), "Removed")
    .when((col("comments_disabled") == True) & (col("ratings_disabled") == True), "All Interactions Disabled")
    .when(col("comments_disabled") == True, "Comments Disabled Only")
    .when(col("ratings_disabled") == True, "Ratings Disabled Only")
    .otherwise("No Issue").alias("problem_type"),
    "views"
)

problem_counts = problem_stats.filter(col("problem_type") != "No Issue").groupBy(
    "category_title", "region", "problem_type"
).agg(count("video_id").alias("num_videos"))

total_window = Window.partitionBy("category_title", "region")

problem_agg = problem_counts.withColumn(
    "total_problems", spark_sum(col("num_videos")).over(total_window)
).withColumn(
    "pct_of_local",
    spark_round(
        (col("num_videos").cast("double") / col("total_problems").cast("double")) * 100, 1
    )
)

rank_window_q4 = Window.partitionBy("category_title", "region").orderBy(desc("pct_of_local"))

query4_result = problem_agg.withColumn(
    "problem_rank", row_number().over(rank_window_q4)
).filter(col("problem_rank") == 1).select(
    "category_title", "region", "problem_type", "num_videos", "pct_of_local"
).withColumn(
    "pct_of_local", concat(col("pct_of_local").cast("string"), lit("%"))
).orderBy("category_title", "region")

query4_result.show(30, truncate=False)

query4_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query4_problem_analysis_proba_2", properties=pg_properties
)


# TAGS - shared base (persist jer se koristi u Q5 i Q6)

print("\n Pripremam tags_exploded...")

tags_exploded = golden_df.select(
    "video_id", "likes", "views", "comment_count", "category_title", "region",
    explode(col("tags_list")).alias("tag")
).filter(
    (col("tag").isNotNull()) &
    (length(trim(col("tag"))) > 2) &
    (~col("tag").isin("uncategorized", "no-tags", "unknown"))
).withColumn("tag", lower(trim(col("tag")))) \
 .persist(StorageLevel.MEMORY_AND_DISK)

tags_exploded.count()  


# UPIT 5

print("\n UPIT 5: Tag analiza - viral score ranking...")

tag_performance_metrics = tags_exploded.groupBy("tag").agg(
    count("*").alias("video_count"),
    countDistinct("region").alias("regions_count"),
    countDistinct("category_title").alias("categories_count"),
    spark_sum("likes").alias("total_likes"),
    spark_sum("views").alias("total_views"),
    spark_sum(col("likes") + col("comment_count")).alias("total_engagement"),
    spark_round(avg("likes"), 0).alias("avg_likes"),
    spark_round(avg(col("likes") + col("comment_count")), 0).alias("avg_engagement"),
    count(when(col("likes") > 100000, 1)).alias("high_performance_videos"),
    spark_round(
        count(when(col("likes") > 100000, 1)) * 100.0 / count("*"), 2
    ).alias("viral_success_rate"),
    (
        count("*") * 0.3 +
        avg("likes") * 0.0001 +
        count(when(col("likes") > 100000, 1)) * 50
    ).alias("viral_score")
).filter(col("video_count") >= 10)


query5_result = tag_performance_metrics \
    .filter(col("viral_score") > 100) \
    .select(
        "tag", "video_count", "avg_likes", "viral_success_rate",
        "regions_count", "categories_count",
        spark_round("viral_score", 0).alias("viral_score")
    ) \
    .orderBy(desc("viral_score")) \
    .limit(30) \
    .withColumn("overall_rank", monotonically_increasing_id() + 1)

print(" TOP 30 tagova UPIT 5:")
query5_result.show(30, truncate=False)

query5_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query5_tag_viral_analysis", properties=pg_properties
)


# UPIT 6

print("\n UPIT 6: Napredna tag analiza sa Power Level i preporukama...")

tag_performance = tags_exploded.groupBy("tag").agg(
    count("*").alias("video_count"),
    countDistinct("region").alias("active_regions"),
    countDistinct("category_title").alias("categories_count"),
    spark_round(avg("likes"), 0).alias("avg_likes"),
    spark_round(avg("views"), 0).alias("avg_views"),
    count(when(col("likes") > 100000, 1)).alias("high_performance_videos"),
    spark_round(
        count(when(col("likes") > 100000, 1)) * 100.0 / count("*"), 2
    ).alias("viral_success_rate"),
    (
        count("*") * 0.3 +
        avg("likes") * 0.0001 +
        count(when(col("likes") > 100000, 1)) * 50
    ).alias("viral_score")
).filter(col("video_count") >= 10)

tag_with_levels = tag_performance \
    .withColumn(
        "tag_power_level",
        when((col("viral_success_rate") > 50) & (col("video_count") > 50), "MAGIC")
        .when((col("viral_success_rate") > 30) & (col("avg_likes") > 500000), "POWERFUL")
        .when((col("video_count") > 100) & (col("avg_likes") > 100000), "RELIABLE")
        .when(col("viral_success_rate") > 20, "PROMISING")
        .otherwise("AVERAGE")
    ) \
    .withColumn(
        "tip_taga",
        when(col("viral_success_rate") > 90, "TOP FREQUENCY")
        .when(col("avg_likes") > 1000000, "TOP PERFORMANCE")
        .when(col("viral_success_rate") > 40, "HIGH SUCCESS")
        .otherwise("GOOD")
    ) \
    .withColumn(
        "recommendation",
        when((col("viral_success_rate") > 50) & (col("video_count") > 50), "MUST USE")
        .when((col("viral_success_rate") > 30) & (col("avg_likes") > 500000), "HIGHLY RECOMMENDED")
        .when((col("video_count") > 100) & (col("avg_likes") > 100000), "SAFE CHOICE")
        .when(col("viral_success_rate") > 20, "EMERGING")
        .otherwise("CONSIDER")
    )


power_level_rank_window = Window.partitionBy("tag_power_level").orderBy(desc("viral_score"))
success_percentile_window = Window.partitionBy("tag_power_level").orderBy("viral_success_rate")
smoothed_window = Window.partitionBy("tag_power_level").orderBy(desc("viral_score")).rowsBetween(-2, 2)

query6_result = tag_with_levels \
    .filter(col("viral_score") > 100) \
    .withColumn("rank_within_power_level", rank().over(power_level_rank_window)) \
    .withColumn(
        "success_percentile",
        spark_round(percent_rank().over(success_percentile_window) * 100, 1)
    ) \
    .withColumn(
        "smoothed_viral_score",
        spark_round(avg("viral_score").over(smoothed_window), 0)
    ) \
    .withColumn(
        "market_position",
        when(percent_rank().over(success_percentile_window) >= 0.90, "TOP 10%")
        .when(percent_rank().over(success_percentile_window) >= 0.75, "TOP 25%")
        .when(percent_rank().over(success_percentile_window) >= 0.50, "TOP 50%")
        .otherwise("STANDARD")
    ) \
    .withColumn("global_rank", monotonically_increasing_id() + 1) \
    .select(
        "tag", "active_regions", "categories_count",
        "tag_power_level", "tip_taga", "recommendation",
        "viral_success_rate", "avg_likes",
        spark_round("viral_score", 0).alias("viral_score"),
        "rank_within_power_level",
        "success_percentile",
        "market_position",
        "smoothed_viral_score",
        "global_rank"
    ) \
    .orderBy(desc("viral_score")) \
    .limit(30)

print(" TOP 30 naprednih tag analiza UPIT 6:")
query6_result.show(30, truncate=False)

query6_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query6_advanced_tag_recommendations", properties=pg_properties
)


# UPIT 7

print("\n UPIT 7: YouTube kanali sa najbržom viralizacijom...")

speed_rank_window = Window.partitionBy("channel_title").orderBy("days_to_trending")
momentum_window = Window.partitionBy("channel_title").orderBy("trending_full_date").rowsBetween(-6, 0)

viral_timeline = golden_df.select(
    "channel_title", "category_title", "video_id",
    "trending_full_date", "views", "likes", "publish_date"
).filter(
    (col("publish_date").isNotNull()) & (col("trending_full_date").isNotNull())
).withColumn(
    "days_to_trending", datediff(col("trending_full_date"), col("publish_date"))
).filter(col("days_to_trending") >= 0).withColumn(
    "speed_rank_in_channel", rank().over(speed_rank_window)
).withColumn(
    "channel_momentum_7d", avg("views").over(momentum_window)
)

query7_result = viral_timeline.filter(
    (col("days_to_trending") >= 0) & (col("days_to_trending") <= 30)
).groupBy("channel_title", "category_title").agg(
    count("*").alias("total_viral_videos"),
    spark_round(avg("days_to_trending"), 1).alias("avg_days_to_viral"),
    spark_round(avg("channel_momentum_7d"), 0).alias("avg_momentum"),
    spark_round(
        count(when(col("days_to_trending") <= 3, 1)) * 100.0 / count("*"), 1
    ).alias("fast_viral_percentage")
).filter(col("total_viral_videos") >= 5).orderBy(
    desc("fast_viral_percentage"), desc("avg_momentum")
).limit(20)

print(" TOP 20 najbrži viralni kanali UPIT 7:")
query7_result.show(20, truncate=False)

query7_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query7_fastest_viral_channels", properties=pg_properties
)


# UPIT 8

print("\n UPIT 8: Optimalna kombinacija opisa i thumbnail kvaliteta...")

content_analysis = filtered_df.select(
    "video_id", "category_title", "region", "views", "likes",
    "comment_count", "description", "thumbnail_link"
).withColumn(
    "description_length_category",
    when(length(col("description")) == 0, "No Description")
    .when(length(col("description")) < 100, "Very Short")
    .when(length(col("description")) < 300, "Short")
    .when(length(col("description")) < 1000, "Medium")
    .when(length(col("description")) < 2000, "Long")
    .otherwise("Very Long")
).withColumn(
    "thumbnail_quality",
    when(col("thumbnail_link").like("%maxresdefault%"), "High Quality")
    .when(col("thumbnail_link").like("%hqdefault%"), "Medium Quality")
    .otherwise("Standard Quality")
)

query8_aggregated = content_analysis.groupBy(
    "category_title", "description_length_category", "thumbnail_quality"
).agg(
    count("*").alias("video_count"),
    spark_round(avg("views"), 0).alias("avg_views"),
    spark_round(avg("likes"), 0).alias("avg_likes")
).filter(col("video_count") >= 10)

performance_rank_window = Window.partitionBy("category_title").orderBy(desc("avg_views"))
performance_percentile_window = Window.partitionBy("category_title").orderBy("avg_views")

query8_result = query8_aggregated.withColumn(
    "performance_rank", rank().over(performance_rank_window)
).withColumn(
    "performance_percentile", percent_rank().over(performance_percentile_window)
).orderBy("category_title", "performance_rank")

print(" Najbolje kombinacije opisa/thumbnail UPIT 8:")
query8_result.show(40, truncate=False)

query8_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query8_content_optimization", properties=pg_properties
)


# UPIT 9

print("\n UPIT 9: Optimalno vreme lansiranje po kategoriji i regionu...")

seasonal_window = Window.partitionBy("category_title", "region").orderBy("trending_year", "trending_month")
seasonal_trend_window = Window.partitionBy("category_title", "region").orderBy(
    "trending_year", "trending_month"
).rowsBetween(-2, 1)

seasonal_patterns = filtered_df.filter(
    col("trending_month").isNotNull() & col("trending_year").isNotNull()
).groupBy("category_title", "region", "trending_month", "trending_year").agg(
    count("*").alias("videos_count"),
    avg("views").alias("avg_views"),
    avg("likes").alias("avg_engagement")
).withColumn(
    "prev_month_views", lag(col("avg_views"), 1).over(seasonal_window)
).withColumn(
    "seasonal_trend", avg(col("videos_count")).over(seasonal_trend_window)
)

avg_seasonal_window = Window.partitionBy("category_title", "region")
seasonal_patterns_with_avg = seasonal_patterns.withColumn(
    "avg_seasonal_trend_cat_region", avg("seasonal_trend").over(avg_seasonal_window)
)

query9_aggregated = seasonal_patterns_with_avg.groupBy(
    "category_title", "region", "trending_month", "avg_seasonal_trend_cat_region"
).agg(
    spark_round(avg("seasonal_trend"), 1).alias("trend_strength"),
    coalesce(
        when(avg("prev_month_views") > 0,
            spark_round(
                ((avg("avg_views") - avg("prev_month_views")) / avg("prev_month_views")) * 100, 1
            )
        ).otherwise(lit(0.0)),
        lit(0.0)
    ).alias("mom_growth_pct"),
    spark_sum("videos_count").alias("total_videos_count"),
    count("*").alias("month_count")
).filter(
    (col("month_count") >= 1) & (col("total_videos_count") >= 15)
)

month_rank_window = Window.partitionBy("category_title", "region").orderBy(desc("trend_strength"))

query9_result = query9_aggregated.withColumn(
    "launch_recommendation",
    when(col("trend_strength") > col("avg_seasonal_trend_cat_region") * 1.2, "OPTIMAL LAUNCH TIME")
    .when(col("trend_strength") > col("avg_seasonal_trend_cat_region"), "GOOD TIME")
    .otherwise("AVOID")
).withColumn(
    "data_confidence",
    when(col("total_videos_count") >= 50, "HIGH CONFIDENCE")
    .when(col("total_videos_count") >= 20, "MEDIUM CONFIDENCE")
    .when(col("total_videos_count") >= 10, "LOW CONFIDENCE")
    .otherwise("VERY LOW CONFIDENCE")
).withColumn(
    "month_rank", rank().over(month_rank_window)
).orderBy("category_title", "region", desc("data_confidence"), "month_rank").select(
    "category_title", "region", "trending_month", "trend_strength",
    "mom_growth_pct", "launch_recommendation", "data_confidence", "month_rank"
)

print(" Optimalno vreme lansiranja UPIT 9:")
query9_result.show(50, truncate=False)

query9_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query9_optimal_launch_timing", properties=pg_properties
)


# UPIT 10

print("\n UPIT 10: Najgledaniji video po kanalu sa regionom...")

channel_window = Window.partitionBy("channel_title").orderBy(desc("views"))
avg_channel_window = Window.partitionBy("channel_title")

channel_top_videos = golden_df.select(
    "channel_title", "video_title", "views", "region",
    "category_title", "publish_year", "trending_year"
).withColumn(
    "rn", row_number().over(channel_window)
).withColumn(
    "avg_views_per_channel", avg("views").over(avg_channel_window)
)

query10_result = channel_top_videos.filter(
    (col("rn") == 1) & (col("views") >= 100000000)
).select(
    "channel_title",
    col("video_title").alias("top_video"),
    col("category_title").alias("top_video_category"),
    col("views").alias("max_views"),
    col("region").alias("top_region"),
    "publish_year",
    spark_round("avg_views_per_channel", 0).alias("avg_views_per_channel")
).orderBy(desc("max_views"))

print(" TOP kanali sa najvećim hitovima UPIT 10:")
query10_result.show(30, truncate=False)

query10_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query10_top_channels_mega_hits", properties=pg_properties
)


# ============================================================
# SHARED BASE za Q11, Q13, Q14 — persistence po videu
# ============================================================

print("\n Pripremam video_persistence_df (shared base za Q11, Q13, Q14)...")

video_persistence_df = golden_df.groupBy(
    "video_id", "video_title", "channel_title", "category_title", "region",
    "publish_time", "thumbnail_link"
).agg(
    countDistinct("trending_full_date").alias("days_on_trending"),
    spark_round(avg("views"), 0).alias("avg_views"),
    spark_max("views").alias("max_views")
).persist(StorageLevel.MEMORY_AND_DISK)

video_persistence_df.count()


# ============================================================
# UPIT 11: Trending persistence po kanalu
# — koliko dana (distinct trending datuma) kanali zadržavaju
#   videe na trending listi, sortirano po kategoriji
# ============================================================

print("\n UPIT 11: Trending persistence po kanalu...")

persistence_rank_window = Window.partitionBy("category_title").orderBy(desc("avg_days_on_trending"))

query11_result = video_persistence_df.groupBy("channel_title", "category_title", "region").agg(
    count("*").alias("total_videos"),
    spark_round(avg("days_on_trending"), 2).alias("avg_days_on_trending"),
    spark_max("days_on_trending").alias("max_days_on_trending"),
    spark_sum("days_on_trending").alias("total_trending_days"),
    spark_round(avg("avg_views"), 0).alias("avg_views")
).withColumn(
    "persistence_rank_in_category", rank().over(persistence_rank_window)
).withColumn(
    "persistence_tier",
    when(col("avg_days_on_trending") >= 10, "LONG TERM DOMINANT")
    .when(col("avg_days_on_trending") >= 5, "CONSISTENT")
    .when(col("avg_days_on_trending") >= 2, "MODERATE")
    .otherwise("FLASH TRENDING")
).orderBy("category_title", "persistence_rank_in_category")

print(" TOP rezultati UPIT 11 - Trending persistence:")
query11_result.show(30, truncate=False)

query11_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query11_trending_persistence", properties=pg_properties
)


# ============================================================
# UPIT 12: Tag cloud po regionu i kategoriji
# — za svaku (category_title, region, tag) kombinaciju:
#   prosečan broj pregleda, engagement score, broj videa
#   (ulaz za Metabase tag cloud vizualizaciju u Koraku 3a)
# ============================================================

print("\n UPIT 12: Tag cloud po regionu i kategoriji...")

tag_region_cat_rank_window = Window.partitionBy("category_title", "region").orderBy(desc("avg_views"))

query12_result = tags_exploded.groupBy("category_title", "region", "tag").agg(
    count("*").alias("video_count"),
    spark_round(avg("views"), 0).alias("avg_views"),
    spark_round(avg("likes"), 0).alias("avg_likes"),
    spark_round(avg(col("likes") + col("comment_count")), 0).alias("avg_engagement"),
    spark_round(
        avg(col("likes") + col("comment_count")) /
        greatest(avg("views"), lit(1)) * 100,
        2
    ).alias("engagement_rate_pct"),
    spark_round(
        count(when(col("likes") > 100000, 1)) * 100.0 / count("*"), 2
    ).alias("viral_success_rate")
).filter(
    col("video_count") >= 3
).withColumn(
    "tag_rank_in_cat_region", rank().over(tag_region_cat_rank_window)
).orderBy("category_title", "region", "tag_rank_in_cat_region")

print(" Rezultati UPIT 12 - Tag cloud po regionu/kategoriji:")
query12_result.show(30, truncate=False)

query12_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query12_tags_by_region_category", properties=pg_properties
)


# ============================================================
# UPIT 13: Obrasci naslova i thumbnail za dugo-trending videe
# — za videe koji ostaju 3+ dana na trending listi,
#   analizira karakteristike naslova i thumbnail tipa
#   po (category_title, region) kombinaciji
#   (ulaz za Metabase analizu u Koraku 3b)
# ============================================================

print("\n UPIT 13: Obrasci naslova i thumbnail za dugo-trending videe...")

title_features_df = video_persistence_df.filter(
    col("video_title").isNotNull()
).withColumn(
    "title_length", length(col("video_title"))
).withColumn(
    "title_word_count", size(spark_split(trim(col("video_title")), "\\s+"))
).withColumn(
    "has_number", col("video_title").rlike("\\d+")
).withColumn(
    "has_brackets",
    col("video_title").rlike("\\[|\\(|\\{")
).withColumn(
    "has_official",
    lower(col("video_title")).rlike("official")
).withColumn(
    "has_feature",
    lower(col("video_title")).rlike("ft\\.|feat\\.|featuring")
).withColumn(
    "has_mv",
    lower(col("video_title")).rlike("m/v|\\bmv\\b|music video")
).withColumn(
    "title_length_category",
    when(col("title_length") <= 30, "Short (<=30)")
    .when(col("title_length") <= 60, "Medium (31-60)")
    .when(col("title_length") <= 90, "Long (61-90)")
    .otherwise("Very Long (>90)")
).withColumn(
    "thumbnail_type",
    when(col("thumbnail_link").like("%maxresdefault%"), "High Quality")
    .when(col("thumbnail_link").like("%hqdefault%"), "Medium Quality")
    .otherwise("Standard Quality")
)

pattern_rank_window = Window.partitionBy("category_title", "region").orderBy(desc("avg_trending_days"))

query13_result = title_features_df.filter(
    col("days_on_trending") >= 3
).groupBy(
    "category_title", "region",
    "has_number", "has_brackets", "has_official", "has_feature", "has_mv",
    "title_length_category", "thumbnail_type"
).agg(
    count("*").alias("video_count"),
    spark_round(avg("days_on_trending"), 2).alias("avg_trending_days"),
    spark_max("days_on_trending").alias("max_trending_days"),
    spark_round(avg("avg_views"), 0).alias("avg_views"),
    spark_round(avg("title_length"), 1).alias("avg_title_length"),
    spark_round(avg("title_word_count"), 1).alias("avg_word_count")
).filter(
    col("video_count") >= 3
).withColumn(
    "pattern_rank_in_cat_region", rank().over(pattern_rank_window)
).orderBy("category_title", "region", "pattern_rank_in_cat_region")

print(" Rezultati UPIT 13 - Obrasci naslova za dugo-trending videe:")
query13_result.show(30, truncate=False)

query13_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query13_title_patterns", properties=pg_properties
)


# ============================================================
# UPIT 14: Heatmapa dan u nedelji × sat objave
# — za svaku (category_title, region, day_of_week, hour_of_day)
#   kombinaciju: prosečan broj dana na trending listi i broj videa
#   (ulaz za Metabase heatmapu u Koraku 4)
# ============================================================

print("\n UPIT 14: Heatmapa dan×sat objave → prosečan trending period...")

heatmap_base_df = video_persistence_df.filter(
    col("publish_time").isNotNull()
).withColumn(
    "publish_ts", to_timestamp(col("publish_time"))
).filter(
    col("publish_ts").isNotNull()
).withColumn(
    "day_of_week", dayofweek(col("publish_ts"))
).withColumn(
    "hour_of_day", hour(col("publish_ts"))
).withColumn(
    "day_name",
    when(col("day_of_week") == 1, "Sunday")
    .when(col("day_of_week") == 2, "Monday")
    .when(col("day_of_week") == 3, "Tuesday")
    .when(col("day_of_week") == 4, "Wednesday")
    .when(col("day_of_week") == 5, "Thursday")
    .when(col("day_of_week") == 6, "Friday")
    .when(col("day_of_week") == 7, "Saturday")
)

heatmap_rank_window = Window.partitionBy("category_title", "region").orderBy(desc("avg_trending_days"))

query14_result = heatmap_base_df.groupBy(
    "category_title", "region", "day_of_week", "day_name", "hour_of_day"
).agg(
    count("*").alias("video_count"),
    spark_round(avg("days_on_trending"), 2).alias("avg_trending_days"),
    spark_max("days_on_trending").alias("max_trending_days"),
    spark_round(avg("avg_views"), 0).alias("avg_views")
).filter(
    col("video_count") >= 5
).withColumn(
    "launch_recommendation",
    when(col("avg_trending_days") >= 5, "OPTIMAL")
    .when(col("avg_trending_days") >= 3, "GOOD")
    .when(col("avg_trending_days") >= 2, "AVERAGE")
    .otherwise("AVOID")
).withColumn(
    "slot_rank_in_cat_region", rank().over(heatmap_rank_window)
).orderBy("category_title", "region", "day_of_week", "hour_of_day")

print(" Rezultati UPIT 14 - Heatmapa dan×sat:")
query14_result.show(50, truncate=False)

query14_result.write.mode("overwrite").jdbc(
    pg_url, "batch_data_queries.query14_publish_heatmap", properties=pg_properties
)


# CLEANUP

tags_exploded.unpersist()
video_persistence_df.unpersist()
filtered_df.unpersist()
golden_df.unpersist()

print("\n BATCH OBRADA ZAVRŠENA!")
spark.stop()