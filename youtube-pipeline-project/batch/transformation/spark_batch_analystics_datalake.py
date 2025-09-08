from pyspark.sql import SparkSession, Window # type: ignore
from pyspark.sql.functions import (  # type: ignore
col, explode, coalesce, lag, trim, length, lower,
count, avg, round as spark_round, rank, asc, desc, sum as spark_sum, when, 
round as spark_round, min as spark_min, countDistinct, datediff, percent_rank, cast, 
concat, lit, row_number) 
from pyspark.sql.types import * # type: ignore


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


golden_df = spark.read.parquet("hdfs://namenode:9000/storage/hdfs/processed/golden_dataset")

print(f"Učitano {golden_df.count()} rekorda iz Data Lake")

golden_df.createOrReplaceTempView("youtube_data")



# UPIT 1: Koja je prosečna gledanost i prosečan broj komentara po kategoriji, regionu i datumu kada su videi postali trending, 
#            i kako se ti proseci razlikuju za videe sa uključenim i onemogućenim komentarima? Koje kategorije i regioni dominiraju po gledanosti, 
#            a kako se trend pregleda menja tokom poslednja tri dana?

print("\n UPIT 1: Prosečna gledanost po kategoriji, regionu i komentarima...")

# query1_result = spark.sql("""
# SELECT 
#     category_title,
#     region,
#     trending_full_date,
#     comments_disabled,
#     COUNT(*) as video_count,
#     ROUND(AVG(views), 2) as avg_views,
#     ROUND(AVG(comment_count), 2) as avg_comments,
#     RANK() OVER (
#         PARTITION BY category_title, region 
#         ORDER BY AVG(views) DESC
#     ) as views_rank,
#     ROUND(
#         AVG(AVG(views)) OVER (
#             PARTITION BY category_title, region, comments_disabled
#             ORDER BY trending_full_date
#             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
#         ), 2
#     ) as moving_avg_views_3d                  
# FROM youtube_data
# WHERE assignable = true
# GROUP BY category_title, region, trending_full_date, comments_disabled
# ORDER BY category_title, region, trending_full_date, comments_disabled
# """)


filtered_df = golden_df.filter(col("assignable") == True)

grouped_df = filtered_df.groupBy(
    "category_title", 
    "region", 
    "trending_full_date", 
    "comments_disabled"
).agg(
    count("*").alias("video_count"),
    spark_round(avg("views"), 2).alias("avg_views"),
    spark_round(avg("comment_count"), 2).alias("avg_comments")
)

views_rank_window = Window.partitionBy("category_title", "region").orderBy(desc("avg_views"))

moving_avg_window = Window.partitionBy(
    "category_title", 
    "region", 
    "comments_disabled"
).orderBy("trending_full_date").rowsBetween(-2, 0)

query1_result = grouped_df.withColumn(
    "views_rank", 
    rank().over(views_rank_window)
).withColumn(
    "moving_avg_views_3d",
    spark_round(avg("avg_views").over(moving_avg_window), 2)
).select(
    "category_title",
    "region", 
    "trending_full_date",
    "comments_disabled",
    "video_count",
    "avg_views",
    "avg_comments",
    "views_rank",
    "moving_avg_views_3d"
).orderBy(
    "category_title", 
    "region", 
    "trending_full_date", 
    "comments_disabled"
)
print(" TOP 15 rezultata UPIT 1:")
query1_result.show(15, truncate=False)


query1_result.write.mode("overwrite").jdbc(
    pg_url,
    "query1_category_region_analysis",
    properties=pg_properties
)


#  UPIT 2: Koje kategorije i kanali ostvaruju najveći angažman korisnika?  
#            Koliki je njihov engagement score (ukupan broj lajkova + komentara),
#            kao i da li taj angažman dolazi iz pozitivnog ili negativnog feedbacka.
#            Kako se rangiraju unutar svojih kategorija?  
#            Koji su top 5 kanala po angažmanu u svakoj kategoriji?

print("\n UPIT 2: Top angažman kanala po kategorijama...")

# query2_result = spark.sql("""
# WITH engagement_stats AS (
#     SELECT
#         category_title,
#         channel_title,
#         COUNT(*) AS total_videos,
#         SUM(likes) AS total_likes,
#         SUM(dislikes) AS total_dislikes,
#         SUM(comment_count) AS total_comments,
#         SUM(likes + comment_count) AS engagement_score,
#         ROUND(SUM(likes + comment_count) / COUNT(*), 2) AS avg_engagement_per_video,
#         CASE 
#             WHEN SUM(dislikes) = 0 THEN NULL
#             ELSE ROUND(SUM(likes) / SUM(dislikes), 2)
#         END AS like_dislike_ratio
#     FROM youtube_data
#     WHERE assignable = true
#     GROUP BY category_title, channel_title
# )
# SELECT *
# FROM (
#     SELECT
#         category_title,
#         channel_title,
#         total_videos,
#         total_likes,
#         total_dislikes,
#         total_comments,
#         engagement_score,
#         avg_engagement_per_video,
#         like_dislike_ratio,
#         RANK() OVER (PARTITION BY category_title ORDER BY engagement_score DESC) AS rank_in_category
#     FROM engagement_stats
# ) ranked
# WHERE rank_in_category <= 5  -- Top 5 kanala po svakoj kategoriji
# ORDER BY category_title, rank_in_category
# """)

filtered_df = golden_df.filter(col("assignable") == True)

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
    "rank_in_category", 
    rank().over(rank_window)
).filter(
    col("rank_in_category") <= 5
).select(
    "category_title",
    "channel_title", 
    "total_videos",
    "total_likes",
    "total_dislikes",
    "total_comments",
    "engagement_score",
    "avg_engagement_per_video",
    "like_dislike_ratio",
    "rank_in_category"
).orderBy("category_title", "rank_in_category")


print(" TOP 25 rezultata UPIT 2:")
query2_result.show(25, truncate=False)

query2_result.write.mode("overwrite").jdbc(
    pg_url,
    "query2_channel_engagement",
    properties=pg_properties
)

#  UPIT 3: Koje YouTube kategorije i regioni su top 10% najbržih viralnih videa i istovremeno među top 10% po trajanju na trending listi?
#            Koliko prosečno treba da video dospe na trending i koliko dugo ostaje, i kako se ove kombinacije rangiraju u odnosu na sve ostale?
#            Koji sadržaji su i instant hit i dugotrajni hit, tj. “zlatne kombinacije”?

print("\n UPIT 3: Zlatne kombinacije viralnih sadržaja...")

# query3_result = spark.sql("""
# WITH stats AS (
#     SELECT
#         category_title AS category,
#         region AS region,
#         DATEDIFF(MIN(trending_full_date), publish_date) AS days_to_trend,
#         COUNT(DISTINCT trending_full_date) AS trend_duration_days
#     FROM youtube_data
#     WHERE publish_date IS NOT NULL AND trending_full_date IS NOT NULL
#     GROUP BY category_title, region, publish_date, video_id
# ),
# aggregated AS (
#     SELECT
#         category,
#         region,
#         ROUND(AVG(days_to_trend), 2) AS avg_days_to_trend,
#         ROUND(AVG(trend_duration_days), 2) AS avg_trend_days
#     FROM stats
#     GROUP BY category, region
# ),
# ranked AS (
#     SELECT
#         category,
#         region,
#         avg_days_to_trend,
#         avg_trend_days,
#         PERCENT_RANK() OVER (ORDER BY avg_days_to_trend ASC) AS pct_fastest_to_trend,
#         PERCENT_RANK() OVER (ORDER BY avg_trend_days DESC) AS pct_longest_trending
#     FROM aggregated
# )
# SELECT
#     category,
#     region,
#     avg_days_to_trend,
#     avg_trend_days,
#     ROUND(pct_fastest_to_trend * 100, 2) AS pct_rank_fastest,
#     ROUND(pct_longest_trending * 100, 2) AS pct_rank_longest
# FROM ranked
# WHERE pct_fastest_to_trend <= 0.10 AND pct_longest_trending >= 0.90
# ORDER BY pct_fastest_to_trend, pct_longest_trending
# """)

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

# Kreiranje aggregated DataFrame (ekvivalent drugog CTE-a)
aggregated = stats.groupBy("category", "region").agg(
    spark_round(avg("days_to_trend"), 2).alias("avg_days_to_trend"),
    spark_round(avg("trend_duration_days"), 2).alias("avg_trend_days")
)

# Window funkcije za percent_rank
fastest_window = Window.orderBy(asc("avg_days_to_trend"))
longest_window = Window.orderBy(desc("avg_trend_days"))

# Kreiranje ranked DataFrame (ekvivalent trećeg CTE-a)
ranked = aggregated.withColumn(
    "pct_fastest_to_trend", 
    percent_rank().over(fastest_window)
).withColumn(
    "pct_longest_trending", 
    percent_rank().over(longest_window)
)

# Finalni select sa filtriranjem top 10% i bottom 10%
query3_result = ranked.select(
    "category",
    "region",
    "avg_days_to_trend",
    "avg_trend_days",
    spark_round(col("pct_fastest_to_trend") * 100, 2).alias("pct_rank_fastest"),
    spark_round(col("pct_longest_trending") * 100, 2).alias("pct_rank_longest")
).filter(
    (col("pct_fastest_to_trend") <= 0.10) & (col("pct_longest_trending") >= 0.90)
).orderBy("pct_fastest_to_trend", "pct_longest_trending")

print(" Rezultati UPIT 3 - Zlatne kombinacije:")
query3_result.show(20, truncate=False)

query3_result.write.mode("overwrite").jdbc(
    pg_url,
    "query3_viral_golden_combinations_proba",
    properties=pg_properties
)

#  UPIT 4: Koji tip problema je najčešći po kombinaciji kategorije i regiona i koliki procenat problematičnih videa čini?

print("\n UPIT 4: Najčešći tip problema po kombinaciji kategorije i regiona...")

# query4_result = spark.sql("""
# WITH problem_stats AS (
#     SELECT
#         video_id,
#         category_title,
#         region,
#         CASE 
#             WHEN video_error_or_removed THEN 'Removed'
#             WHEN comments_disabled = true AND ratings_disabled = true THEN 'All Interactions Disabled'
#             WHEN comments_disabled = true THEN 'Comments Disabled Only'
#             WHEN ratings_disabled = true THEN 'Ratings Disabled Only'
#             ELSE 'No Issue'
#         END AS problem_type,
#         views
#     FROM youtube_data
# ),
# problem_agg AS (
#     SELECT
#         category_title,
#         region,
#         problem_type,
#         COUNT(video_id) AS num_videos,
#         ROUND(
#             CAST(COUNT(video_id) AS DOUBLE) / SUM(COUNT(video_id)) OVER (PARTITION BY category_title, region) * 100,
#             1
#         ) AS pct_of_local
#     FROM problem_stats
#     WHERE problem_type != 'No Issue'
#     GROUP BY category_title, region, problem_type
# ),
# ranked AS (
#     SELECT
#         *,
#         ROW_NUMBER() OVER (PARTITION BY category_title, region ORDER BY pct_of_local DESC) AS problem_rank
#     FROM problem_agg
# )
# SELECT
#     category_title,
#     region,
#     problem_type,
#     num_videos,
#     pct_of_local || '%' AS pct_of_local
# FROM ranked
# WHERE problem_rank = 1
# ORDER BY category_title, region
# """)

problem_stats = golden_df.filter(col("category_title").isNotNull() & (col("category_title") != "")).select(
    "video_id",
    "category_title", 
    "region",
    when(col("video_error_or_removed"), "Removed")
    .when((col("comments_disabled") == True) & (col("ratings_disabled") == True), "All Interactions Disabled")
    .when(col("comments_disabled") == True, "Comments Disabled Only")
    .when(col("ratings_disabled") == True, "Ratings Disabled Only")
    .otherwise("No Issue").alias("problem_type"),
    "views"
)

print("DEBUG - problem_stats sample:")
problem_stats.filter(col("problem_type") != "No Issue").show(5)

problem_counts = problem_stats.filter(col("problem_type") != "No Issue").groupBy(
    "category_title", "region", "problem_type"
).agg(
    count("video_id").alias("num_videos")
)


total_window = Window.partitionBy("category_title", "region")

problem_agg = problem_counts.withColumn(
    "total_problems",
    spark_sum(col("num_videos")).over(total_window)
).withColumn(
    "pct_of_local",
    spark_round(
        (col("num_videos").cast("double") / col("total_problems").cast("double")) * 100, 1
    )
)

rank_window = Window.partitionBy("category_title", "region").orderBy(desc("pct_of_local"))

ranked = problem_agg.withColumn(
    "problem_rank", 
    row_number().over(rank_window)
)

query4_result = ranked.filter(col("problem_rank") == 1).select(
    "category_title",
    "region", 
    "problem_type",
    "num_videos",
    "pct_of_local"
).withColumn(
    "pct_of_local", 
    concat(col("pct_of_local").cast("string"), lit("%"))
).orderBy("category_title", "region")

query4_result.show(30, truncate=False)

query4_result.write.mode("overwrite").jdbc(
    pg_url,
    "query4_problem_analysis_proba_2",
    properties=pg_properties
)


# UPIT 5: Koji su tagovi, izdvojeni iz liste tagova u videima, najčešći i najuspešniji prema osnovnim metrikama kao što su broj videa, 
#            prosečni lajkovi, broj regiona i kategorija u kojima se pojavljuju, viral rate i ukupna popularnost, 
#            i kako se rangiraju prema kombinovanom “viral score” pokazatelju?

print("\n UPIT 5: Tag analiza - viral score ranking...")

tags_exploded = golden_df.select(
    "video_id", "likes", "views", "comment_count", "category_title", "region",
    explode(col("tags_list")).alias("tag")
).filter(
    (col("tag").isNotNull()) &
    (length(trim(col("tag"))) > 2) &
    (~col("tag").isin("uncategorized", "no-tags", "unknown"))
).withColumn("tag", lower(trim(col("tag"))))

tags_exploded.createOrReplaceTempView("tags_exploded")

# query5_result = spark.sql("""
# WITH tag_performance_metrics AS (
#     SELECT 
#         tag,
#         COUNT(*) as video_count,
#         COUNT(DISTINCT region) as regions_count,
#         COUNT(DISTINCT category_title) as categories_count,
#         SUM(likes) as total_likes,
#         SUM(views) as total_views,
#         SUM(likes + comment_count) as total_engagement,
#         ROUND(AVG(likes), 0) as avg_likes,
#         ROUND(AVG(likes + comment_count), 0) as avg_engagement,
#         COUNT(CASE WHEN likes > 100000 THEN 1 END) as high_performance_videos,
#         ROUND(
#             COUNT(CASE WHEN likes > 100000 THEN 1 END) * 100.0 / COUNT(*), 2
#         ) as viral_success_rate,
#         (COUNT(*) * 0.3 + AVG(likes) * 0.0001 + 
#          COUNT(CASE WHEN likes > 100000 THEN 1 END) * 50) as viral_score
#     FROM tags_exploded
#     GROUP BY tag
#     HAVING COUNT(*) >= 10
# )
# SELECT 
#     ROW_NUMBER() OVER (ORDER BY viral_score DESC) as overall_rank,
#     tag,
#     video_count,
#     avg_likes,
#     viral_success_rate,
#     regions_count,
#     categories_count,
#     ROUND(viral_score, 0) as viral_score
# FROM tag_performance_metrics
# WHERE viral_score > 100
# ORDER BY viral_score DESC
# LIMIT 30
# """)

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

window_rank = Window.orderBy(desc("viral_score"))

query5_result = tag_performance_metrics.filter(col("viral_score") > 100).select(
    row_number().over(window_rank).alias("overall_rank"),
    "tag",
    "video_count", 
    "avg_likes",
    "viral_success_rate",
    "regions_count",
    "categories_count",
    spark_round("viral_score", 0).alias("viral_score")
).orderBy(desc("viral_score")).limit(30)

print(" TOP 30 tagova UPIT 5:")
query5_result.show(30, truncate=False)

query5_result.write.mode("overwrite").jdbc(
    pg_url,
    "query5_tag_viral_analysis",
    properties=pg_properties
)


#  UPIT 6: Koje tagove, pored osnovne popularnosti, karakteriše najbolja kombinacija viralnog potencijala i tržišne pozicije, 
#            kakav im je dodeljeni "power level" na osnovu uspešnosti u različitim kategorijama i regionima, 
#            koju preporuku za buduću upotrebu zaslužuju na osnovu viral score-a i success rate-a, 
#            i kako se rangiraju globalno kao i unutar svojih power level grupa uz smoothed viral score trend analizu?

print("\n UPIT 6: Napredna tag analiza sa Power Level i preporukama...")

# query6_result = spark.sql("""
# WITH tag_performance AS (
#     SELECT 
#         tag,
#         COUNT(*) as video_count,
#         COUNT(DISTINCT region) as active_regions,
#         COUNT(DISTINCT category_title) as categories_count,
#         ROUND(AVG(likes), 0) as avg_likes,
#         ROUND(AVG(views), 0) as avg_views,
#         COUNT(CASE WHEN likes > 100000 THEN 1 END) as high_performance_videos,
#         ROUND(
#             COUNT(CASE WHEN likes > 100000 THEN 1 END) * 100.0 / COUNT(*), 2
#         ) as viral_success_rate,
#         (COUNT(*) * 0.3 + AVG(likes) * 0.0001 + 
#          COUNT(CASE WHEN likes > 100000 THEN 1 END) * 50) as viral_score
#     FROM tags_exploded
#     GROUP BY tag
#     HAVING COUNT(*) >= 10
# ),
# tag_with_levels AS (
#     SELECT *,
#         CASE 
#             WHEN viral_success_rate > 50 AND video_count > 50 THEN 'MAGIC'
#             WHEN viral_success_rate > 30 AND avg_likes > 500000 THEN 'POWERFUL'
#             WHEN video_count > 100 AND avg_likes > 100000 THEN 'RELIABLE'
#             WHEN viral_success_rate > 20 THEN 'PROMISING'
#             ELSE 'AVERAGE'
#         END as tag_power_level,
#         CASE 
#             WHEN viral_success_rate > 90 THEN 'TOP FREQUENCY'
#             WHEN avg_likes > 1000000 THEN 'TOP PERFORMANCE'
#             WHEN viral_success_rate > 40 THEN 'HIGH SUCCESS'
#             ELSE 'GOOD'
#         END as tip_taga,
#         CASE 
#             WHEN viral_success_rate > 50 AND video_count > 50 THEN 'UST USE!'
#             WHEN viral_success_rate > 30 AND avg_likes > 500000 THEN 'HIGHLY RECOMMENDED'
#             WHEN video_count > 100 AND avg_likes > 100000 THEN 'SAFE CHOICE'
#             WHEN viral_success_rate > 20 THEN 'EMERGING'
#             ELSE 'CONSIDER'
#         END as preporuka
#     FROM tag_performance
# )
# SELECT 
#     tag,
#     active_regions,
#     categories_count,
#     tag_power_level,
#     ROUND(viral_score, 0) as viral_score,
#     tip_taga,
#     preporuka,
#     viral_success_rate,
#     RANK() OVER (ORDER BY viral_score DESC) as global_rank,
#     RANK() OVER (PARTITION BY tag_power_level ORDER BY viral_score DESC) as rank_within_power_level,
#     ROUND(PERCENT_RANK() OVER (ORDER BY viral_success_rate) * 100, 1) as success_percentile,
#     ROUND(
#         AVG(viral_score) OVER (
#             ORDER BY viral_score DESC 
#             ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
#         ), 0
#     ) as smoothed_viral_score,
#     CASE 
#         WHEN PERCENT_RANK() OVER (ORDER BY viral_success_rate) >= 0.90 THEN 'TOP 10%'
#         WHEN PERCENT_RANK() OVER (ORDER BY viral_success_rate) >= 0.75 THEN 'TOP 25%'
#         WHEN PERCENT_RANK() OVER (ORDER BY viral_success_rate) >= 0.50 THEN 'TOP 50%'
#         ELSE 'STANDARD'
#     END as market_position
# FROM tag_with_levels
# WHERE viral_score > 100
# ORDER BY viral_score DESC
# LIMIT 30
# """)


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

# CTE 2: tag_with_levels
tag_with_levels = tag_performance.withColumn(
    "tag_power_level",
    when((col("viral_success_rate") > 50) & (col("video_count") > 50), "MAGIC")
    .when((col("viral_success_rate") > 30) & (col("avg_likes") > 500000), "POWERFUL")
    .when((col("video_count") > 100) & (col("avg_likes") > 100000), "RELIABLE")
    .when(col("viral_success_rate") > 20, "PROMISING")
    .otherwise("AVERAGE")
).withColumn(
    "tip_taga",
    when(col("viral_success_rate") > 90, "TOP FREQUENCY")
    .when(col("avg_likes") > 1000000, "TOP PERFORMANCE")
    .when(col("viral_success_rate") > 40, "HIGH SUCCESS")
    .otherwise("GOOD")
).withColumn(
    "recommendation",
    when((col("viral_success_rate") > 50) & (col("video_count") > 50), "MUST USE")
    .when((col("viral_success_rate") > 30) & (col("avg_likes") > 500000), "HIGHLY RECOMMENDED")
    .when((col("video_count") > 100) & (col("avg_likes") > 100000), "SAFE CHOICE")
    .when(col("viral_success_rate") > 20, "EMERGING")
    .otherwise("CONSIDER")
)

# Window funkcije
global_rank_window = Window.orderBy(desc("viral_score"))
power_level_rank_window = Window.partitionBy("tag_power_level").orderBy(desc("viral_score"))
success_percentile_window = Window.orderBy("viral_success_rate")
smoothed_window = Window.orderBy(desc("viral_score")).rowsBetween(-2, 2)

# Finalni rezultat
query6_result = tag_with_levels.filter(col("viral_score") > 100).select(
    "tag",
    "active_regions",
    "categories_count", 
    "tag_power_level",
    spark_round("viral_score", 0).alias("viral_score"),
    "tip_taga",
    "recommendation",
    "viral_success_rate",
    rank().over(global_rank_window).alias("global_rank"),
    rank().over(power_level_rank_window).alias("rank_within_power_level"),
    spark_round(percent_rank().over(success_percentile_window) * 100, 1).alias("success_percentile"),
    spark_round(
        avg("viral_score").over(smoothed_window), 0
    ).alias("smoothed_viral_score")
).withColumn(
    "market_position",
    when(percent_rank().over(success_percentile_window) >= 0.90, "TOP 10%")
    .when(percent_rank().over(success_percentile_window) >= 0.75, "TOP 25%")
    .when(percent_rank().over(success_percentile_window) >= 0.50, "TOP 50%")
    .otherwise("STANDARD")
).orderBy(desc("viral_score")).limit(30)


print(" TOP 30 naprednih tag analiza UPIT 6:")
query6_result.show(30, truncate=False)

query6_result.write.mode("overwrite").jdbc(
    pg_url,
    "query6_advanced_tag_recommendations",
    properties=pg_properties
)


#  UPIT 7: Koji YouTube kanali u određenim kategorijama najbrže postižu viralni status i kako im se menja dinamiku popularnosti kroz vreme?

print("\n UPIT 7: YouTube kanali sa najbržom viralizacijom...")

# query7_result = spark.sql("""
# WITH viral_timeline AS (
#     SELECT 
#         channel_title,
#         category_title,
#         video_id,
#         trending_full_date,
#         views,
#         likes,
#         publish_date,
#         datediff(trending_full_date, publish_date) as days_to_trending,
#         RANK() OVER (
#             PARTITION BY channel_title 
#             ORDER BY datediff(trending_full_date, publish_date)
#         ) as speed_rank_in_channel,
#         AVG(views) OVER (
#             PARTITION BY channel_title
#             ORDER BY trending_full_date
#             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
#         ) as channel_momentum_7d
#     FROM youtube_data
#     WHERE publish_date IS NOT NULL 
#       AND trending_full_date IS NOT NULL
#       AND datediff(trending_full_date, publish_date) >= 0
# )
# SELECT 
#     channel_title,
#     category_title,
#     COUNT(*) as total_viral_videos,
#     ROUND(AVG(days_to_trending), 1) as avg_days_to_viral,
#     ROUND(AVG(channel_momentum_7d), 0) as avg_momentum,
#     ROUND(
#         COUNT(CASE WHEN days_to_trending <= 3 THEN 1 END) * 100.0 / COUNT(*), 1
#     ) as fast_viral_percentage
# FROM viral_timeline
# WHERE days_to_trending BETWEEN 0 AND 30
# GROUP BY channel_title, category_title
# HAVING COUNT(*) >= 5
# ORDER BY fast_viral_percentage DESC, avg_momentum DESC
# LIMIT 20
# """)

speed_rank_window = Window.partitionBy("channel_title").orderBy("days_to_trending")
momentum_window = Window.partitionBy("channel_title").orderBy("trending_full_date").rowsBetween(-6, 0)

viral_timeline = golden_df.select(
    "channel_title",
    "category_title", 
    "video_id",
    "trending_full_date",
    "views",
    "likes",
    "publish_date"
).filter(
    (col("publish_date").isNotNull()) &
    (col("trending_full_date").isNotNull())
).withColumn(
    "days_to_trending",
    datediff(col("trending_full_date"), col("publish_date"))
).filter(
    col("days_to_trending") >= 0
).withColumn(
    "speed_rank_in_channel",
    rank().over(speed_rank_window)
).withColumn(
    "channel_momentum_7d",
    avg("views").over(momentum_window)
)

# Finalni rezultat
query7_result = viral_timeline.filter(
    (col("days_to_trending") >= 0) & (col("days_to_trending") <= 30)
).groupBy("channel_title", "category_title").agg(
    count("*").alias("total_viral_videos"),
    spark_round(avg("days_to_trending"), 1).alias("avg_days_to_viral"),
    spark_round(avg("channel_momentum_7d"), 0).alias("avg_momentum"),
    spark_round(
        count(when(col("days_to_trending") <= 3, 1)) * 100.0 / count("*"), 1
    ).alias("fast_viral_percentage")
).filter(
    col("total_viral_videos") >= 5
).orderBy(
    desc("fast_viral_percentage"), desc("avg_momentum")
).limit(20)

print(" TOP 20 najbrži viralni kanali UPIT 7:")
query7_result.show(20, truncate=False)

query7_result.write.mode("overwrite").jdbc(
    pg_url,
    "query7_fastest_viral_channels",
    properties=pg_properties
)


# UPIT 8: Koja kombinacija dužine opisa videa i kvaliteta thumbnail-a donosi najbolje performanse 
#            (najviše pregleda i lajkova) za svaku kategoriju YouTube sadržaja?

print("\n UPIT 8: Optimalna kombinacija opisa i thumbnail kvaliteta...")

# query8_result = spark.sql("""
# SET max_parallel_workers_per_gather = 0
# SET parallel_tuple_cost = 1000000    
# WITH content_analysis AS (
#     SELECT 
#         video_id,
#         category_title,
#         region,
#         views,
#         likes,
#         comment_count,
#         CASE 
#             WHEN LENGTH(description) = 0 THEN 'No Description'
#             WHEN LENGTH(description) < 100 THEN 'Very Short'
#             WHEN LENGTH(description) < 300 THEN 'Short'
#             WHEN LENGTH(description) < 1000 THEN 'Medium'
#             WHEN LENGTH(description) < 2000 THEN 'Long'
#             ELSE 'Very Long'
#         END as description_length_category,
#         CASE 
#             WHEN thumbnail_link LIKE '%maxresdefault%' THEN 'High Quality'
#             WHEN thumbnail_link LIKE '%hqdefault%' THEN 'Medium Quality'
#             ELSE 'Standard Quality'
#         END as thumbnail_quality,
#         LENGTH(description) as desc_length
#     FROM youtube_data
#     WHERE assignable = true
# )
# SELECT 
#     category_title,
#     description_length_category,
#     thumbnail_quality,
#     COUNT(*) as video_count,
#     ROUND(AVG(views), 0) as avg_views,
#     ROUND(AVG(likes), 0) as avg_likes,
#     RANK() OVER (
#         PARTITION BY category_title 
#         ORDER BY AVG(views) DESC
#     ) as performance_rank,
#     PERCENT_RANK() OVER (ORDER BY AVG(views)) as performance_percentile
# FROM content_analysis
# GROUP BY category_title, description_length_category, thumbnail_quality
# HAVING COUNT(*) >= 10
# ORDER BY category_title, performance_rank
# """)

content_analysis = golden_df.select(
    "video_id",
    "category_title",
    "region", 
    "views",
    "likes",
    "comment_count",
    "description",
    "thumbnail_link",
    "assignable"
).filter(col("assignable") == True).withColumn(
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
).withColumn(
    "desc_length", 
    length(col("description"))
)

# Agregacija prvo
query8_aggregated = content_analysis.groupBy(
    "category_title", "description_length_category", "thumbnail_quality"
).agg(
    count("*").alias("video_count"),
    spark_round(avg("views"), 0).alias("avg_views"),
    spark_round(avg("likes"), 0).alias("avg_likes")
).filter(col("video_count") >= 10)

# Window funkcije za UPIT 8 - koriste avg_views kolonu
performance_rank_window = Window.partitionBy("category_title").orderBy(desc("avg_views"))
performance_percentile_window = Window.orderBy("avg_views")

query8_result = query8_aggregated.withColumn(
    "performance_rank",
    rank().over(performance_rank_window)
).withColumn(
    "performance_percentile", 
    percent_rank().over(performance_percentile_window)
).orderBy("category_title", "performance_rank")


print(" Najbolje kombinacije opisa/thumbnail UPIT 8:")
query8_result.show(40, truncate=False)

query8_result.write.mode("overwrite").jdbc(
    pg_url,
    "query8_content_optimization",
    properties=pg_properties
)

#  UPIT 9: Za svaku kategoriju sadržaja i geografski region, koji meseci u godini predstavljaju optimalno vreme za lansiranje YouTube videa
#            koji će imati najveću šansu za uspeh, rangiran po sezonskoj popularnosti i trendu gledanosti?

print("\n UPIT 9: Optimalno vreme lansiranje po kategoriji i regionu...")

# query9_result = spark.sql("""
# WITH seasonal_patterns AS (
#     SELECT 
#         category_title,
#         region,
#         trending_month,
#         trending_year,
#         COUNT(*) as videos_count,
#         AVG(views) as avg_views,
#         AVG(likes) as avg_engagement,
#         LAG(AVG(views), 1) OVER (
#             PARTITION BY category_title, region
#             ORDER BY trending_year, trending_month
#         ) as prev_month_views,
#         AVG(COUNT(*)) OVER (
#             PARTITION BY category_title, region
#             ORDER BY trending_year, trending_month
#             ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING
#         ) as seasonal_trend
#     FROM youtube_data
#     WHERE assignable = true
#       AND trending_month IS NOT NULL
#       AND trending_year IS NOT NULL
#     GROUP BY category_title, region, trending_month, trending_year
# ),
# seasonal_patterns_with_avg AS (
#     SELECT *,
#            AVG(seasonal_trend) OVER (PARTITION BY category_title, region) as avg_seasonal_trend_cat_region
#     FROM seasonal_patterns
# )
# SELECT 
#     category_title,
#     region,
#     trending_month,
#     ROUND(AVG(seasonal_trend), 1) as trend_strength,
#     COALESCE(
#         CASE 
#             WHEN AVG(prev_month_views) > 0 THEN
#                 ROUND(((AVG(avg_views) - AVG(prev_month_views)) / AVG(prev_month_views)) * 100, 1)
#             ELSE 0.0
#         END, 0.0
#     ) as mom_growth_pct,
#     CASE 
#         WHEN AVG(seasonal_trend) > avg_seasonal_trend_cat_region * 1.2 THEN 'OPTIMAL LAUNCH TIME'
#         WHEN AVG(seasonal_trend) > avg_seasonal_trend_cat_region THEN 'GOOD TIME'
#         ELSE 'AVOID'
#     END as launch_recommendation,
#     CASE 
#         WHEN SUM(videos_count) >= 50 THEN 'HIGH CONFIDENCE'
#         WHEN SUM(videos_count) >= 20 THEN 'MEDIUM CONFIDENCE'
#         WHEN SUM(videos_count) >= 10 THEN 'LOW CONFIDENCE'
#         ELSE 'VERY LOW CONFIDENCE'
#     END as data_confidence,
#     RANK() OVER (
#         PARTITION BY category_title, region 
#         ORDER BY AVG(seasonal_trend) DESC
#     ) as month_rank
# FROM seasonal_patterns_with_avg
# GROUP BY category_title, region, trending_month, avg_seasonal_trend_cat_region
# HAVING COUNT(*) >= 1 AND SUM(videos_count) >= 15
# ORDER BY category_title, region, data_confidence DESC, month_rank
# """)

seasonal_window = Window.partitionBy("category_title", "region").orderBy("trending_year", "trending_month")
seasonal_trend_window = Window.partitionBy("category_title", "region").orderBy("trending_year", "trending_month").rowsBetween(-2, 1)

seasonal_patterns = golden_df.filter(
    (col("assignable") == True) &
    (col("trending_month").isNotNull()) &
    (col("trending_year").isNotNull())
).groupBy("category_title", "region", "trending_month", "trending_year").agg(
    count("*").alias("videos_count"),
    avg("views").alias("avg_views"),
    avg("likes").alias("avg_engagement")
).withColumn(
    "prev_month_views",
    lag(col("avg_views"), 1).over(seasonal_window)
).withColumn(
    "seasonal_trend",
    avg(col("videos_count")).over(seasonal_trend_window)
)

avg_seasonal_window = Window.partitionBy("category_title", "region")
seasonal_patterns_with_avg = seasonal_patterns.withColumn(
    "avg_seasonal_trend_cat_region",
    avg("seasonal_trend").over(avg_seasonal_window)
)

query9_aggregated = seasonal_patterns_with_avg.groupBy(
    "category_title", "region", "trending_month", "avg_seasonal_trend_cat_region"
).agg(
    spark_round(avg("seasonal_trend"), 1).alias("trend_strength"),
    coalesce(
        when(avg("prev_month_views") > 0,
            spark_round(((avg("avg_views") - avg("prev_month_views")) / avg("prev_month_views")) * 100, 1)
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
    "month_rank",
    rank().over(month_rank_window)
).orderBy("category_title", "region", desc("data_confidence"), "month_rank").select(
    "category_title",
    "region",
    "trending_month",
    "trend_strength",
    "mom_growth_pct",
    "launch_recommendation",
    "data_confidence",
    "month_rank"
)


print(" Optimalno vreme lansiranja UPIT 9:")
query9_result.show(50, truncate=False)

query9_result.write.mode("overwrite").jdbc(
    pg_url,
    "query9_optimal_launch_timing",
    properties=pg_properties
)

#  UPIT 10: Koji kanal ima koji najgledaniji video preko milijardu pregleda i u kojoj zemlji?

print("\n UPIT 10: Najgledaniji video po kanalu sa regionom...")

# query10_result = spark.sql("""
# WITH channel_top_videos AS (
#     SELECT 
#         channel_title,
#         video_title,
#         views,
#         region,
#         category_title,
#         publish_year,
#         trending_year,
#         ROW_NUMBER() OVER (PARTITION BY channel_title ORDER BY views DESC) as rn,
#         AVG(views) OVER (PARTITION BY channel_title) as avg_views_per_channel
#     FROM youtube_data
# )
# SELECT 
#     channel_title,
#     video_title as top_video,
#     category_title as top_video_category,
#     views as max_views,
#     region as top_region,
#     publish_year,
#     ROUND(avg_views_per_channel, 0) as avg_views_per_channel
# FROM channel_top_videos 
# WHERE rn = 1 AND views >= 100000000
# ORDER BY views DESC
# """)

channel_window = Window.partitionBy("channel_title").orderBy(desc("views"))
avg_channel_window = Window.partitionBy("channel_title")

channel_top_videos = golden_df.select(
    "channel_title",
    "video_title",
    "views",
    "region", 
    "category_title",
    "publish_year",
    "trending_year"
).withColumn(
    "rn",
    row_number().over(channel_window)
).withColumn(
    "avg_views_per_channel",
    avg("views").over(avg_channel_window)
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
    pg_url,
    "query10_top_channels_mega_hits",
    properties=pg_properties
)


# # DODATNI UPITI


# # BONUS UPIT 1: Ukupna statistika po regionima
# print("\n BONUS UPIT 1: Executive Summary po regionima...")

# bonus1_result = spark.sql("""
# SELECT 
#     region,
#     COUNT(DISTINCT video_id) as total_videos,
#     COUNT(DISTINCT channel_title) as unique_channels,
#     COUNT(DISTINCT category_title) as categories_covered,
#     ROUND(AVG(views), 0) as avg_views_per_video,
#     ROUND(AVG(likes), 0) as avg_likes_per_video,
#     MAX(views) as highest_views,
#     SUM(views) as total_views_region,
#     ROUND(AVG(likes) / NULLIF(AVG(dislikes), 0), 2) as avg_like_dislike_ratio,
#     COUNT(CASE WHEN comments_disabled = true THEN 1 END) as videos_comments_disabled,
#     ROUND(COUNT(CASE WHEN comments_disabled = true THEN 1 END) * 100.0 / COUNT(*), 1) as pct_comments_disabled
# FROM youtube_data
# GROUP BY region
# ORDER BY total_views_region DESC
# """)

# bonus1_result.show(20, truncate=False)
# # bonus1_result.coalesce(1).write.mode("overwrite").parquet(
# #     "hdfs://namenode:9000/storage/hdfs/curated/bonus1_regional_executive_summary"
# # )

# bonus1_result.write.mode("overwrite").jdbc(
#     pg_url,
#     "bonus1_regional_executive_summary",
#     properties=pg_properties
# )

# # BONUS UPIT 2: Kategorial Performance Matrix
# print("\n BONUS UPIT 2: Performance Matrix po kategorijama...")

# bonus2_result = spark.sql("""
# WITH category_metrics AS (
#     SELECT 
#         category_title,
#         COUNT(DISTINCT video_id) as total_videos,
#         COUNT(DISTINCT channel_title) as unique_channels,
#         COUNT(DISTINCT region) as regions_present,
#         ROUND(AVG(views), 0) as avg_views,
#         ROUND(AVG(likes), 0) as avg_likes,
#         ROUND(AVG(comment_count), 0) as avg_comments,
#         ROUND(STDDEV(views), 0) as views_volatility,
#         COUNT(CASE WHEN views > 1000000 THEN 1 END) as million_view_videos,
#         ROUND(COUNT(CASE WHEN views > 1000000 THEN 1 END) * 100.0 / COUNT(*), 1) as viral_rate,
#         PERCENTILE_APPROX(views, 0.5) as median_views,
#         PERCENTILE_APPROX(likes, 0.5) as median_likes
#     FROM youtube_data
#     WHERE assignable = true
#     GROUP BY category_title
# ),
# ranked_categories AS (
#     SELECT *,
#         RANK() OVER (ORDER BY avg_views DESC) as views_rank,
#         RANK() OVER (ORDER BY viral_rate DESC) as viral_rank,
#         RANK() OVER (ORDER BY total_videos DESC) as volume_rank,
#         CASE 
#             WHEN viral_rate > 15 AND avg_views > 500000 THEN 'HIGH PERFORMANCE'
#             WHEN viral_rate > 10 OR avg_views > 300000 THEN 'MEDIUM PERFORMANCE'
#             ELSE 'STANDARD PERFORMANCE'
#         END as performance_tier
#     FROM category_metrics
# )
# SELECT 
#     category_title,
#     total_videos,
#     unique_channels,
#     regions_present,
#     avg_views,
#     viral_rate,
#     performance_tier,
#     views_rank,
#     viral_rank
# FROM ranked_categories
# ORDER BY views_rank
# """)

# bonus2_result.show(25, truncate=False)
# bonus2_result.coalesce(1).write.mode("overwrite").parquet(
#     "hdfs://namenode:9000/storage/hdfs/curated/bonus2_category_performance_matrix"
# )

# # bonus2_result.write.mode("overwrite").jdbc(
# #     pg_url,
# #     "bonus2_category_performance_matrix",
# #     properties=pg_properties
# # )

# # BONUS UPIT 3: Vremenski trendovi i dinamika
# print("\n BONUS UPIT 3: Vremenski trendovi i growth analiza...")

# bonus3_result = spark.sql("""
# WITH monthly_trends AS (
#     SELECT 
#         trending_year,
#         trending_month,
#         category_title,
#         COUNT(*) as monthly_video_count,
#         ROUND(AVG(views), 0) as avg_monthly_views,
#         ROUND(AVG(likes), 0) as avg_monthly_likes,
#         COUNT(DISTINCT channel_title) as active_channels
#     FROM youtube_data
#     WHERE assignable = true 
#       AND trending_month IS NOT NULL 
#       AND trending_year IS NOT NULL
#     GROUP BY trending_year, trending_month, category_title
# ),
# growth_analysis AS (
#     SELECT *,
#         LAG(monthly_video_count) OVER (
#             PARTITION BY category_title 
#             ORDER BY trending_year, trending_month
#         ) as prev_month_count,
#         LAG(avg_monthly_views) OVER (
#             PARTITION BY category_title 
#             ORDER BY trending_year, trending_month
#         ) as prev_month_views,
#         AVG(monthly_video_count) OVER (
#             PARTITION BY category_title
#             ORDER BY trending_year, trending_month
#             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
#         ) as rolling_3m_count
#     FROM monthly_trends
# )
# SELECT 
#     trending_year,
#     trending_month,
#     category_title,
#     monthly_video_count,
#     avg_monthly_views,
#     active_channels,
#     CASE 
#         WHEN prev_month_count IS NOT NULL THEN
#             ROUND(((monthly_video_count - prev_month_count) * 100.0 / prev_month_count), 1)
#         ELSE NULL
#     END as mom_growth_pct,
#     ROUND(rolling_3m_count, 1) as rolling_3m_avg
# FROM growth_analysis
# WHERE trending_year IS NOT NULL AND trending_month IS NOT NULL
# ORDER BY trending_year DESC, trending_month DESC, category_title
# """)

# bonus3_result.show(40, truncate=False)
# # bonus3_result.coalesce(1).write.mode("overwrite").parquet(
# #     "hdfs://namenode:9000/storage/hdfs/curated/bonus3_temporal_trends_growth"
# # )

# bonus3_result.write.mode("overwrite").jdbc(
#     pg_url,
#     "bonus3_temporal_trends_growth",
#     properties=pg_properties
# )

# # AGREGACIJA I FINALNI IZVJEŠTAJ

# print("\n KREIRANJE FINALNOG IZVJEŠTAJA...")

# # Kreiraj sažetak svih analiza
# summary_report = spark.sql("""
# SELECT 
#     'Total Videos Analyzed' as metric,
#     CAST(COUNT(*) AS STRING) as value,
#     '' as details
# FROM youtube_data
# UNION ALL
# SELECT 
#     'Total Categories' as metric,
#     CAST(COUNT(DISTINCT category_title) AS STRING) as value,
#     '' as details
# FROM youtube_data
# WHERE assignable = true
# UNION ALL
# SELECT 
#     'Total Regions' as metric,
#     CAST(COUNT(DISTINCT region) AS STRING) as value,
#     '' as details
# FROM youtube_data
# UNION ALL
# SELECT 
#     'Date Range' as metric,
#     CAST(COUNT(DISTINCT trending_full_date) AS STRING) as value,
#     CONCAT(MIN(trending_full_date), ' to ', MAX(trending_full_date)) as details
# FROM youtube_data
# WHERE trending_full_date IS NOT NULL
# UNION ALL
# SELECT 
#     'Total Channels' as metric,
#     CAST(COUNT(DISTINCT channel_title) AS STRING) as value,
#     '' as details
# FROM youtube_data
# UNION ALL
# SELECT 
#     'Average Views Per Video' as metric,
#     CAST(ROUND(AVG(views), 0) AS STRING) as value,
#     '' as details
# FROM youtube_data
# """)

# print(" FINALNI IZVJEŠTAJ:")
# summary_report.show(truncate=False)

# #Sačuvaj finalni izvještaj
# # summary_report.coalesce(1).write.mode("overwrite").json(
# #     "hdfs://namenode:9000/storage/hdfs/curated/final_batch_report"
# # )

# summary_report.write.mode("overwrite").jdbc(
#     pg_url,
#     "final_batch_report",
#     properties=pg_properties
# )

print("\n BATCH OBRADA ZAVRŠENA!")

spark.stop()