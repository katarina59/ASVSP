# spark_batch_curated.py - Complete Pure Data Lake Batch Processing
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col, length, trim, lower, explode # type: ignore
from pyspark.sql.types import * # type: ignore

# =====================================================
# 🔥 KREIRANJE SPARK SESIJE
# =====================================================
spark = SparkSession.builder \
    .appName("YouTube curated - Complete Pure Data Lake Processing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()


# =====================================================
# 📊 UČITAVANJE PODATAKA IZ DATA LAKE
# =====================================================
print("🔄 Učitavanje podataka iz Data Lake (HDFS)...")

# Učitaj golden dataset iz parquet formata
golden_df = spark.read.parquet("hdfs://namenode:9000/storage/hdfs/processed/golden_dataset")

print(f"✅ Učitano {golden_df.count()} rekorda iz Data Lake")

# =====================================================
# 🏗️ KREIRANJE ANALYTICAL VIEWS (IN-MEMORY)
# =====================================================
print("🏗️ Kreiranje analytical views u Spark sesiji...")

# Kreiraj view direktno od golden dataset-a
golden_df.createOrReplaceTempView("youtube_data")

print("✅ Analytical views kreirani u Spark sesiji")

# =====================================================
# 📈 BATCH ANALITIKA - DIREKTNO NAD DATA LAKE
# =====================================================


# =====================================================
# 💾 DODATNI UPITI - EXECUTIVE SUMMARY I INSIGHTS
# =====================================================

# BONUS UPIT 1: Ukupna statistika po regionima
print("\n📈 BONUS UPIT 1: Executive Summary po regionima...")

bonus1_result = spark.sql("""
SELECT 
    region,
    COUNT(DISTINCT video_id) as total_videos,
    COUNT(DISTINCT channel_title) as unique_channels,
    COUNT(DISTINCT category_title) as categories_covered,
    ROUND(AVG(views), 0) as avg_views_per_video,
    ROUND(AVG(likes), 0) as avg_likes_per_video,
    MAX(views) as highest_views,
    SUM(views) as total_views_region,
    ROUND(AVG(likes) / NULLIF(AVG(dislikes), 0), 2) as avg_like_dislike_ratio,
    COUNT(CASE WHEN comments_disabled = true THEN 1 END) as videos_comments_disabled,
    ROUND(COUNT(CASE WHEN comments_disabled = true THEN 1 END) * 100.0 / COUNT(*), 1) as pct_comments_disabled
FROM youtube_data
GROUP BY region
ORDER BY total_views_region DESC
""")

bonus1_result.show(20, truncate=False)
bonus1_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/bonus1_regional_executive_summary"
)

# BONUS UPIT 2: Kategorial Performance Matrix
print("\n📈 BONUS UPIT 2: Performance Matrix po kategorijama...")

bonus2_result = spark.sql("""
WITH category_metrics AS (
    SELECT 
        category_title,
        COUNT(DISTINCT video_id) as total_videos,
        COUNT(DISTINCT channel_title) as unique_channels,
        COUNT(DISTINCT region) as regions_present,
        ROUND(AVG(views), 0) as avg_views,
        ROUND(AVG(likes), 0) as avg_likes,
        ROUND(AVG(comment_count), 0) as avg_comments,
        ROUND(STDDEV(views), 0) as views_volatility,
        COUNT(CASE WHEN views > 1000000 THEN 1 END) as million_view_videos,
        ROUND(COUNT(CASE WHEN views > 1000000 THEN 1 END) * 100.0 / COUNT(*), 1) as viral_rate,
        PERCENTILE_APPROX(views, 0.5) as median_views,
        PERCENTILE_APPROX(likes, 0.5) as median_likes
    FROM youtube_data
    WHERE assignable = true
    GROUP BY category_title
),
ranked_categories AS (
    SELECT *,
        RANK() OVER (ORDER BY avg_views DESC) as views_rank,
        RANK() OVER (ORDER BY viral_rate DESC) as viral_rank,
        RANK() OVER (ORDER BY total_videos DESC) as volume_rank,
        CASE 
            WHEN viral_rate > 15 AND avg_views > 500000 THEN 'HIGH PERFORMANCE'
            WHEN viral_rate > 10 OR avg_views > 300000 THEN 'MEDIUM PERFORMANCE'
            ELSE 'STANDARD PERFORMANCE'
        END as performance_tier
    FROM category_metrics
)
SELECT 
    category_title,
    total_videos,
    unique_channels,
    regions_present,
    avg_views,
    viral_rate,
    performance_tier,
    views_rank,
    viral_rank
FROM ranked_categories
ORDER BY views_rank
""")

bonus2_result.show(25, truncate=False)
bonus2_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/bonus2_category_performance_matrix"
)

# BONUS UPIT 3: Vremenski trendovi i dinamika
print("\n📈 BONUS UPIT 3: Vremenski trendovi i growth analiza...")

bonus3_result = spark.sql("""
WITH monthly_trends AS (
    SELECT 
        trending_year,
        trending_month,
        category_title,
        COUNT(*) as monthly_video_count,
        ROUND(AVG(views), 0) as avg_monthly_views,
        ROUND(AVG(likes), 0) as avg_monthly_likes,
        COUNT(DISTINCT channel_title) as active_channels
    FROM youtube_data
    WHERE assignable = true 
      AND trending_month IS NOT NULL 
      AND trending_year IS NOT NULL
    GROUP BY trending_year, trending_month, category_title
),
growth_analysis AS (
    SELECT *,
        LAG(monthly_video_count) OVER (
            PARTITION BY category_title 
            ORDER BY trending_year, trending_month
        ) as prev_month_count,
        LAG(avg_monthly_views) OVER (
            PARTITION BY category_title 
            ORDER BY trending_year, trending_month
        ) as prev_month_views,
        AVG(monthly_video_count) OVER (
            PARTITION BY category_title
            ORDER BY trending_year, trending_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as rolling_3m_count
    FROM monthly_trends
)
SELECT 
    trending_year,
    trending_month,
    category_title,
    monthly_video_count,
    avg_monthly_views,
    active_channels,
    CASE 
        WHEN prev_month_count IS NOT NULL THEN
            ROUND(((monthly_video_count - prev_month_count) * 100.0 / prev_month_count), 1)
        ELSE NULL
    END as mom_growth_pct,
    ROUND(rolling_3m_count, 1) as rolling_3m_avg
FROM growth_analysis
WHERE trending_year IS NOT NULL AND trending_month IS NOT NULL
ORDER BY trending_year DESC, trending_month DESC, category_title
""")

bonus3_result.show(40, truncate=False)
bonus3_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/bonus3_temporal_trends_growth"
)

# =====================================================
# 💾 AGREGACIJA I FINALNI IZVJEŠTAJ
# =====================================================
print("\n📋 KREIRANJE FINALNOG IZVJEŠTAJA...")

# Kreiraj sažetak svih analiza
summary_report = spark.sql("""
SELECT 
    'Total Videos Analyzed' as metric,
    CAST(COUNT(*) AS STRING) as value,
    '' as details
FROM youtube_data
UNION ALL
SELECT 
    'Total Categories' as metric,
    CAST(COUNT(DISTINCT category_title) AS STRING) as value,
    '' as details
FROM youtube_data
WHERE assignable = true
UNION ALL
SELECT 
    'Total Regions' as metric,
    CAST(COUNT(DISTINCT region) AS STRING) as value,
    '' as details
FROM youtube_data
UNION ALL
SELECT 
    'Date Range' as metric,
    CAST(COUNT(DISTINCT trending_full_date) AS STRING) as value,
    CONCAT(MIN(trending_full_date), ' to ', MAX(trending_full_date)) as details
FROM youtube_data
WHERE trending_full_date IS NOT NULL
UNION ALL
SELECT 
    'Total Channels' as metric,
    CAST(COUNT(DISTINCT channel_title) AS STRING) as value,
    '' as details
FROM youtube_data
UNION ALL
SELECT 
    'Average Views Per Video' as metric,
    CAST(ROUND(AVG(views), 0) AS STRING) as value,
    '' as details
FROM youtube_data
""")

print("📊 FINALNI IZVJEŠTAJ:")
summary_report.show(truncate=False)

# Sačuvaj finalni izvještaj
summary_report.coalesce(1).write.mode("overwrite").json(
    "hdfs://namenode:9000/storage/hdfs/curated/final_batch_report"
)

print("\n✅ BATCH OBRADA ZAVRŠENA!")
print("📁 Svi rezultati sačuvani u: hdfs://namenode:9000/storage/hdfs/curated/")
print("🎯 Korišćen je pure Data Lake pristup direktno nad HDFS podacima")
print("🔄 Svi upiti transformisani iz Star Schema u Data Lake format")

spark.stop()