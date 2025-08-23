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
# 🔍 UPIT 6: Koje tagove, pored osnovne popularnosti, karakteriše najbolja kombinacija viralnog potencijala i tržišne pozicije, 
#            kakav im je dodeljeni "power level" na osnovu uspešnosti u različitim kategorijama i regionima, 
#            koju preporuku za buduću upotrebu zaslužuju na osnovu viral score-a i success rate-a, 
#            i kako se rangiraju globalno kao i unutar svojih power level grupa uz smoothed viral score trend analizu?
# =====================================================
# Kreiraj eksplodiranu verziju tagova
tags_exploded = golden_df.select(
    "video_id", "likes", "views", "comment_count", "category_title", "region",
    explode(col("tags_list")).alias("tag")
).filter(
    (col("tag").isNotNull()) &
    (length(trim(col("tag"))) > 2) &
    (~col("tag").isin("uncategorized", "no-tags", "unknown"))
).withColumn("tag", lower(trim(col("tag"))))

tags_exploded.createOrReplaceTempView("tags_exploded")


print("\n🔍 UPIT 6: Napredna tag analiza sa Power Level i preporukama...")

query6_result = spark.sql("""
WITH tag_performance AS (
    SELECT 
        tag,
        COUNT(*) as video_count,
        COUNT(DISTINCT region) as active_regions,
        COUNT(DISTINCT category_title) as categories_count,
        ROUND(AVG(likes), 0) as avg_likes,
        ROUND(AVG(views), 0) as avg_views,
        COUNT(CASE WHEN likes > 100000 THEN 1 END) as high_performance_videos,
        ROUND(
            COUNT(CASE WHEN likes > 100000 THEN 1 END) * 100.0 / COUNT(*), 2
        ) as viral_success_rate,
        (COUNT(*) * 0.3 + AVG(likes) * 0.0001 + 
         COUNT(CASE WHEN likes > 100000 THEN 1 END) * 50) as viral_score
    FROM tags_exploded
    GROUP BY tag
    HAVING COUNT(*) >= 10
),
tag_with_levels AS (
    SELECT *,
        CASE 
            WHEN viral_success_rate > 50 AND video_count > 50 THEN 'MAGIC'
            WHEN viral_success_rate > 30 AND avg_likes > 500000 THEN 'POWERFUL'
            WHEN video_count > 100 AND avg_likes > 100000 THEN 'RELIABLE'
            WHEN viral_success_rate > 20 THEN 'PROMISING'
            ELSE 'AVERAGE'
        END as tag_power_level,
        CASE 
            WHEN viral_success_rate > 90 THEN '🔥 TOP FREQUENCY'
            WHEN avg_likes > 1000000 THEN '⭐ TOP PERFORMANCE'
            WHEN viral_success_rate > 40 THEN '🎯 HIGH SUCCESS'
            ELSE '📊 GOOD'
        END as tip_taga,
        CASE 
            WHEN viral_success_rate > 50 AND video_count > 50 THEN '👑 MUST USE!'
            WHEN viral_success_rate > 30 AND avg_likes > 500000 THEN '💪 HIGHLY RECOMMENDED'
            WHEN video_count > 100 AND avg_likes > 100000 THEN '✅ SAFE CHOICE'
            WHEN viral_success_rate > 20 THEN '🌟 EMERGING'
            ELSE '📈 CONSIDER'
        END as preporuka
    FROM tag_performance
)
SELECT 
    tag,
    active_regions,
    categories_count,
    tag_power_level,
    ROUND(viral_score, 0) as viral_score,
    tip_taga,
    preporuka,
    viral_success_rate,
    RANK() OVER (ORDER BY viral_score DESC) as global_rank,
    RANK() OVER (PARTITION BY tag_power_level ORDER BY viral_score DESC) as rank_within_power_level,
    ROUND(PERCENT_RANK() OVER (ORDER BY viral_success_rate) * 100, 1) as success_percentile,
    ROUND(
        AVG(viral_score) OVER (
            ORDER BY viral_score DESC 
            ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
        ), 0
    ) as smoothed_viral_score,
    CASE 
        WHEN PERCENT_RANK() OVER (ORDER BY viral_success_rate) >= 0.90 THEN '🏆 TOP 10%'
        WHEN PERCENT_RANK() OVER (ORDER BY viral_success_rate) >= 0.75 THEN '🥈 TOP 25%'
        WHEN PERCENT_RANK() OVER (ORDER BY viral_success_rate) >= 0.50 THEN '🥉 TOP 50%'
        ELSE '📊 STANDARD'
    END as market_position
FROM tag_with_levels
WHERE viral_score > 100
ORDER BY viral_score DESC
LIMIT 30
""")

print("📊 TOP 30 naprednih tag analiza UPIT 6:")
query6_result.show(30, truncate=False)
query6_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/query6_advanced_tag_recommendations"
)

print("✅ UPIT 6 ZAVRŠEN!")
spark.stop()