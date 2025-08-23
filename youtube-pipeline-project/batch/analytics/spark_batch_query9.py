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
# 🔍 UPIT 9: Za svaku kategoriju sadržaja i geografski region, koji meseci u godini predstavljaju optimalno vreme za lansiranje YouTube videa
#            koji će imati najveću šansu za uspeh, rangiran po sezonskoj popularnosti i trendu gledanosti?
# =====================================================
print("\n🔍 UPIT 9: Optimalno vreme lansiranje po kategoriji i regionu...")

query9_result = spark.sql("""
WITH seasonal_patterns AS (
    SELECT 
        category_title,
        region,
        trending_month,
        trending_year,
        COUNT(*) as videos_count,
        AVG(views) as avg_views,
        AVG(likes) as avg_engagement,
        LAG(AVG(views), 1) OVER (
            PARTITION BY category_title, region
            ORDER BY trending_year, trending_month
        ) as prev_month_views,
        AVG(COUNT(*)) OVER (
            PARTITION BY category_title, region
            ORDER BY trending_year, trending_month
            ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING
        ) as seasonal_trend
    FROM youtube_data
    WHERE assignable = true
      AND trending_month IS NOT NULL
      AND trending_year IS NOT NULL
    GROUP BY category_title, region, trending_month, trending_year
),
seasonal_patterns_with_avg AS (
    SELECT *,
           AVG(seasonal_trend) OVER (PARTITION BY category_title, region) as avg_seasonal_trend_cat_region
    FROM seasonal_patterns
)
SELECT 
    category_title,
    region,
    trending_month,
    ROUND(AVG(seasonal_trend), 1) as trend_strength,
    COALESCE(
        CASE 
            WHEN AVG(prev_month_views) > 0 THEN
                ROUND(((AVG(avg_views) - AVG(prev_month_views)) / AVG(prev_month_views)) * 100, 1)
            ELSE 0.0
        END, 0.0
    ) as mom_growth_pct,
    CASE 
        WHEN AVG(seasonal_trend) > avg_seasonal_trend_cat_region * 1.2 THEN 'OPTIMAL LAUNCH TIME'
        WHEN AVG(seasonal_trend) > avg_seasonal_trend_cat_region THEN 'GOOD TIME'
        ELSE 'AVOID'
    END as launch_recommendation,
    CASE 
        WHEN SUM(videos_count) >= 50 THEN 'HIGH CONFIDENCE'
        WHEN SUM(videos_count) >= 20 THEN 'MEDIUM CONFIDENCE'
        WHEN SUM(videos_count) >= 10 THEN 'LOW CONFIDENCE'
        ELSE 'VERY LOW CONFIDENCE'
    END as data_confidence,
    RANK() OVER (
        PARTITION BY category_title, region 
        ORDER BY AVG(seasonal_trend) DESC
    ) as month_rank
FROM seasonal_patterns_with_avg
GROUP BY category_title, region, trending_month, avg_seasonal_trend_cat_region
HAVING COUNT(*) >= 1 AND SUM(videos_count) >= 15
ORDER BY category_title, region, data_confidence DESC, month_rank
""")

print("📊 Optimalno vreme lansiranja UPIT 9:")
query9_result.show(50, truncate=False)
query9_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/query9_optimal_launch_timing"
)

print("✅ UPIT 9 ZAVRŠEN!")
spark.stop()