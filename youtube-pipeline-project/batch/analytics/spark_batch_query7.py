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
# 🔍 UPIT 7: Koji YouTube kanali u određenim kategorijama najbrže postižu viralni status i kako im se menja dinamiku popularnosti kroz vreme?
# =====================================================
print("\n🔍 UPIT 7: YouTube kanali sa najbržom viralizacijom...")

query7_result = spark.sql("""
WITH viral_timeline AS (
    SELECT 
        channel_title,
        category_title,
        video_id,
        trending_full_date,
        views,
        likes,
        publish_date,
        datediff(trending_full_date, publish_date) as days_to_trending,
        RANK() OVER (
            PARTITION BY channel_title 
            ORDER BY datediff(trending_full_date, publish_date)
        ) as speed_rank_in_channel,
        AVG(views) OVER (
            PARTITION BY channel_title
            ORDER BY trending_full_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as channel_momentum_7d
    FROM youtube_data
    WHERE publish_date IS NOT NULL 
      AND trending_full_date IS NOT NULL
      AND datediff(trending_full_date, publish_date) >= 0
)
SELECT 
    channel_title,
    category_title,
    COUNT(*) as total_viral_videos,
    ROUND(AVG(days_to_trending), 1) as avg_days_to_viral,
    ROUND(AVG(channel_momentum_7d), 0) as avg_momentum,
    ROUND(
        COUNT(CASE WHEN days_to_trending <= 3 THEN 1 END) * 100.0 / COUNT(*), 1
    ) as fast_viral_percentage
FROM viral_timeline
WHERE days_to_trending BETWEEN 0 AND 30
GROUP BY channel_title, category_title
HAVING COUNT(*) >= 5
ORDER BY fast_viral_percentage DESC, avg_momentum DESC
LIMIT 20
""")

print("📊 TOP 20 najbrži viralni kanali UPIT 7:")
query7_result.show(20, truncate=False)
query7_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/query7_fastest_viral_channels"
)

print("✅ UPIT 7 ZAVRŠEN!")
spark.stop()