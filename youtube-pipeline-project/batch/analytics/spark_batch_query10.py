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
# 🔍 UPIT 10: Koji kanal ima koji najgledaniji video preko milijardu pregleda i u kojoj zemlji?
# =====================================================
print("\n🔍 UPIT 10: Najgledaniji video po kanalu sa regionom...")

query10_result = spark.sql("""
WITH channel_top_videos AS (
    SELECT 
        channel_title,
        video_title,
        views,
        region,
        category_title,
        publish_year,
        trending_year,
        ROW_NUMBER() OVER (PARTITION BY channel_title ORDER BY views DESC) as rn,
        AVG(views) OVER (PARTITION BY channel_title) as avg_views_per_channel
    FROM youtube_data
)
SELECT 
    channel_title,
    video_title as top_video,
    category_title as top_video_category,
    views as max_views,
    region as top_region,
    publish_year,
    ROUND(avg_views_per_channel, 0) as avg_views_per_channel
FROM channel_top_videos 
WHERE rn = 1 AND views >= 100000000
ORDER BY views DESC
""")

print("📊 TOP kanali sa najvećim hitovima UPIT 10:")
query10_result.show(30, truncate=False)
query10_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/query10_top_channels_mega_hits"
)

print("✅ UPIT 10 ZAVRŠEN!")
spark.stop()