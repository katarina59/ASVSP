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
# 🔍 UPIT 8: Koja kombinacija dužine opisa videa i kvaliteta thumbnail-a donosi najbolje performanse 
#            (najviše pregleda i lajkova) za svaku kategoriju YouTube sadržaja?
# =====================================================
print("\n🔍 UPIT 8: Optimalna kombinacija opisa i thumbnail kvaliteta...")

query8_result = spark.sql("""
SET max_parallel_workers_per_gather = 0
SET parallel_tuple_cost = 1000000  
WITH content_analysis AS (
    SELECT 
        video_id,
        category_title,
        region,
        views,
        likes,
        comment_count,
        CASE 
            WHEN LENGTH(description) = 0 THEN 'No Description'
            WHEN LENGTH(description) < 100 THEN 'Very Short'
            WHEN LENGTH(description) < 300 THEN 'Short'
            WHEN LENGTH(description) < 1000 THEN 'Medium'
            WHEN LENGTH(description) < 2000 THEN 'Long'
            ELSE 'Very Long'
        END as description_length_category,
        CASE 
            WHEN thumbnail_link LIKE '%maxresdefault%' THEN 'High Quality'
            WHEN thumbnail_link LIKE '%hqdefault%' THEN 'Medium Quality'
            ELSE 'Standard Quality'
        END as thumbnail_quality,
        LENGTH(description) as desc_length
    FROM youtube_data
    WHERE assignable = true
)
SELECT 
    category_title,
    description_length_category,
    thumbnail_quality,
    COUNT(*) as video_count,
    ROUND(AVG(views), 0) as avg_views,
    ROUND(AVG(likes), 0) as avg_likes,
    RANK() OVER (
        PARTITION BY category_title 
        ORDER BY AVG(views) DESC
    ) as performance_rank,
    PERCENT_RANK() OVER (ORDER BY AVG(views)) as performance_percentile
FROM content_analysis
GROUP BY category_title, description_length_category, thumbnail_quality
HAVING COUNT(*) >= 10
ORDER BY category_title, performance_rank
""")

print("📊 Najbolje kombinacije opisa/thumbnail UPIT 8:")
query8_result.show(40, truncate=False)
query8_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/query8_content_optimization"
)


print("✅ UPIT 8 ZAVRŠEN!")
spark.stop()