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
# 🔍 UPIT 5: Koji su tagovi, izdvojeni iz liste tagova u videima, najčešći i najuspešniji prema osnovnim metrikama kao što su broj videa, 
#            prosečni lajkovi, broj regiona i kategorija u kojima se pojavljuju, viral rate i ukupna popularnost, 
#            i kako se rangiraju prema kombinovanom “viral score” pokazatelju?
# =====================================================
print("\n🔍 UPIT 5: Tag analiza - viral score ranking...")

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

query5_result = spark.sql("""
WITH tag_performance_metrics AS (
    SELECT 
        tag,
        COUNT(*) as video_count,
        COUNT(DISTINCT region) as regions_count,
        COUNT(DISTINCT category_title) as categories_count,
        SUM(likes) as total_likes,
        SUM(views) as total_views,
        SUM(likes + comment_count) as total_engagement,
        ROUND(AVG(likes), 0) as avg_likes,
        ROUND(AVG(likes + comment_count), 0) as avg_engagement,
        COUNT(CASE WHEN likes > 100000 THEN 1 END) as high_performance_videos,
        ROUND(
            COUNT(CASE WHEN likes > 100000 THEN 1 END) * 100.0 / COUNT(*), 2
        ) as viral_success_rate,
        (COUNT(*) * 0.3 + AVG(likes) * 0.0001 + 
         COUNT(CASE WHEN likes > 100000 THEN 1 END) * 50) as viral_score
    FROM tags_exploded
    GROUP BY tag
    HAVING COUNT(*) >= 10
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY viral_score DESC) as overall_rank,
    tag,
    video_count,
    avg_likes,
    viral_success_rate,
    regions_count,
    categories_count,
    ROUND(viral_score, 0) as viral_score
FROM tag_performance_metrics
WHERE viral_score > 100
ORDER BY viral_score DESC
LIMIT 30
""")

print("📊 TOP 30 tagova UPIT 5:")
query5_result.show(30, truncate=False)
query5_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/query5_tag_viral_analysis"
)

print("✅ UPIT 5 ZAVRŠEN!")
spark.stop()