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
# 🔍 UPIT 1: Koja je prosečna gledanost i prosečan broj komentara po kategoriji, regionu i datumu kada su videi postali trending, 
#            i kako se ti proseci razlikuju za videe sa uključenim i onemogućenim komentarima? Koje kategorije i regioni dominiraju po gledanosti, 
#            a kako se trend pregleda menja tokom poslednja tri dana?
# =====================================================
print("\n🔍 UPIT 1: Prosečna gledanost po kategoriji, regionu i komentarima...")

query1_result = spark.sql("""
SELECT 
    category_title,
    region,
    trending_full_date,
    comments_disabled,
    COUNT(*) as video_count,
    ROUND(AVG(views), 2) as avg_views,
    ROUND(AVG(comment_count), 2) as avg_comments,
    RANK() OVER (
        PARTITION BY category_title, region 
        ORDER BY AVG(views) DESC
    ) as views_rank,
    ROUND(
        AVG(AVG(views)) OVER (
            PARTITION BY category_title, region, comments_disabled
            ORDER BY trending_full_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    ) as moving_avg_views_3d                  
FROM youtube_data
WHERE assignable = true
GROUP BY category_title, region, trending_full_date, comments_disabled
ORDER BY category_title, region, trending_full_date, comments_disabled
""")

print("📊 TOP 15 rezultata UPIT 1:")
query1_result.show(15, truncate=False)
query1_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/query1_category_region_analysis"
)

print("✅ UPIT 1 ZAVRŠEN!")
spark.stop()