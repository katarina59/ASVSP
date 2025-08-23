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
# 🔍 UPIT 4: Koji tip problema je najčešći po kombinaciji kategorije i regiona i koliki procenat problematičnih videa čini?
# =====================================================
print("\n🔍 UPIT 4: Najčešći tip problema po kombinaciji kategorije i regiona...")

query4_result = spark.sql("""
WITH problem_stats AS (
    SELECT
        video_id,
        category_title,
        region,
        CASE 
            WHEN video_error_or_removed THEN 'Removed'
            WHEN comments_disabled = true AND ratings_disabled = true THEN 'All Interactions Disabled'
            WHEN comments_disabled = true THEN 'Comments Disabled Only'
            WHEN ratings_disabled = true THEN 'Ratings Disabled Only'
            ELSE 'No Issue'
        END AS problem_type,
        views
    FROM youtube_data
),
problem_agg AS (
    SELECT
        category_title,
        region,
        problem_type,
        COUNT(video_id) AS num_videos,
        ROUND(
            CAST(COUNT(video_id) AS DOUBLE) / SUM(COUNT(video_id)) OVER (PARTITION BY category_title, region) * 100,
            1
        ) AS pct_of_local
    FROM problem_stats
    WHERE problem_type != 'No Issue'
    GROUP BY category_title, region, problem_type
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY category_title, region ORDER BY pct_of_local DESC) AS problem_rank
    FROM problem_agg
)
SELECT
    category_title,
    region,
    problem_type,
    num_videos,
    pct_of_local || '%' AS pct_of_local
FROM ranked
WHERE problem_rank = 1
ORDER BY category_title, region
""")

print("📊 Rezultati UPIT 4 - Najčešći problemi:")
query4_result.show(30, truncate=False)
query4_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/query4_problem_analysis"
)

print("✅ UPIT 4 ZAVRŠEN!")
spark.stop()