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
# 🔍 UPIT 3: Koje YouTube kategorije i regioni su top 10% najbržih viralnih videa i istovremeno među top 10% po trajanju na trending listi?
#            Koliko prosečno treba da video dospe na trending i koliko dugo ostaje, i kako se ove kombinacije rangiraju u odnosu na sve ostale?
#            Koji sadržaji su i instant hit i dugotrajni hit, tj. “zlatne kombinacije”?
# =====================================================
print("\n🔍 UPIT 3: Zlatne kombinacije viralnih sadržaja...")

query3_result = spark.sql("""
WITH stats AS (
    SELECT
        category_title AS category,
        region AS region,
        DATEDIFF(MIN(trending_full_date), publish_date) AS days_to_trend,
        COUNT(DISTINCT trending_full_date) AS trend_duration_days
    FROM youtube_data
    WHERE publish_date IS NOT NULL AND trending_full_date IS NOT NULL
    GROUP BY category_title, region, publish_date, video_id
),
aggregated AS (
    SELECT
        category,
        region,
        ROUND(AVG(days_to_trend), 2) AS avg_days_to_trend,
        ROUND(AVG(trend_duration_days), 2) AS avg_trend_days
    FROM stats
    GROUP BY category, region
),
ranked AS (
    SELECT
        category,
        region,
        avg_days_to_trend,
        avg_trend_days,
        PERCENT_RANK() OVER (ORDER BY avg_days_to_trend ASC) AS pct_fastest_to_trend,
        PERCENT_RANK() OVER (ORDER BY avg_trend_days DESC) AS pct_longest_trending
    FROM aggregated
)
SELECT
    category,
    region,
    avg_days_to_trend,
    avg_trend_days,
    ROUND(pct_fastest_to_trend * 100, 2) AS pct_rank_fastest,
    ROUND(pct_longest_trending * 100, 2) AS pct_rank_longest
FROM ranked
WHERE pct_fastest_to_trend <= 0.10 AND pct_longest_trending >= 0.90
ORDER BY pct_fastest_to_trend, pct_longest_trending
""")

print("📊 Rezultati UPIT 3 - Zlatne kombinacije:")
query3_result.show(20, truncate=False)
query3_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/query3_viral_golden_combinations"
)

print("✅ UPIT 3 ZAVRŠEN!")
spark.stop()