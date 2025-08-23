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
# 🔍 UPIT 2: Koje kategorije i kanali ostvaruju najveći angažman korisnika?  
#            Koliki je njihov engagement score (ukupan broj lajkova + komentara),
#            kao i da li taj angažman dolazi iz pozitivnog ili negativnog feedbacka.
#            Kako se rangiraju unutar svojih kategorija?  
#            Koji su top 5 kanala po angažmanu u svakoj kategoriji?
# =====================================================
print("\n🔍 UPIT 2: Top angažman kanala po kategorijama...")

query2_result = spark.sql("""
WITH engagement_stats AS (
    SELECT
        category_title,
        channel_title,
        COUNT(*) AS total_videos,
        SUM(likes) AS total_likes,
        SUM(dislikes) AS total_dislikes,
        SUM(comment_count) AS total_comments,
        SUM(likes + comment_count) AS engagement_score,
        ROUND(SUM(likes + comment_count) / COUNT(*), 2) AS avg_engagement_per_video,
        CASE 
            WHEN SUM(dislikes) = 0 THEN NULL
            ELSE ROUND(SUM(likes) / SUM(dislikes), 2)
        END AS like_dislike_ratio
    FROM youtube_data
    WHERE assignable = true
    GROUP BY category_title, channel_title
)
SELECT *
FROM (
    SELECT
        category_title,
        channel_title,
        total_videos,
        total_likes,
        total_dislikes,
        total_comments,
        engagement_score,
        avg_engagement_per_video,
        like_dislike_ratio,
        RANK() OVER (PARTITION BY category_title ORDER BY engagement_score DESC) AS rank_in_category
    FROM engagement_stats
) ranked
WHERE rank_in_category <= 5  -- Top 5 kanala po svakoj kategoriji
ORDER BY category_title, rank_in_category
""")

print("📊 TOP 25 rezultata UPIT 2:")
query2_result.show(25, truncate=False)
query2_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/query2_channel_engagement"
)

print("✅ UPIT 2 ZAVRŠEN!")
spark.stop()