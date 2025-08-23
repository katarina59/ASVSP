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



# =====================================================
# 🔍 UPIT 6: Koje tagove, pored osnovne popularnosti, karakteriše najbolja kombinacija viralnog potencijala i tržišne pozicije, 
#            kakav im je dodeljeni "power level" na osnovu uspešnosti u različitim kategorijama i regionima, 
#            koju preporuku za buduću upotrebu zaslužuju na osnovu viral score-a i success rate-a, 
#            i kako se rangiraju globalno kao i unutar svojih power level grupa uz smoothed viral score trend analizu?
# =====================================================
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

# =====================================================
# 💾 DODATNI UPITI - EXECUTIVE SUMMARY I INSIGHTS
# =====================================================

# BONUS UPIT 1: Ukupna statistika po regionima
print("\n📈 BONUS UPIT 1: Executive Summary po regionima...")

bonus1_result = spark.sql("""
SELECT 
    region,
    COUNT(DISTINCT video_id) as total_videos,
    COUNT(DISTINCT channel_title) as unique_channels,
    COUNT(DISTINCT category_title) as categories_covered,
    ROUND(AVG(views), 0) as avg_views_per_video,
    ROUND(AVG(likes), 0) as avg_likes_per_video,
    MAX(views) as highest_views,
    SUM(views) as total_views_region,
    ROUND(AVG(likes) / NULLIF(AVG(dislikes), 0), 2) as avg_like_dislike_ratio,
    COUNT(CASE WHEN comments_disabled = true THEN 1 END) as videos_comments_disabled,
    ROUND(COUNT(CASE WHEN comments_disabled = true THEN 1 END) * 100.0 / COUNT(*), 1) as pct_comments_disabled
FROM youtube_data
GROUP BY region
ORDER BY total_views_region DESC
""")

bonus1_result.show(20, truncate=False)
bonus1_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/bonus1_regional_executive_summary"
)

# BONUS UPIT 2: Kategorial Performance Matrix
print("\n📈 BONUS UPIT 2: Performance Matrix po kategorijama...")

bonus2_result = spark.sql("""
WITH category_metrics AS (
    SELECT 
        category_title,
        COUNT(DISTINCT video_id) as total_videos,
        COUNT(DISTINCT channel_title) as unique_channels,
        COUNT(DISTINCT region) as regions_present,
        ROUND(AVG(views), 0) as avg_views,
        ROUND(AVG(likes), 0) as avg_likes,
        ROUND(AVG(comment_count), 0) as avg_comments,
        ROUND(STDDEV(views), 0) as views_volatility,
        COUNT(CASE WHEN views > 1000000 THEN 1 END) as million_view_videos,
        ROUND(COUNT(CASE WHEN views > 1000000 THEN 1 END) * 100.0 / COUNT(*), 1) as viral_rate,
        PERCENTILE_APPROX(views, 0.5) as median_views,
        PERCENTILE_APPROX(likes, 0.5) as median_likes
    FROM youtube_data
    WHERE assignable = true
    GROUP BY category_title
),
ranked_categories AS (
    SELECT *,
        RANK() OVER (ORDER BY avg_views DESC) as views_rank,
        RANK() OVER (ORDER BY viral_rate DESC) as viral_rank,
        RANK() OVER (ORDER BY total_videos DESC) as volume_rank,
        CASE 
            WHEN viral_rate > 15 AND avg_views > 500000 THEN 'HIGH PERFORMANCE'
            WHEN viral_rate > 10 OR avg_views > 300000 THEN 'MEDIUM PERFORMANCE'
            ELSE 'STANDARD PERFORMANCE'
        END as performance_tier
    FROM category_metrics
)
SELECT 
    category_title,
    total_videos,
    unique_channels,
    regions_present,
    avg_views,
    viral_rate,
    performance_tier,
    views_rank,
    viral_rank
FROM ranked_categories
ORDER BY views_rank
""")

bonus2_result.show(25, truncate=False)
bonus2_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/bonus2_category_performance_matrix"
)

# BONUS UPIT 3: Vremenski trendovi i dinamika
print("\n📈 BONUS UPIT 3: Vremenski trendovi i growth analiza...")

bonus3_result = spark.sql("""
WITH monthly_trends AS (
    SELECT 
        trending_year,
        trending_month,
        category_title,
        COUNT(*) as monthly_video_count,
        ROUND(AVG(views), 0) as avg_monthly_views,
        ROUND(AVG(likes), 0) as avg_monthly_likes,
        COUNT(DISTINCT channel_title) as active_channels
    FROM youtube_data
    WHERE assignable = true 
      AND trending_month IS NOT NULL 
      AND trending_year IS NOT NULL
    GROUP BY trending_year, trending_month, category_title
),
growth_analysis AS (
    SELECT *,
        LAG(monthly_video_count) OVER (
            PARTITION BY category_title 
            ORDER BY trending_year, trending_month
        ) as prev_month_count,
        LAG(avg_monthly_views) OVER (
            PARTITION BY category_title 
            ORDER BY trending_year, trending_month
        ) as prev_month_views,
        AVG(monthly_video_count) OVER (
            PARTITION BY category_title
            ORDER BY trending_year, trending_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as rolling_3m_count
    FROM monthly_trends
)
SELECT 
    trending_year,
    trending_month,
    category_title,
    monthly_video_count,
    avg_monthly_views,
    active_channels,
    CASE 
        WHEN prev_month_count IS NOT NULL THEN
            ROUND(((monthly_video_count - prev_month_count) * 100.0 / prev_month_count), 1)
        ELSE NULL
    END as mom_growth_pct,
    ROUND(rolling_3m_count, 1) as rolling_3m_avg
FROM growth_analysis
WHERE trending_year IS NOT NULL AND trending_month IS NOT NULL
ORDER BY trending_year DESC, trending_month DESC, category_title
""")

bonus3_result.show(40, truncate=False)
bonus3_result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/storage/hdfs/curated/bonus3_temporal_trends_growth"
)

# =====================================================
# 💾 AGREGACIJA I FINALNI IZVJEŠTAJ
# =====================================================
print("\n📋 KREIRANJE FINALNOG IZVJEŠTAJA...")

# Kreiraj sažetak svih analiza
summary_report = spark.sql("""
SELECT 
    'Total Videos Analyzed' as metric,
    CAST(COUNT(*) AS STRING) as value,
    '' as details
FROM youtube_data
UNION ALL
SELECT 
    'Total Categories' as metric,
    CAST(COUNT(DISTINCT category_title) AS STRING) as value,
    '' as details
FROM youtube_data
WHERE assignable = true
UNION ALL
SELECT 
    'Total Regions' as metric,
    CAST(COUNT(DISTINCT region) AS STRING) as value,
    '' as details
FROM youtube_data
UNION ALL
SELECT 
    'Date Range' as metric,
    CAST(COUNT(DISTINCT trending_full_date) AS STRING) as value,
    CONCAT(MIN(trending_full_date), ' to ', MAX(trending_full_date)) as details
FROM youtube_data
WHERE trending_full_date IS NOT NULL
UNION ALL
SELECT 
    'Total Channels' as metric,
    CAST(COUNT(DISTINCT channel_title) AS STRING) as value,
    '' as details
FROM youtube_data
UNION ALL
SELECT 
    'Average Views Per Video' as metric,
    CAST(ROUND(AVG(views), 0) AS STRING) as value,
    '' as details
FROM youtube_data
""")

print("📊 FINALNI IZVJEŠTAJ:")
summary_report.show(truncate=False)

# Sačuvaj finalni izvještaj
summary_report.coalesce(1).write.mode("overwrite").json(
    "hdfs://namenode:9000/storage/hdfs/curated/final_batch_report"
)

print("\n✅ BATCH OBRADA ZAVRŠENA!")
print("📁 Svi rezultati sačuvani u: hdfs://namenode:9000/storage/hdfs/curated/")
print("🎯 Korišćen je pure Data Lake pristup direktno nad HDFS podacima")
print("🔄 Svi upiti transformisani iz Star Schema u Data Lake format")

spark.stop()