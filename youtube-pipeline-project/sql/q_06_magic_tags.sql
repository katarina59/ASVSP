-- =====================================================
-- 6️⃣ Koji tagovi, pored osnovne popularnosti, ostvaruju najbolje sezonske trendove i regionalne rezultate, kakva je njihova geografska dominacija i 
--    broj aktivnih regiona, kakav im je dodeljeni “power level” na osnovu uspešnosti, 
--    i koju preporuku za buduću upotrebu zaslužuju na osnovu kombinacije svih ovih faktora?
-- =====================================================

CREATE OR REPLACE VIEW vw_q06_magic_tags AS
WITH tag_explosion AS (
    SELECT 
        f.video_id,
        f.likes,
        f.dislikes,
        f.views,
        f.comment_count,
        dc.category_title,
        dr.region,
        dt.trending_month,
        dt.trending_year,
        dp.publish_month,
        dp.publish_year,
        LOWER(TRIM(UNNEST(string_to_array(
            REPLACE(REPLACE(REPLACE(dv.tags_list::TEXT, '[', ''), ']', ''), '"', ''), ', '
        )))) as tag,
        CASE 
            WHEN f.dislikes > 0 THEN f.likes::DECIMAL / f.dislikes
            ELSE f.likes
        END as like_dislike_ratio,
        (f.likes + f.comment_count) as engagement_score
    FROM fact_trending_videos f
    JOIN dim_video dv ON f.video_id = dv.video_id
    JOIN dim_category dc ON f.category_id = dc.category_id
    JOIN dim_region dr ON f.region_id = dr.region_id
    JOIN dim_trending_date dt ON f.trending_date_fixed = dt.trending_date_fixed
    JOIN dim_publish_date dp ON f.publish_time = dp.publish_time
    WHERE dv.tags_list IS NOT NULL 
      AND dc.assignable = true
),
tag_performance_metrics AS (
    SELECT 
        tag,
        COUNT(*) as video_count,
        COUNT(DISTINCT region) as regions_count,
        COUNT(DISTINCT category_title) as categories_count,
        SUM(likes) as total_likes,
        SUM(views) as total_views,
        SUM(engagement_score) as total_engagement,
        AVG(likes) as avg_likes,
        AVG(like_dislike_ratio) as avg_like_ratio,
        AVG(engagement_score) as avg_engagement,
        COUNT(CASE WHEN likes > 100000 THEN 1 END) as high_performance_videos,
        ROUND(
            COUNT(CASE WHEN likes > 100000 THEN 1 END) * 100.0 / COUNT(*), 2
        ) as viral_success_rate,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as frequency_rank,
        ROW_NUMBER() OVER (ORDER BY SUM(likes) DESC) as likes_rank,
        ROW_NUMBER() OVER (ORDER BY AVG(likes) DESC) as avg_likes_rank,
        PERCENT_RANK() OVER (ORDER BY COUNT(*)) as frequency_percentile,
        PERCENT_RANK() OVER (ORDER BY AVG(likes)) as performance_percentile,
        (COUNT(*) * 0.3 + AVG(likes) * 0.0001 + 
         COUNT(CASE WHEN likes > 100000 THEN 1 END) * 50) as viral_score
    FROM tag_explosion
    WHERE LENGTH(tag) > 2  
      AND tag NOT LIKE '%,%'  
      AND tag != ''
      AND tag IS NOT NULL
    GROUP BY tag
    HAVING COUNT(*) >= 10
),
seasonal_tag_trends AS (
    SELECT 
        tag,
        trending_month,
        trending_year,
        COUNT(*) as monthly_count,
        AVG(COUNT(*)) OVER (
            PARTITION BY tag 
            ORDER BY trending_year, trending_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as rolling_3month_avg,
        ROW_NUMBER() OVER (
            PARTITION BY trending_month
            ORDER BY COUNT(*) DESC
        ) as seasonal_rank
    FROM tag_explosion
    WHERE tag IS NOT NULL AND tag != ''
    GROUP BY tag, trending_month, trending_year
    HAVING COUNT(*) >= 3
),
regional_tag_analysis AS (
    SELECT 
        tag,
        region,
        COUNT(*) as region_count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY tag) as region_share_pct
    FROM tag_explosion
    WHERE tag IS NOT NULL AND tag != ''
    GROUP BY tag, region
),
magic_tags AS (
    SELECT 
        tpm.tag,
        COALESCE(AVG(str.rolling_3month_avg), 0) as avg_seasonal_trend,
        COALESCE(MIN(str.seasonal_rank), 999) as best_seasonal_position,
        COUNT(DISTINCT rta.region) as active_regions,
        COALESCE(MAX(rta.region_share_pct), 0) as max_regional_dominance,
        CASE 
            WHEN tpm.viral_success_rate > 50 AND tpm.video_count > 50 THEN 'MAGIC'
            WHEN tpm.viral_success_rate > 30 AND tpm.avg_likes > 500000 THEN 'POWERFUL'
            WHEN tpm.video_count > 100 AND tpm.avg_likes > 100000 THEN 'RELIABLE'
            WHEN tpm.viral_success_rate > 20 THEN 'PROMISING'
            ELSE 'AVERAGE'
        END as tag_power_level,
        tpm.viral_score,
        tpm.frequency_percentile,
        tpm.performance_percentile,
        tpm.viral_success_rate
    FROM tag_performance_metrics tpm
    LEFT JOIN seasonal_tag_trends str ON tpm.tag = str.tag
    LEFT JOIN regional_tag_analysis rta ON tpm.tag = rta.tag
    GROUP BY tpm.tag, tpm.video_count, tpm.avg_likes, tpm.viral_success_rate,
             tpm.frequency_percentile, tpm.performance_percentile, tpm.viral_score
)
SELECT 
    tag as "Tag",
    active_regions as "Aktivni Regioni",
    ROUND(max_regional_dominance, 1) as "Max Regional %",
    tag_power_level as "Power Level",
    ROUND(viral_score, 0) as "Viral Score",
    CASE 
        WHEN frequency_percentile > 0.9 THEN '🔥 TOP FREQUENCY'
        WHEN performance_percentile > 0.9 THEN '⭐ TOP PERFORMANCE'  
        WHEN viral_success_rate > 40 THEN '🎯 HIGH SUCCESS'
        ELSE '📊 GOOD'
    END as "Tip Taga",
    CASE 
        WHEN tag_power_level = 'MAGIC' THEN '👑 MUST USE!'
        WHEN tag_power_level = 'POWERFUL' THEN '💪 HIGHLY RECOMMENDED'
        WHEN tag_power_level = 'RELIABLE' THEN '✅ SAFE CHOICE'
        WHEN tag_power_level = 'PROMISING' THEN '🌟 EMERGING'
        ELSE '📈 CONSIDER'
    END as "Preporuka"
FROM magic_tags
WHERE viral_score > 100
ORDER BY viral_score DESC
LIMIT 30;
