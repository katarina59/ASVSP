-- =====================================================
-- 5️⃣ Koji su tagovi, izdvojeni iz liste tagova u videima, najčešći i najuspešniji prema osnovnim metrikama kao što su broj videa, 
 --   prosečni lajkovi, broj regiona i kategorija u kojima se pojavljuju, viral rate i ukupna popularnost, 
 --   i kako se rangiraju prema kombinovanom “viral score” pokazatelju?
-- =====================================================

CREATE OR REPLACE VIEW vw_q05_tags_core_metrics AS
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
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY viral_score DESC) as overall_rank,
    tag as "Tag",
    video_count as "Broj Videa",
    ROUND(avg_likes, 0) as "Prosečni Lajkovi",
    viral_success_rate as "Viral Rate %",
    regions_count as "Regioni",
    categories_count as "Kategorije"
FROM tag_performance_metrics
WHERE viral_score > 100
ORDER BY viral_score DESC
LIMIT 30;
