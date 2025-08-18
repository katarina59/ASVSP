-- =====================================================
-- 8️⃣ Koja kombinacija dužine opisa videa i kvaliteta thumbnail-a donosi najbolje performanse (najviše pregleda i lajkova) za svaku kategoriju YouTube sadržaja?
-- =====================================================

-- DODAJ NA POČETAK UPITA:

CREATE OR REPLACE VIEW vw_q08_desc_thumb_performance AS
SET max_parallel_workers_per_gather = 0;
SET parallel_tuple_cost = 1000000;

WITH content_analysis AS (
    SELECT 
        f.video_id,
        dc.category_title,
        dr.region,
        f.views,
        f.likes,
        f.comment_count,

        -- Analiza dužine opisa
        CASE 
            WHEN LENGTH(dv.description) = 0 THEN 'No Description'
            WHEN LENGTH(dv.description) < 100 THEN 'Very Short'
            WHEN LENGTH(dv.description) < 300 THEN 'Short'
            WHEN LENGTH(dv.description) < 1000 THEN 'Medium'
            WHEN LENGTH(dv.description) < 2000 THEN 'Long'
            ELSE 'Very Long'
        END as description_length_category,

        -- Thumbnail analiza (izvlačimo parametre iz URL-a)
        CASE 
            WHEN dv.thumbnail_link LIKE '%maxresdefault%' THEN 'High Quality'
            WHEN dv.thumbnail_link LIKE '%hqdefault%' THEN 'Medium Quality'
            ELSE 'Standard Quality'
        END as thumbnail_quality,

        LENGTH(dv.description) as desc_length

    FROM fact_trending_videos f
    JOIN dim_video dv ON f.video_id = dv.video_id
    JOIN dim_category dc ON f.category_id = dc.category_id
    JOIN dim_region dr ON f.region_id = dr.region_id
    WHERE dc.assignable = true
)
SELECT 
    category_title,
    description_length_category,
    thumbnail_quality,

    COUNT(*) as video_count,
    ROUND(AVG(views), 0) as avg_views,
    ROUND(AVG(likes), 0) as avg_likes,

    -- Window funkcija: Ranking kombinacija unutar kategorije
    RANK() OVER (
        PARTITION BY category_title 
        ORDER BY AVG(views) DESC
    ) as performance_rank,

    -- Window funkcija: Percentil performansi
    PERCENT_RANK() OVER (
        ORDER BY AVG(views)
    ) as performance_percentile
FROM content_analysis
GROUP BY category_title, description_length_category, thumbnail_quality
HAVING COUNT(*) >= 10
ORDER BY category_title, performance_rank;
