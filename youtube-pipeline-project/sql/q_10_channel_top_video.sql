-- =====================================================
-- 1️⃣0️⃣ Koji kanal ima koji najgledaniji video preko milijardu pregleda i u kojoj zemlji?
-- =====================================================


-- GLAVNI UPIT: Najgledaniji video po kanalu sa regionom gde je ostvaren
CREATE OR REPLACE VIEW vw_q10_channel_top_video AS
WITH channel_top_videos AS (
    SELECT 
        v.channel_title,
        v.video_title,
        f.views,
        r.region,
        ROW_NUMBER() OVER (PARTITION BY v.channel_title ORDER BY f.views DESC) as rn
    FROM fact_trending_videos f
    JOIN dim_video v ON f.video_id = v.video_id
    JOIN dim_region r ON f.region_id = r.region_id
)
SELECT 
    channel_title,
    video_title as top_video,
    views as max_views,
    region as top_region
FROM channel_top_videos 
WHERE rn = 1 AND views >= 100000000
ORDER BY views DESC;

-- ALTERNATIVA: Korišćenje FIRST_VALUE() - kompaktniji pristup
SELECT DISTINCT
    v.channel_title,
    FIRST_VALUE(v.video_title) OVER (PARTITION BY v.channel_title ORDER BY f.views DESC) as top_video,
    FIRST_VALUE(f.views) OVER (PARTITION BY v.channel_title ORDER BY f.views DESC) as max_views,
    FIRST_VALUE(r.region) OVER (PARTITION BY v.channel_title ORDER BY f.views DESC) as top_region
FROM fact_trending_videos f
JOIN dim_video v ON f.video_id = v.video_id
JOIN dim_region r ON f.region_id = r.region_id
ORDER BY max_views DESC;

-- PROŠIRENA ANALIZA: Sa dodatnim metrikama i kategoriom
WITH channel_analytics AS (
    SELECT 
        v.channel_title,
        v.video_title,
        c.category_title,
        f.views,
        f.likes,
        f.comment_count,
        r.region,
        pd.publish_year,
        td.trending_year,
        ROW_NUMBER() OVER (PARTITION BY v.channel_title ORDER BY f.views DESC) as video_rank,
        AVG(f.views) OVER (PARTITION BY v.channel_title) as avg_views_per_channel
    FROM fact_trending_videos f
    JOIN dim_video v ON f.video_id = v.video_id
    JOIN dim_region r ON f.region_id = r.region_id
    JOIN dim_category c ON f.category_id = c.category_id
    JOIN dim_publish_date pd ON f.publish_time = pd.publish_time
    JOIN dim_trending_date td ON f.trending_date_fixed = td.trending_date_fixed
)
SELECT 
    channel_title,
    video_title as top_video,
    category_title as top_video_category,
    views as max_views,
    region as top_region,
    publish_year,
    ROUND(avg_views_per_channel, 0) as avg_views_per_channel
FROM channel_analytics 
WHERE video_rank = 1
ORDER BY views DESC;
