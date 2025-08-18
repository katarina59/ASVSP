-- =====================================================
-- 1️⃣ Koja je prosečna gledanost i prosečan broj komentara po kategoriji, regionu i datumu kada su videi postali trending, 
--    i kako se ti proseci razlikuju za videe sa uključenim i onemogućenim komentarima? Koje kategorije i regioni dominiraju po gledanosti, 
--    a kako se trend pregleda menja tokom poslednja tri dana?
-- =====================================================

CREATE OR REPLACE VIEW vw_q01_avg_views_comments AS
SELECT 
    dc.category_title,
    dr.region,
    dt.trending_full_date,
    dv.comments_disabled,
    
    COUNT(*) as video_count,
    ROUND(AVG(f.views), 2) as avg_views,
    ROUND(AVG(f.comment_count), 2) as avg_comments,
    
    -- Window funkcija: Rangiranje po gledanosti unutar kategorije i regije
    RANK() OVER (
        PARTITION BY dc.category_title, dr.region 
        ORDER BY AVG(f.views) DESC
    ) as views_rank,
    
    -- Window funkcija: Pokretni prosek pregleda za poslednja 3 dana
    ROUND(
        AVG(AVG(f.views)) OVER (
            PARTITION BY dc.category_title, dr.region, dv.comments_disabled
            ORDER BY dt.trending_full_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    ) as moving_avg_views_3d

FROM fact_trending_videos f
JOIN dim_category dc ON f.category_id = dc.category_id
JOIN dim_region dr ON f.region_id = dr.region_id  
JOIN dim_trending_date dt ON f.trending_date_fixed = dt.trending_date_fixed
JOIN dim_video dv ON f.video_id = dv.video_id
WHERE dc.assignable = true

GROUP BY 
    dc.category_title, 
    dr.region, 
    dt.trending_full_date, 
    dv.comments_disabled
    
ORDER BY 
    dc.category_title, 
    dr.region, 
    dt.trending_full_date, 
    dv.comments_disabled;
