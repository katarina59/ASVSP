-- =====================================================
-- 9️⃣ Za svaku kategoriju sadržaja i geografski region, koji meseci u godini predstavljaju optimalno vreme za lansiranje YouTube videa
--    koji će imati najveću šansu za uspeh, rangiran po sezonskoj popularnosti i trendu gledanosti?
-- =====================================================

CREATE OR REPLACE VIEW vw_q09_seasonal_launch AS
WITH seasonal_patterns AS (
    SELECT 
        dc.category_title,
        dr.region,
        dt.trending_month,
        dt.trending_year,
        
        COUNT(*) as videos_count,
        AVG(f.views) as avg_views,
        AVG(f.likes) as avg_engagement,
        
        -- Window funkcija: Poređenje sa prethodnim mesecom (umesto prošle godine)
        LAG(AVG(f.views), 1) OVER (
            PARTITION BY dc.category_title, dr.region
            ORDER BY dt.trending_year, dt.trending_month
        ) as prev_month_views,
        
        -- Window funkcija: Pokretni prosek sezonskih trendova (kraći period)
        AVG(COUNT(*)) OVER (
            PARTITION BY dc.category_title, dr.region
            ORDER BY dt.trending_year, dt.trending_month
            ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING
        ) as seasonal_trend
        
    FROM fact_trending_videos f
    JOIN dim_category dc ON f.category_id = dc.category_id
    JOIN dim_region dr ON f.region_id = dr.region_id
    JOIN dim_trending_date dt ON f.trending_date_fixed = dt.trending_date_fixed
    WHERE dc.assignable = true
    GROUP BY dc.category_title, dr.region, dt.trending_month, dt.trending_year
)
SELECT 
    category_title,
    region,
    trending_month,
    ROUND(AVG(seasonal_trend), 1) as trend_strength,
    
    -- Month-over-month rast 
    COALESCE(
        CASE 
            WHEN AVG(prev_month_views) > 0 THEN
                ROUND(((AVG(avg_views) - AVG(prev_month_views)) / AVG(prev_month_views)) * 100, 1)
            ELSE NULL
        END, 
        0.0  -- Umesto NULL, prikaži 0.0
    ) as mom_growth_pct,
    
    -- Ista logika preporuka kao u originalnom upitu
    CASE 
        WHEN AVG(seasonal_trend) > (
            SELECT AVG(seasonal_trend) * 1.2 
            FROM seasonal_patterns sp2 
            WHERE sp2.category_title = seasonal_patterns.category_title
              AND sp2.region = seasonal_patterns.region
        ) THEN 'OPTIMAL LAUNCH TIME'
        WHEN AVG(seasonal_trend) > (
            SELECT AVG(seasonal_trend) 
            FROM seasonal_patterns sp2 
            WHERE sp2.category_title = seasonal_patterns.category_title
              AND sp2.region = seasonal_patterns.region
        ) THEN 'GOOD TIME'
        ELSE 'AVOID'
    END as launch_recommendation,
    
    -- Dodatno: Confidence indicator
    CASE 
        WHEN SUM(videos_count) >= 50 THEN 'HIGH CONFIDENCE'
        WHEN SUM(videos_count) >= 20 THEN 'MEDIUM CONFIDENCE'
        WHEN SUM(videos_count) >= 10 THEN 'LOW CONFIDENCE'
        ELSE 'VERY LOW CONFIDENCE'
    END as data_confidence,
    
    -- Window funkcija: Ranking meseca unutar kategorije i regiona
    RANK() OVER (
        PARTITION BY category_title, region 
        ORDER BY AVG(seasonal_trend) DESC
    ) as month_rank

FROM seasonal_patterns
GROUP BY category_title, region, trending_month

-- Smanjen prag sa 3 na 1 jer imaš uglavnom data_points=1
HAVING COUNT(*) >= 1 AND SUM(videos_count) >= 15  -- Minimum 15 videa ukupno

ORDER BY 
    category_title, 
    region, 
    data_confidence DESC,  -- Prvo prikaži pouzdane rezultate
    month_rank;
