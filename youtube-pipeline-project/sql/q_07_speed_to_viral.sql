-- =====================================================
-- 7️⃣ Koji YouTube kanali u određenim kategorijama najbrže postižu viralni status i kako im se menja dinamiku popularnosti kroz vreme?
-- =====================================================

CREATE OR REPLACE VIEW vw_q07_speed_to_viral AS
WITH viral_timeline AS (
    SELECT 
        dv.channel_title,
        dc.category_title,
        f.video_id,
        dt.trending_full_date,
        f.views,
        f.likes,
        dp.publish_date,
        (dt.trending_full_date - dp.publish_date) as days_to_trending,
        
        -- Window funkcija: Rang videa po brzini viralizacije unutar kanala
        RANK() OVER (
            PARTITION BY dv.channel_title 
            ORDER BY (dt.trending_full_date - dp.publish_date)
        ) as speed_rank_in_channel,
        
        -- Window funkcija: Pokretni prosek performansi kanala kroz vreme
        AVG(f.views) OVER (
            PARTITION BY dv.channel_title
            ORDER BY dt.trending_full_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as channel_momentum_7d
        
    FROM fact_trending_videos f
    JOIN dim_video dv ON f.video_id = dv.video_id
    JOIN dim_category dc ON f.category_id = dc.category_id
    JOIN dim_publish_date dp ON f.publish_time = dp.publish_time
    JOIN dim_trending_date dt ON f.trending_date_fixed = dt.trending_date_fixed
    WHERE dp.publish_date IS NOT NULL 
      AND dt.trending_full_date >= dp.publish_date
)
SELECT 
    channel_title,
    category_title,
    COUNT(*) as total_viral_videos,
    ROUND(AVG(days_to_trending), 1) as avg_days_to_viral,
    ROUND(AVG(channel_momentum_7d), 0) as avg_momentum,
    
    -- Procenat "brzih" videa (trending za manje od 3 dana)
    ROUND(
        COUNT(CASE WHEN days_to_trending <= 3 THEN 1 END) * 100.0 / COUNT(*), 1
    ) as fast_viral_percentage
    
FROM viral_timeline
WHERE days_to_trending BETWEEN 0 AND 30
GROUP BY channel_title, category_title
HAVING COUNT(*) >= 5
ORDER BY fast_viral_percentage DESC, avg_momentum DESC
LIMIT 20;
