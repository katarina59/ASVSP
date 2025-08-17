-- =====================================================
-- 1️⃣ Koja je prosečna gledanost i prosečan broj komentara po kategoriji, regionu i datumu kada su videi postali trending, 
--    i kako se ti proseci razlikuju za videe sa uključenim i onemogućenim komentarima? Koje kategorije i regioni dominiraju po gledanosti, 
--    a kako se trend pregleda menja tokom poslednja tri dana?
-- =====================================================


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


-- =====================================================
-- 2️⃣ 2. Koje kategorije i kanali ostvaruju najveći angažman korisnika?  
--          Koliki je njihov engagement score (ukupan broj lajkova + komentara),
--          kao i da li taj angažman dolazi iz pozitivnog ili negativnog feedbacka.
--          Kako se rangiraju unutar svojih kategorija?  
--          Koji su top 5 kanala po angažmanu u svakoj kategoriji?
-- =====================================================

WITH engagement_stats AS (
    SELECT
        dc.category_title,
        dv.channel_title,
        COUNT(*) AS total_videos,
        SUM(f.likes) AS total_likes,
        SUM(f.dislikes) AS total_dislikes,
        SUM(f.comment_count) AS total_comments,
        SUM(f.likes + f.comment_count) AS engagement_score,
        ROUND(SUM(f.likes + f.comment_count)::NUMERIC / COUNT(*), 2) AS avg_engagement_per_video,

        -- Odnos lajkova prema dislajkovima
        CASE 
            WHEN SUM(f.dislikes) = 0 THEN NULL
            ELSE ROUND(SUM(f.likes)::NUMERIC / NULLIF(SUM(f.dislikes), 0), 2)
        END AS like_dislike_ratio

    FROM fact_trending_videos f
    JOIN dim_category dc ON f.category_id = dc.category_id
    JOIN dim_video dv ON f.video_id = dv.video_id
    WHERE dc.assignable = true
    GROUP BY dc.category_title, dv.channel_title
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
ORDER BY category_title, rank_in_category;


-- =====================================================
-- 3️⃣ Koje YouTube kategorije i regioni su top 10% najbržih viralnih videa i istovremeno među top 10% po trajanju na trending listi?
--    Koliko prosečno treba da video dospe na trending i koliko dugo ostaje, i kako se ove kombinacije rangiraju u odnosu na sve ostale?
--    Koji sadržaji su i instant hit i dugotrajni hit, tj. “zlatne kombinacije”?
-- =====================================================
WITH stats AS (
    SELECT
        c.category_title AS category,
        r.region AS region,
        (MIN(td.trending_full_date) - pd.publish_date) AS days_to_trend,
        COUNT(DISTINCT td.trending_full_date) AS trend_duration_days
    FROM fact_trending_videos f
    JOIN dim_category c 
        ON f.category_id = c.category_id
    JOIN dim_region r 
        ON f.region_id = r.region_id
    JOIN dim_publish_date pd
        ON f.publish_time = pd.publish_time
    JOIN dim_trending_date td
        ON f.trending_date_fixed = td.trending_date_fixed
    GROUP BY c.category_title, r.region, pd.publish_date, f.video_id
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
    ROUND((pct_fastest_to_trend * 100)::numeric, 2) AS pct_rank_fastest,
    ROUND((pct_longest_trending * 100)::numeric, 2) AS pct_rank_longest
FROM ranked
WHERE pct_fastest_to_trend <= 0.10 AND pct_longest_trending >= 0.90
ORDER BY pct_fastest_to_trend, pct_longest_trending;

-- =====================================================
-- 4️⃣ Koji tip problema je najčešći po kombinaciji kategorije i regiona i koliki procenat problematičnih videa čini?
-- =====================================================
WITH problem_stats AS (
    SELECT
        f.video_id,
        dc.category_title,
        dr.region,
        CASE 
            WHEN dv.video_error_or_removed THEN 'Removed'
            WHEN dv.comments_disabled = true AND dv.ratings_disabled = true THEN 'All Interactions Disabled'
            WHEN dv.comments_disabled = true THEN 'Comments Disabled Only'
            WHEN dv.ratings_disabled = true THEN 'Ratings Disabled Only'
            ELSE 'No Issue'
        END AS problem_type,
        f.views
    FROM fact_trending_videos f
    JOIN dim_video dv ON f.video_id = dv.video_id
    JOIN dim_category dc ON f.category_id = dc.category_id
    JOIN dim_region dr ON f.region_id = dr.region_id
),
problem_agg AS (
    SELECT
        category_title,
        region,
        problem_type,
        COUNT(video_id) AS num_videos,
        ROUND(
            COUNT(video_id)::NUMERIC / SUM(COUNT(video_id)) OVER (PARTITION BY category_title, region) * 100,
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
ORDER BY category_title, region;


-- =====================================================
-- 5️⃣ Koji su tagovi, izdvojeni iz liste tagova u videima, najčešći i najuspešniji prema osnovnim metrikama kao što su broj videa, 
 --   prosečni lajkovi, broj regiona i kategorija u kojima se pojavljuju, viral rate i ukupna popularnost, 
 --   i kako se rangiraju prema kombinovanom “viral score” pokazatelju?
-- =====================================================

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


-- =====================================================
-- 6️⃣ Koji tagovi, pored osnovne popularnosti, ostvaruju najbolje sezonske trendove i regionalne rezultate, kakva je njihova geografska dominacija i 
--    broj aktivnih regiona, kakav im je dodeljeni “power level” na osnovu uspešnosti, 
--    i koju preporuku za buduću upotrebu zaslužuju na osnovu kombinacije svih ovih faktora?
-- =====================================================
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


-- =====================================================
-- 7️⃣ Koji YouTube kanali u određenim kategorijama najbrže postižu viralni status i kako im se menja dinamiku popularnosti kroz vreme?
-- =====================================================

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

-- =====================================================
-- 8️⃣ Koja kombinacija dužine opisa videa i kvaliteta thumbnail-a donosi najbolje performanse (najviše pregleda i lajkova) za svaku kategoriju YouTube sadržaja?
-- =====================================================

-- DODAJ NA POČETAK UPITA:
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


-- =====================================================
-- 9️⃣ Za svaku kategoriju sadržaja i geografski region, koji meseci u godini predstavljaju optimalno vreme za lansiranje YouTube videa
--    koji će imati najveću šansu za uspeh, rangiran po sezonskoj popularnosti i trendu gledanosti?
-- =====================================================

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



-- =====================================================
-- 1️⃣0️⃣ Koji kanal ima koji najgledaniji video preko milijardu pregleda i u kojoj zemlji?
-- =====================================================


-- GLAVNI UPIT: Najgledaniji video po kanalu sa regionom gde je ostvaren
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