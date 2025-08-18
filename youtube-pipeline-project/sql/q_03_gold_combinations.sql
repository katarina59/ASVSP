-- =====================================================
-- 3️⃣ Koje YouTube kategorije i regioni su top 10% najbržih viralnih videa i istovremeno među top 10% po trajanju na trending listi?
--    Koliko prosečno treba da video dospe na trending i koliko dugo ostaje, i kako se ove kombinacije rangiraju u odnosu na sve ostale?
--    Koji sadržaji su i instant hit i dugotrajni hit, tj. “zlatne kombinacije”?
-- =====================================================

CREATE OR REPLACE VIEW vw_q03_gold_combinations AS
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
