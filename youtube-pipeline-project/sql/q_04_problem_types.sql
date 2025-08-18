-- =====================================================
-- 4️⃣ Koji tip problema je najčešći po kombinaciji kategorije i regiona i koliki procenat problematičnih videa čini?
-- =====================================================

CREATE OR REPLACE VIEW vw_q04_problem_types AS
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
