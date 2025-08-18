-- =====================================================
-- 2️⃣ 2. Koje kategorije i kanali ostvaruju najveći angažman korisnika?  
--          Koliki je njihov engagement score (ukupan broj lajkova + komentara),
--          kao i da li taj angažman dolazi iz pozitivnog ili negativnog feedbacka.
--          Kako se rangiraju unutar svojih kategorija?  
--          Koji su top 5 kanala po angažmanu u svakoj kategoriji?
-- =====================================================


CREATE OR REPLACE VIEW vw_q02_top_engagement_channels AS
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
