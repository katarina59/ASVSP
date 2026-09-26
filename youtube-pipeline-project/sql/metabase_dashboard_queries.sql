SELECT COUNT(DISTINCT channel_title) AS ukupno_kanala
FROM batch_data_queries.query11_trending_persistence;

SELECT COUNT(DISTINCT region) AS ukupno_regiona
FROM batch_data_queries.query11_trending_persistence;

SELECT COUNT(DISTINCT channel_title) AS aktivnih_kanala_sada
FROM real_time_data_queries.query1_top_trending
WHERE start >= NOW() - INTERVAL '30 minutes';

SELECT COUNT(*) AS broj_anomalija_sada
FROM real_time_data_queries.query1_viral_anomalies
WHERE captured_at >= NOW() - INTERVAL '30 minutes';

SELECT
    category_title,
    SUM(video_count)                              AS ukupno_videa,
    ROUND(SUM(avg_views * video_count))            AS ukupno_pregleda_procena,
    ROUND(AVG(avg_views)::numeric, 0)              AS prosecno_pregleda_po_videu,
    ROUND(AVG(avg_comments)::numeric, 1)           AS prosecno_komentara_po_videu
FROM batch_data_queries.query1_category_region_analysis
WHERE region = {{region}}
GROUP BY category_title
ORDER BY ukupno_pregleda_procena DESC
LIMIT 15;

SELECT
    trending_full_date,
    category_title,
    ROUND(AVG(avg_views)::numeric, 0) AS avg_views,
    SUM(video_count)                  AS video_count
FROM batch_data_queries.query1_category_region_analysis
WHERE region = {{region}}
GROUP BY trending_full_date, category_title
ORDER BY trending_full_date;

SELECT
    channel_title,
    total_videos,
    avg_days_on_trending,
    max_days_on_trending,
    persistence_tier,
    persistence_rank_in_category,
    avg_views
FROM batch_data_queries.query11_trending_persistence
WHERE region = {{region}}
  AND category_title = {{category}}
ORDER BY persistence_rank_in_category
LIMIT 20;

SELECT
    channel_title,
    total_videos,
    engagement_score,
    avg_engagement_per_video,
    like_dislike_ratio,
    rank_in_category
FROM batch_data_queries.query2_channel_engagement
WHERE category_title = {{category}}
ORDER BY rank_in_category;

SELECT
    trending_month,
    trend_strength,
    mom_growth_pct,
    launch_recommendation,
    data_confidence,
    month_rank
FROM batch_data_queries.query9_optimal_launch_timing
WHERE region = {{region}}
  AND category_title = {{category}}
ORDER BY trending_month;

SELECT
    tag,
    video_count,
    avg_views,
    avg_engagement,
    engagement_rate_pct,
    viral_success_rate,
    tag_rank_in_cat_region
FROM batch_data_queries.query12_tags_by_region_category
WHERE region = {{region}}
  AND category_title = {{category}}
ORDER BY avg_views DESC
LIMIT 40;

SELECT
    tag,
    video_count,
    avg_views,
    avg_likes,
    avg_engagement,
    engagement_rate_pct,
    viral_success_rate
FROM batch_data_queries.query12_tags_by_region_category
WHERE region = {{region}}
  AND category_title = {{category}}
  AND tag = {{tag}};

SELECT
    title_length_category,
    thumbnail_type,
    has_number,
    has_brackets,
    has_official,
    has_feature,
    has_mv,
    video_count,
    avg_trending_days,
    max_trending_days,
    avg_views,
    avg_title_length,
    avg_word_count,
    pattern_rank_in_cat_region
FROM batch_data_queries.query13_title_patterns
WHERE region = {{region}}
  AND category_title = {{category}}
ORDER BY pattern_rank_in_cat_region
LIMIT 20;

SELECT
    description_length_category,
    thumbnail_quality,
    video_count,
    avg_views,
    avg_likes,
    performance_rank
FROM batch_data_queries.query8_content_optimization
WHERE category_title = {{category}}
ORDER BY performance_rank
LIMIT 15;

SELECT
    day_name,
    hour_of_day,
    day_of_week,
    avg_trending_days,
    video_count,
    launch_recommendation,
    slot_rank_in_cat_region
FROM batch_data_queries.query14_publish_heatmap
WHERE region = {{region}}
  AND category_title = {{category}}
ORDER BY day_of_week, hour_of_day;

SELECT
    day_name || ' ' || hour_of_day || 'h–' || (hour_of_day + 1) || 'h' AS preporuceni_termin,
    avg_trending_days,
    video_count
FROM batch_data_queries.query14_publish_heatmap
WHERE region = {{region}}
  AND category_title = {{category}}
  AND launch_recommendation = 'OPTIMAL'
ORDER BY avg_trending_days DESC, video_count DESC
LIMIT 1;

SELECT
    channel_title,
    current_trending_videos,
    REPLACE(current_views, ',', '')::numeric    AS current_views_num,
    REPLACE(historical_avg, ',', '')::numeric   AS historical_avg_num,
    anomaly_x                                    AS velocity_multiplier,
    performance_vs_history,
    growth_indicator,
    channel_tier
FROM real_time_data_queries.query3_top_performers
WHERE captured_at >= NOW() - INTERVAL '30 minutes'
  [[ AND channel_title = {{channel}} ]]
ORDER BY anomaly_x DESC
LIMIT 50;

SELECT
    rt.channel_title,
    rt.status,
    rt.trending_videos_count,
    rt.avg_views,
    rt.viral_score,
    rt.velocity,
    rt.intelligent_score,
    b.category_title            AS batch_category,
    b.rank_in_category           AS batch_rank_in_category
FROM real_time_data_queries.query1_top_trending rt
LEFT JOIN batch_data_queries.query2_channel_engagement b
       ON rt.channel_title = b.channel_title
WHERE rt.start >= NOW() - INTERVAL '30 minutes'
  AND [[ rt.channel_title = {{channel}} ]]
ORDER BY rt.intelligent_score DESC
LIMIT 30;

SELECT
    channel_title,
    trending_videos_count,
    avg_views,
    anomaly_score,
    popularity_tier
FROM real_time_data_queries.query1_viral_anomalies
WHERE captured_at >= NOW() - INTERVAL '30 minutes'
ORDER BY anomaly_score DESC
LIMIT 20;

SELECT
    channel_title,
    batch_category,
    video_count,
    avg_views,
    batch_avg_views,
    improvement_vs_batch,
    performance_trend,
    title_strategy_grade,
    viral_potential_score,
    strategy_effectiveness,
    avg_title_length,
    avg_word_count,
    avg_emotional_score,
    avg_optimization_score,
    window_start
FROM real_time_data_queries.query4_combined_analysis
WHERE window_start >= NOW() - INTERVAL '30 minutes'
  AND [[ channel_title = {{channel}} ]]
ORDER BY viral_potential_score DESC
LIMIT 30;

SELECT channel_title, 'Brojevi u naslovu'    AS obrazac_naslova, numbers_avg_views        AS prosecno_pregleda, window_start
FROM real_time_data_queries.query4_combined_analysis
WHERE window_start >= NOW() - INTERVAL '30 minutes' AND numbers_avg_views IS NOT NULL
UNION ALL
SELECT channel_title, 'Viralne ključne reči' AS obrazac_naslova, viral_keywords_avg_views AS prosecno_pregleda, window_start
FROM real_time_data_queries.query4_combined_analysis
WHERE window_start >= NOW() - INTERVAL '30 minutes' AND viral_keywords_avg_views IS NOT NULL
UNION ALL
SELECT channel_title, 'Naslov sa pitanjem (?)' AS obrazac_naslova, question_avg_views     AS prosecno_pregleda, window_start
FROM real_time_data_queries.query4_combined_analysis
WHERE window_start >= NOW() - INTERVAL '30 minutes' AND question_avg_views IS NOT NULL
ORDER BY prosecno_pregleda DESC
LIMIT 30;

SELECT
    current_tag AS tag,
    current_usage,
    current_avg_views,
    historical_avg_views,
    performance_vs_historical,
    trend_status,
    tag_momentum_score,
    channels_using_tag,
    window_start
FROM real_time_data_queries.query5_historical_comparison
WHERE window_start >= NOW() - INTERVAL '30 minutes'
ORDER BY tag_momentum_score DESC
LIMIT 20;

SELECT
    category_title,
    region,
    total_videos,
    avg_days_on_trending,
    max_days_on_trending,
    total_trending_days,
    persistence_tier,
    persistence_rank_in_category
FROM batch_data_queries.query11_trending_persistence
WHERE channel_title = {{channel}}
ORDER BY total_trending_days DESC;

SELECT
    category_title,
    total_viral_videos,
    avg_days_to_viral,
    avg_momentum,
    fast_viral_percentage
FROM batch_data_queries.query7_fastest_viral_channels
WHERE channel_title = {{channel}};

SELECT
    top_video,
    top_video_category,
    max_views,
    top_region,
    publish_year,
    avg_views_per_channel
FROM batch_data_queries.query10_top_channels_mega_hits
WHERE channel_title = {{channel}};

SELECT
    channel_title,
    status,
    trending_videos_count,
    avg_views,
    intelligent_score,
    start AS poslednje_vidjen
FROM real_time_data_queries.query1_top_trending
WHERE channel_title = {{channel}}
  AND start >= NOW() - INTERVAL '30 minutes'
ORDER BY start DESC
LIMIT 1;
