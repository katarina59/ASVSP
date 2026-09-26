CREATE OR REPLACE VIEW real_time_data_queries.query1_top_trending_with_context AS
SELECT
    rt.start,
    rt.channel_title,
    rt.status,
    rt.trending_videos_count,
    rt.avg_views,
    rt.viral_score,
    rt.velocity,
    rt.consistency,
    rt.intelligent_score,
    b.category_title AS batch_category_title,
    b.total_videos AS batch_total_videos,
    b.engagement_score AS batch_engagement_score,
    b.avg_engagement_per_video AS batch_avg_engagement_per_video,
    b.rank_in_category AS batch_rank_in_category
FROM real_time_data_queries.query1_top_trending rt
LEFT JOIN batch_data_queries.query2_channel_engagement b
    ON rt.channel_title = b.channel_title;
