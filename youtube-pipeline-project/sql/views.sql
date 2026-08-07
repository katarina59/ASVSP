-- Kontekst batch podataka za near-real-time Metabase vizualizaciju.
-- Spaja real-time rezultat (kanal trenutno na trending listi) sa batch istorijskim
-- angažmanom istog kanala, da se real-time rezultat prikazuje u širem kontekstu
-- istorijskih (batch) podataka, a ne izolovano (zahtev iz Big_Data_Architecture dokumenta).
--
-- NAPOMENA: ovaj fajl treba pokrenuti TEK nakon što su oba izvora bar jednom napunjena
-- (DAG-ovi batch_query i near_realtime_query1), jer CREATE VIEW zahteva da referencirane
-- tabele već postoje. Isti obrazac (join na channel_title/category_title) treba ponoviti
-- za query2-5 po potrebi, u zavisnosti od toga koje kolone ti upiti imaju.

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
