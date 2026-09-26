ALTER TABLE IF EXISTS real_time_data_queries.query1_viral_anomalies_staging
    ADD COLUMN IF NOT EXISTS captured_at TIMESTAMP DEFAULT now();
ALTER TABLE IF EXISTS real_time_data_queries.query1_viral_anomalies
    ADD COLUMN IF NOT EXISTS captured_at TIMESTAMP DEFAULT now();

ALTER TABLE IF EXISTS real_time_data_queries.query2_performance_vs_historical_staging
    ADD COLUMN IF NOT EXISTS captured_at TIMESTAMP DEFAULT now();
ALTER TABLE IF EXISTS real_time_data_queries.query2_performance_vs_historical
    ADD COLUMN IF NOT EXISTS captured_at TIMESTAMP DEFAULT now();

ALTER TABLE IF EXISTS real_time_data_queries.query3_top_performers_staging
    ADD COLUMN IF NOT EXISTS captured_at TIMESTAMP DEFAULT now();
ALTER TABLE IF EXISTS real_time_data_queries.query3_top_performers
    ADD COLUMN IF NOT EXISTS captured_at TIMESTAMP DEFAULT now();

ALTER TABLE IF EXISTS real_time_data_queries.query3_category_comparison_staging
    ADD COLUMN IF NOT EXISTS captured_at TIMESTAMP DEFAULT now();
ALTER TABLE IF EXISTS real_time_data_queries.query3_category_comparison
    ADD COLUMN IF NOT EXISTS captured_at TIMESTAMP DEFAULT now();
