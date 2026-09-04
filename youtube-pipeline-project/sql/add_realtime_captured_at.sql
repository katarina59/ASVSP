-- Dodaje captured_at TIMESTAMP kolonu (default now()) na real-time tabele koje je
-- do sada nisu imale, pa se Metabase kartica koja gleda "šta se dešava SADA" (Korak 5a
-- iz Sofijinog dashboard flow-a) mogla filtrirati samo po vremenu upisa, ne po
-- celoj nagomilanoj istoriji. Vidi napomenu #3 u sql/metabase_dashboard_queries.sql.
--
-- Idempotentno — sigurno ga je pokretati na svaki DAG run (ADD COLUMN IF NOT EXISTS,
-- ALTER TABLE IF EXISTS ako tabela još nije ni kreirana pri prvom pokretanju).
-- Pokreće se i za "_staging" i za finalnu tabelu, jer Spark upisuje (append) u
-- staging sa istom šemom kao DataFrame, a Airflow persist_results radi
-- "INSERT INTO finalna SELECT * FROM staging" — obe moraju imati istu kolonu.

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
