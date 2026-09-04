-- ============================================================================
-- METABASE DASHBOARD QUERIES — Sofijin dashboard flow (User_Dashboard_flow_master_rad.pdf)
-- ============================================================================
-- Svaki blok = jedna Metabase "Question" (kartica). Komentar iznad svakog upita
-- govori: KORAK iz PDF-a, naziv kartice, preporučeni tip vizualizacije, i koje
-- Metabase varijable kartica koristi.
--
-- METABASE VARIJABLE — konvencija u ovom fajlu:
--   {{region}}   → Text/Field Filter varijabla, mapirati na region iz naziva fajla (US, GB, DE, CA, FR, RU, MX, KR, JP, IN)
--   {{category}} → Text/Field Filter varijabla (category_title)
--   {{channel}}  → Text varijabla (channel_title), za drill-down klikom na kanal
--   {{tag}}      → Text varijabla, za drill-down klikom na tag u tag cloud-u
--   [[ ... {{var}} ... ]] → Metabase "optional block" sintaksa: ceo WHERE uslov
--       se ignoriše ako varijabla nije postavljena. Koristi se svuda gde kartica
--       treba da radi i PRE nego što Sofija nešto klikne (npr. Korak 1 bez kategorije).
--
-- PREPORUKA ZA DASHBOARD FILTERE:
--   Napraviti dashboard-level filtere "Region" i "Category" i povezati ih na
--   template varijable {{region}}/{{category}} u SVIM karticama koje ih koriste —
--   tako klik u Koraku 1 (region) automatski filtrira kartice u Koraku 2, 3, 4.
--   Metabase to radi nativno kad se isto ime varijable pojavljuje u više kartica
--   vezanih za isti dashboard filter.
--
-- POZNATA OGRANIČENJA PODATAKA (bitno pre nego što se kartice prave):
--   1) query2_channel_engagement (batch) NEMA kolonu region — engagement rank je
--      globalan po kategoriji, ne po (region, kategorija). Kartice koje to koriste
--      su obeležene "(bez regiona)".
--   2) query1_category_region_analysis NEMA lajkove — "engagement" u Koraku 1 je
--      aproksimacija preko avg_comments, ne pravi (likes+comment_count)/views.
--   3) [REŠENO] Real-time tabele real_time_data_queries.query1_viral_anomalies,
--      query2_performance_vs_historical, query3_top_performers,
--      query3_category_comparison sada imaju `captured_at TIMESTAMP DEFAULT now()`
--      kolonu (dodato u Spark foreachBatch write + sql/add_realtime_captured_at.sql
--      migracija, wired kroz ensure_captured_at task u near_realtime_query1/2/3
--      DAG-ovima). Upiti ispod filtriraju na `captured_at >= NOW() - INTERVAL
--      '30 minutes'` da prikažu samo trenutni snapshot, ne celu nagomilanu istoriju.
--      NAPOMENA: migracija + restart streaming Spark job-ova moraju da se izvrše
--      pre nego što ove kartice počnu da rade — do tada je captured_at NULL za
--      sve stare redove upisane pre migracije.
--   4) real_time_data_queries.query3_top_performers.current_views i .historical_avg
--      su TEXT kolone (Spark F.format_number → "1,234,567"), ne brojevi — moraju
--      se čistiti REPLACE(...,',','')::numeric pre bilo kakvog sortiranja/grafikona.
-- ============================================================================


-- ============================================================================
-- KORAK 0 — KPI TRAKA NA VRHU DASHBOARD-A (opciono, ali pravi "wow" utisak)
-- Red od 4 "Number"/"Trend" kartice na vrhu dashboard-a, iznad Koraka 1.
-- ============================================================================

-- 0.1 "Number" kartica — ukupno praćenih kanala u golden dataset-u
SELECT COUNT(DISTINCT channel_title) AS ukupno_kanala
FROM batch_data_queries.query11_trending_persistence;

-- 0.2 "Number" kartica — broj regiona koje agencija pokriva
SELECT COUNT(DISTINCT region) AS ukupno_regiona
FROM batch_data_queries.query11_trending_persistence;

-- 0.3 "Number" kartica — koliko je kanala trenutno (poslednjih 30 min) na trending listi u real-time feed-u
SELECT COUNT(DISTINCT channel_title) AS aktivnih_kanala_sada
FROM real_time_data_queries.query1_top_trending
WHERE start >= NOW() - INTERVAL '30 minutes';

-- 0.4 "Number" kartica sa uslovnim bojenjem (crveno ako > 0) — trenutne viralne anomalije
SELECT COUNT(*) AS broj_anomalija_sada
FROM real_time_data_queries.query1_viral_anomalies
WHERE captured_at >= NOW() - INTERVAL '30 minutes';


-- ============================================================================
-- KORAK 1 — Odaberi region → top kategorije (PDF str. 2, tačka 1)
-- Vizualizacija: horizontalni Bar chart (2 kartice jedna pored druge) ili
-- kombinovani Bar+Line. Klik na kategoriju (Metabase "click behavior" →
-- prosledi category_title kao dashboard filter) vodi u Korak 2.
-- Varijable: {{region}} (obavezna, iz naziva CSV-a; Field Filter ili Text)
-- ============================================================================

-- 1.1 Top kategorije po ukupnom broju pregleda za odabrani region — Bar chart
-- (total_views_est je procena: SUM(avg_views * video_count) po danu, jer Q1
-- ne čuva sirov SUM(views) već samo prosek po grupi — vidi ograničenje #2)
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

-- 1.2 Isti prikaz kao kretanje kroz vreme (Line chart, X = trending_full_date,
-- serija = category_title) — daje Sofiji osećaj trenda, ne samo ukupnog ranga
SELECT
    trending_full_date,
    category_title,
    ROUND(AVG(avg_views)::numeric, 0) AS avg_views,
    SUM(video_count)                  AS video_count
FROM batch_data_queries.query1_category_region_analysis
WHERE region = {{region}}
GROUP BY trending_full_date, category_title
ORDER BY trending_full_date;


-- ============================================================================
-- KORAK 2 — Klikni kategoriju → analitika te kategorije u regionu (PDF str. 2, tačka 2)
-- Vizualizacija: Tabela (top kanali) + Number/Gauge (prosečan engagement) + Line chart (sezonalnost)
-- Varijable: {{region}}, {{category}}
-- ============================================================================

-- 2.1 Top kanali po trending persistence u (region, kategorija) — Tabela, sortirano
-- persistence_tier kao "conditional formatting" kolona (LONG TERM DOMINANT = zeleno)
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

-- 2.2 Prosečan engagement top kanala u toj kategoriji — Bar chart
-- NAPOMENA (ograničenje #1): query2 nema region, pa je ovo rangiranje globalno
-- po kategoriji preko svih regiona, ne samo za {{region}}. Naslov kartice u
-- Metabase-u treba da kaže "Top 5 kanala po engagement-u (globalno)".
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

-- 2.3 Sezonski grafikon — koji meseci su najjači za tu (region, kategorija)
-- kombinaciju — Line/Bar chart, X = trending_month
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


-- ============================================================================
-- KORAK 3a — Tag cloud (PDF str. 2, tačka 3a)
-- Vizualizacija: Metabase nema nativni "word cloud" tip — koristiti bar chart
-- sortiran opadajuće (radi kao "de facto" tag cloud) ILI plugin/custom viz ako
-- je dostupan. Veličina/dužina bara = avg_views. Klik na tag → {{tag}} varijabla
-- prosleđena sledećoj kartici (3a.2).
-- Varijable: {{region}}, {{category}} (za 3a.1), + {{tag}} (za 3a.2, iz klika)
-- ============================================================================

-- 3a.1 Tag cloud osnova — svi tagovi za (region, kategorija), veličina = avg_views
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

-- 3a.2 Klik na tag → detalji tog taga u (region, kategorija) — Number/Detail kartice
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


-- ============================================================================
-- KORAK 3b — Naslov i thumbnail za dugo-trending videe (PDF str. 2, tačka 3b)
-- Vizualizacija: Tabela sa "conditional formatting" na avg_trending_days,
-- + pomoćni Bar chart za thumbnail/opis (3b.2, bez regiona)
-- Varijable: {{region}}, {{category}}
-- ============================================================================

-- 3b.1 Obrasci naslova/thumbnail-a videa koji dugo ostaju na trending listi
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

-- 3b.2 Dopunska kartica — najbolja kombinacija dužine opisa i thumbnail kvaliteta
-- po pregledima (Q8 nema region — globalno po kategoriji)
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


-- ============================================================================
-- KORAK 4 — Optimalno vreme objave: heatmapa dan × sat (PDF str. 2, tačka 4)
-- Vizualizacija: Metabase "Pivot table" (redovi = day_name, kolone = hour_of_day,
-- vrednost = avg_trending_days) obojen kao heatmap preko conditional formatting,
-- ILI ako je dostupan heatmap viz plugin, direktno on.
-- Varijable: {{region}}, {{category}}
-- ============================================================================

-- 4.1 Heatmapa dan × sat objave → prosečno trajanje na trending listi
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

-- 4.2 "Number/Text" kartica sa konkretnom preporukom — najbolji termin
-- (Metabase: prikazati kao karticu tipa "Detail" ili formatirati u jednu rečenicu)
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


-- ============================================================================
-- KORAK 5a — Real-time panel: status videa vs. istorijski benchmark (PDF str. 2, tačka 5a)
-- Vizualizacija: Tabela sa "conditional formatting" (color po growth_indicator:
-- VIRAL EXPLOSION = crveno/narandžasto, Breakout = žuto, Stable/Normal = zeleno,
-- Declining = sivo) — ovo je desni panel dashboard-a, auto-refresh na 1-2 min
-- (Metabase dashboard auto-refresh opcija).
-- Varijable: {{channel}} (opciono, za drill-down iz Koraka 6)
-- NAPOMENA: query3_top_performers.current_views/historical_avg su TEXT sa
-- zarezima (ograničenje #4) — čišćeni ispod pre castovanja.
-- ============================================================================

-- 5a.1 GLAVNI real-time status panel — "iznad proseka / ispod proseka / anomalija"
-- (performance_vs_history i growth_indicator su TAČNO Sofijine tri oznake iz PDF-a)
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

-- 5a.2 Dopuna: da li je kanal ESTABLISHED ili EMERGING i koliki mu je "intelligent score"
-- (spaja se sa batch kontekstom kanala, isti obrazac kao postojeći sql/views.sql)
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

-- 5a.3 "Anomalija" traka — ko trenutno raste neuobičajenom brzinom (Sofijino pitanje #8)
-- Vizualizacija: Number/Alert kartica, crveno ako ima redova; Metabase Alert može
-- da pošalje notifikaciju kad ovaj upit vrati > 0 redova.
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


-- ============================================================================
-- KORAK 5b — Real-time: strategija naslova (PDF str. 2, tačka 5b)
-- Vizualizacija: Tabela + Bar chart poređenje (numbers_avg_views vs
-- viral_keywords_avg_views vs question_avg_views po kanalu)
-- Varijable: {{channel}} opciono
-- ============================================================================

-- 5b.1 Real-time strategija naslova po kanalu, poređenje sa batch istorijom
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

-- 5b.2 Koji obrazac naslova (brojevi/viral ključne reči/pitanje) SADA donosi
-- najviše pregleda — pretvoreno u "long" format za lakši bar chart u Metabase-u
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

-- 5b.3 (bonus, van striktnog PDF opisa ali koristan) — real-time tag momentum,
-- dopunjuje strategiju naslova podacima o tagovima koji SADA rastu
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


-- ============================================================================
-- KORAK 6 — Trajanje dominacije kanala (PDF str. 3, tačka 6)
-- Vizualizacija: Detail/profil kartice (batch istorija) + Tabela (trenutni status)
-- Aktivira se klikom na channel_title bilo gde na dashboard-u (Korak 2 tabela,
-- Korak 5a tabela...) → prosleđuje se kao {{channel}}.
-- Varijable: {{channel}} (obavezna za ovaj korak)
-- ============================================================================

-- 6.1 Batch profil kanala — u kojim kategorijama/regionima dominira i koliko dugo
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

-- 6.2 Brzina viralizacije tog kanala (Q7) — koliko brzo mu videi uđu u trending
SELECT
    category_title,
    total_viral_videos,
    avg_days_to_viral,
    avg_momentum,
    fast_viral_percentage
FROM batch_data_queries.query7_fastest_viral_channels
WHERE channel_title = {{channel}};

-- 6.3 Najveći hit tog kanala ikad (Q10, samo videi sa 100M+ pregleda)
SELECT
    top_video,
    top_video_category,
    max_views,
    top_region,
    publish_year,
    avg_views_per_channel
FROM batch_data_queries.query10_top_channels_mega_hits
WHERE channel_title = {{channel}};

-- 6.4 Da li je kanal TRENUTNO aktivan u real-time panelu — "Number/Boolean" indikator
-- (prazan rezultat = kanal trenutno nije na trending listi)
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
