from pyspark.sql import SparkSession # type: ignore

# -----------------------
# 1️⃣ Spark session
# -----------------------
spark = SparkSession.builder \
    .appName("Golden to Star Schema in Citus") \
    .getOrCreate()

# -----------------------
# 2️⃣ JDBC & psycopg2 parametri
# -----------------------
jdbc_url = "jdbc:postgresql://citus_coordinator:5432/youtube_dw"
db_properties = {
    "user": "citus",
    "password": "citus",
    "driver": "org.postgresql.Driver"
}

pg_conn_params = {
    "host": "citus_coordinator",
    "port": 5432,
    "dbname": "youtube_dw",
    "user": "citus",
    "password": "citus"
}


# -----------------------
# 5️⃣ Učitaj golden dataset iz HDFS
# -----------------------
golden_path = "hdfs://namenode:9000/storage/hdfs/processed/golden_dataset"
df = spark.read.parquet(golden_path)

# -----------------------
# 6️⃣ Kreiranje dimenzija
# -----------------------
dim_category = df.select(
    "category_id",
    "category_title",
    "assignable"
).dropDuplicates(["category_id"])

dim_video = df.select(
    "video_id",
    "video_title",
    "channel_title",
    "tags_list",
    "thumbnail_link",
    "comments_disabled",
    "ratings_disabled",
    "video_error_or_removed",
    "description"
).dropDuplicates(["video_id"])

dim_publish_date = df.select(
    "publish_time",
    "publish_date",
    "publish_month",
    "publish_year"
).dropDuplicates(["publish_time"])

dim_trending_date = df.select(
    "trending_date_fixed",
    "trending_full_date",
    "trending_month",
    "trending_year"
).dropDuplicates(["trending_date_fixed"])

dim_region = df.select(
    "region_id",
    "region"
).dropDuplicates(["region_id"])

# -----------------------
# 7️⃣ Kreiranje fact tabele
# -----------------------
fact_trending_videos = df.select(
    "video_id",
    "category_id",
    "publish_time",
    "trending_date_fixed",
    "region_id",
    "views",
    "likes",
    "dislikes",
    "comment_count"
)

# -----------------------
# 8️⃣ Upis u Citus
# -----------------------
def write_table(df, table_name):
    print(f"➡️ Writing {table_name}, number of rows: {df.count()}")
    df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=db_properties)
    print(f"✅ {table_name} upisana u Citus!")

write_table(dim_category, "dim_category")
write_table(dim_video, "dim_video")
write_table(dim_publish_date, "dim_publish_date")
write_table(dim_trending_date, "dim_trending_date")
write_table(dim_region, "dim_region")
write_table(fact_trending_videos, "fact_trending_videos")

# print("✅ Star shema uspešno upisana u Citus!")

spark.stop()


# MOZDA DA UMESTO OBICNOG UPISA UBACIMO AUTO-REFRESH FACT TABELE

# -----------------------
# 8️⃣ Upis u Citus sa auto-refresh
# -----------------------
# def refresh_table(df, table_name):
#     """
#     Briše postojeće podatke u tabeli i upisuje nove.
#     """
#     # Spark mode "overwrite" već radi brisanje stare tabele, ali
#     # u slučaju fact tabele možemo dodatno osigurati
#     df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=db_properties)
#     print(f"✅ Tabela '{table_name}' osvežena i upisana u Citus.")

# # Osvežavamo dimenzije i fact tabelu
# refresh_table(dim_category, "dim_category")
# refresh_table(dim_video, "dim_video")
# refresh_table(dim_publish_date, "dim_publish_date")
# refresh_table(dim_trending_date, "dim_trending_date")
# refresh_table(dim_region, "dim_region")
# refresh_table(fact_trending_videos, "fact_trending_videos")






# VEROVATNO CE SE OBRISATI

# # -----------------------
# # 3️⃣ SQL za kreiranje tabela (ako ne postoje)
# # -----------------------
# create_tables_sql = [
#     """
#     CREATE TABLE IF NOT EXISTS dim_category (
#         category_id int PRIMARY KEY,
#         category_title text,
#         assignable boolean
#     );
#     SELECT create_reference_table('dim_category');
#     """,
#     """
#     CREATE TABLE IF NOT EXISTS dim_video (
#         video_id text PRIMARY KEY,
#         video_title text,
#         channel_title text,
#         tags_list text,
#         thumbnail_link text,
#         comments_disabled boolean,
#         ratings_disabled boolean,
#         video_error_or_removed boolean,
#         description text
#     );
#     SELECT create_reference_table('dim_video');
#     """,
#     """
#     CREATE TABLE IF NOT EXISTS dim_publish_date (
#         publish_time timestamp PRIMARY KEY,
#         publish_date date,
#         publish_month int,
#         publish_year int
#     );
#     SELECT create_reference_table('dim_publish_date');
#     """,
#     """
#     CREATE TABLE IF NOT EXISTS dim_trending_date (
#         trending_date_fixed date PRIMARY KEY,
#         trending_full_date date,
#         trending_month int,
#         trending_year int
#     );
#     SELECT create_reference_table('dim_trending_date');
#     """,
#     """
#     CREATE TABLE IF NOT EXISTS dim_region (
#         region_id int PRIMARY KEY,
#         region text
#     );
#     SELECT create_reference_table('dim_region');
#     """,
#     """
#     CREATE TABLE IF NOT EXISTS fact_trending_videos (
#         video_id text,
#         category_id int,
#         publish_time timestamp,
#         trending_date_fixed date,
#         region_id int,
#         views bigint,
#         likes bigint,
#         dislikes bigint,
#         comment_count bigint
#     );
#     SELECT create_distributed_table('fact_trending_videos', 'region_id');
#     """
# ]

# # -----------------------
# # 4️⃣ Kreiranje tabela u Citus
# # -----------------------
# conn = psycopg2.connect(**pg_conn_params)
# conn.autocommit = True
# cur = conn.cursor()
# for sql in create_tables_sql:
#     cur.execute(sql)
# cur.close()
# conn.close()

# print("✅ Tabele su kreirane ili već postoje.")