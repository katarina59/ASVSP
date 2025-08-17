# processed_zone_golden_dataset.py
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col, lit, to_date, when, month, year, array, concat_ws,concat, size, expr,regexp_replace, trim, length, split as spark_split # type: ignore

spark = SparkSession.builder \
    .appName("Processed Zone - Golden Dataset") \
    .getOrCreate()

region_map = {
    "CA": 1, "DE": 2, "FR": 3, "GB": 4, "IN": 5,
    "JP": 6, "KR": 7, "MX": 8, "RU": 9, "US": 10
}


# Lista regiona
regions = list(region_map.keys())

all_dataframes = []

for region in regions:
    # Učitaj video detalje iz raw zone (Avro format)
    videos_df = spark.read.format("avro").load(f"hdfs://namenode:9000/storage/hdfs/raw/avro/{region}videos")

    # Učitaj kategorije iz raw zone (Avro format)
    categories_df = spark.read.format("avro").load(f"hdfs://namenode:9000/storage/hdfs/raw/avro/{region}_category_id")

    # Spoji po category_id
    joined_df = videos_df.alias("v").join(
        categories_df.alias("c"),
        col("v.category_id") == col("c.category_id"),
        "left"
    )

    split_date = spark_split(col("v.trending_date"), "\\.")

    processed_df = joined_df.select(
        col("v.video_id"),
        col("v.title").alias("video_title"),
        col("v.channel_title"),
        col("v.publish_time"),
        col("v.views").cast("long"),
        col("v.likes").cast("long"),
        col("v.dislikes").cast("long"),
        col("v.comment_count").cast("long"),
        col("v.category_id").cast("int"),
        col("c.title").alias("category_title"),
        col("c.assignable").cast("boolean"),
        when(col("v.tags").isNull() | (trim(col("v.tags")) == ""), 
            array(lit("uncategorized")))  # Default tag ako nema tagova
        .otherwise(
            spark_split(
                regexp_replace(trim(col("v.tags")), "\\|+", "|"),  # Ukloni duplikate |
                r"\|"
            )
        ).alias("tags_list"),
        col("v.thumbnail_link"),
        col("v.comments_disabled").cast("boolean"),
        col("v.ratings_disabled").cast("boolean"),
        col("v.video_error_or_removed").cast("boolean"),
        col("v.trending_date"),
        col("v.description"),
    ).withColumn(
        "trending_date_fixed",
        concat_ws(".", split_date.getItem(1), split_date.getItem(2), split_date.getItem(0))
    ).withColumn(
        "trending_full_date",
        to_date(col("trending_date_fixed"), "dd.MM.yy")
    ).withColumn(
        "trending_month", 
            when(month(col("trending_full_date")) == 1, "Januar") 
            .when(month(col("trending_full_date")) == 2, "Februar") 
            .when(month(col("trending_full_date")) == 3, "Mart")
            .when(month(col("trending_full_date")) == 4, "April")
            .when(month(col("trending_full_date")) == 5, "Maj")
            .when(month(col("trending_full_date")) == 6, "Jun")
            .when(month(col("trending_full_date")) == 7, "Jul")
            .when(month(col("trending_full_date")) == 8, "Avgust")
            .when(month(col("trending_full_date")) == 9, "Septembar")
            .when(month(col("trending_full_date")) == 10, "Oktobar")
            .when(month(col("trending_full_date")) == 11, "Novembar")
            .when(month(col("trending_full_date")) == 12, "Decembar")
    ).withColumn(
        "trending_year", year(col("trending_full_date"))
    ).withColumn(
        "publish_date", to_date(col("v.publish_time"))
    ).withColumn(
        "publish_month", month(col("v.publish_time"))
    ).withColumn(
        "publish_year", year(col("v.publish_time"))
    ).withColumn(
        "region", lit(region)
    ).withColumn(
        "region_id", lit(region_map[region])
    ).withColumn(
        # Ukloni non-ASCII karaktere iz video_title  
        "video_title",
        regexp_replace(col("video_title"), "[^\\x20-\\x7E]", "")
    ).withColumn(
        "video_title", trim(col("video_title"))
    ).withColumn(
        # Ograniči dužinu title-ova da ne prave problem u terminalu
        "video_title",
        when(length(col("video_title")) > 100, 
            concat(col("video_title").substr(1, 97), lit("...")))
        .otherwise(col("video_title"))
    ).filter(
        # Filtriraj nevalidne podatke
        (col("video_title").isNotNull()) &
        (col("views") > 0) # Samo videi sa pregledima
    ).withColumn(
        "video_title", regexp_replace(col("video_title"), "\\s+", " ")
    ).withColumn(
        # 🔥 DODATNO ČIŠĆENJE TAGS_LIST: ukloni prazne stringove iz array-a
        "tags_list",
        when(size(col("tags_list")) == 0, array(lit("no-tags")))
        .otherwise(
            # Filter praznih stringova iz array-a
            expr("filter(tags_list, x -> trim(x) != '' AND x IS NOT NULL)")
        )
    ).withColumn(
        # Ako je array prazan nakon filtriranja, dodaj default tag
        "tags_list",
        when(size(col("tags_list")) == 0, array(lit("unknown")))
        .otherwise(col("tags_list"))
    )

    all_dataframes.append(processed_df)

# Spoji sve regione u jedan DataFrame
final_df = all_dataframes[0]
for df in all_dataframes[1:]:
    final_df = final_df.unionByName(df)

# Snimi u processed zonu u Parquet formatu
final_df.write.format("parquet").mode("overwrite").save("hdfs://namenode:9000/storage/hdfs/processed/golden_dataset")

print(final_df.columns)
final_df.printSchema()

spark.stop()

