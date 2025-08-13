# processed_zone_golden_dataset.py
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col, lit, to_date, month, year, concat_ws, split as spark_split # type: ignore

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
        spark_split(col("v.tags"), r"\|").alias("tags_list"), 
        col("v.thumbnail_link"),
        col("v.comments_disabled").cast("boolean"),
        col("v.ratings_disabled").cast("boolean"),
        col("v.video_error_or_removed").cast("boolean"),
        col("v.trending_date") 
    ).withColumn(
        "trending_date_fixed",
        concat_ws(".", split_date.getItem(1), split_date.getItem(2), split_date.getItem(0))
    ).withColumn(
        "trending_full_date",
        to_date(col("trending_date_fixed"), "dd.MM.yy")
    ).withColumn(
        "trending_month", month(col("trending_full_date"))
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

