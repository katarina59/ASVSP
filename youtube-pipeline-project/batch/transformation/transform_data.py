import argparse
import json
import sys

from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import (
    col, lit, array, when, trim, regexp_replace, split as spark_split, to_date, concat_ws
) # type: ignore

from transform_data_common import region_map, clean_and_enrich, GOLDEN_DATASET_COLUMNS

GOLDEN_DATASET_PATH = "hdfs://namenode:9000/storage/hdfs/processed/golden_dataset"
RAW_ZONE_PATH = "hdfs://namenode:9000/storage/hdfs/raw"

pg_url = "jdbc:postgresql://postgres:5432/airflow"
pg_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}


def tags_string_to_list(tags_col):
    return when(tags_col.isNull() | (trim(tags_col) == ""), array(lit("uncategorized"))) \
        .otherwise(
            spark_split(regexp_replace(trim(tags_col), "\\|+", "|"), r"\|")
        )


def run_initial(spark):

    regions = list(region_map.keys())

    for i, region in enumerate(regions):
        print(f"Processing region: {region}")

        videos_df = spark.read.option("header", True).csv(f"{RAW_ZONE_PATH}/{region}videos.csv")

        json_lines = spark.sparkContext.textFile(f"{RAW_ZONE_PATH}/{region}_category_id.json").collect()
        json_obj = json.loads("".join(json_lines))
        items = json_obj["items"]
        rdd = spark.sparkContext.parallelize(items).map(lambda x: json.dumps(x))
        categories_df = spark.read.json(rdd).select(
            col("id").alias("category_id"),
            col("snippet.title").alias("category_title"),
            col("snippet.assignable").alias("assignable"),
        )

        joined_df = videos_df.alias("v").join(
            categories_df.alias("c"),
            col("v.category_id") == col("c.category_id"),
            "left"
        )

        raw_shape_df = joined_df.select(
            col("v.video_id"),
            col("v.title").alias("video_title"),
            col("v.channel_title"),
            col("v.publish_time"),
            col("v.views").cast("long"),
            col("v.likes").cast("long"),
            col("v.dislikes").cast("long"),
            col("v.comment_count").cast("long"),
            col("v.category_id").cast("int"),
            col("c.category_title"),
            col("c.assignable").cast("boolean"),
            tags_string_to_list(col("v.tags")).alias("tags_list"),
            col("v.thumbnail_link"),
            col("v.comments_disabled").cast("boolean"),
            col("v.ratings_disabled").cast("boolean"),
            col("v.video_error_or_removed").cast("boolean"),
            col("v.trending_date"),
            col("v.description"),
            lit(region).alias("region"),
        )

        processed_df = clean_and_enrich(raw_shape_df).select(GOLDEN_DATASET_COLUMNS)
        processed_df = processed_df.repartition(2)

        write_mode = "overwrite" if i == 0 else "append"
        processed_df.write.mode(write_mode).format("parquet").save(GOLDEN_DATASET_PATH)

    print("Initial transform completed successfully!")


def run_incremental(spark, watermark):

    sink_df = spark.read.jdbc(pg_url, "sink_data.sink_videos", properties=pg_properties)

    split_date = spark_split(col("trending_date"), "\\.")

    filtered_df = sink_df.withColumn(
        "trending_full_date_probe",
        to_date(
            concat_ws(".", split_date.getItem(1), split_date.getItem(2), split_date.getItem(0)),
            "dd.MM.yy"
        )
    ).filter(col("trending_full_date_probe") > lit(watermark)).drop("trending_full_date_probe")

    new_count = filtered_df.count()
    print(f"Novih redova iznad watermark-a ({watermark}): {new_count}")

    if new_count == 0:
        print("Nema novih podataka za append.")
        return

    raw_shape_df = filtered_df.select(
        col("video_id"),
        col("title").alias("video_title"),
        col("channel_title"),
        col("publish_time").cast("string"),
        col("views"),
        col("likes"),
        col("dislikes"),
        col("comment_count"),
        col("category_id"),
        col("category_title"),
        col("assignable"),
        tags_string_to_list(col("tags")).alias("tags_list"),
        col("thumbnail_link"),
        col("comments_disabled"),
        col("ratings_disabled"),
        col("video_error_or_removed"),
        col("trending_date"),
        col("description"),
        col("region"),
    )

    processed_df = clean_and_enrich(raw_shape_df).select(GOLDEN_DATASET_COLUMNS)

    golden_df = spark.read.parquet(GOLDEN_DATASET_PATH)
    existing_keys = golden_df.select("video_id", "trending_date").distinct()
    new_df = processed_df.join(existing_keys, on=["video_id", "trending_date"], how="left_anti")

    new_rows = new_df.count()
    print(f"Appendujem {new_rows} novih redova na golden dataset (od {processed_df.count()} kandidata)")

    if new_rows == 0:
        print("Svi kandidati već postoje u golden datasetu — preskačem append.")
    else:
        new_df.write.mode("append").format("parquet").save(GOLDEN_DATASET_PATH)

    total_rows = spark.read.parquet(GOLDEN_DATASET_PATH).count()
    print(f"Golden dataset sada ima {total_rows} redova ukupno")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["initial", "incremental"], default="initial")
    parser.add_argument("--watermark", default=None, help="Watermark datum (YYYY-MM-DD), obavezno za --mode incremental")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("transform_data").getOrCreate()

    if args.mode == "initial":
        run_initial(spark)
    else:
        if not args.watermark:
            print("--watermark je obavezan za --mode incremental")
            spark.stop()
            sys.exit(1)
        run_incremental(spark, args.watermark)

    spark.stop()


if __name__ == "__main__":
    main()
