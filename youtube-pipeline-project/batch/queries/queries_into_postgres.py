from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.types import * # type: ignore

spark = SparkSession.builder \
    .appName("Ingesting queries into Postgres") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()


pg_url = "jdbc:postgresql://postgres:5432/airflow"
pg_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

spark.catalog.clearCache()

query1_result = spark.read.parquet("hdfs://namenode:9000/storage/hdfs/curated/query1_category_region_analysis")

query1_result.write.mode("overwrite").jdbc(
    pg_url,
    "query1_category_region_analysis",
    properties=pg_properties
)

query2_result = spark.read.parquet("hdfs://namenode:9000/storage/hdfs/curated/query2_channel_engagement")

query2_result.write.mode("overwrite").jdbc(
    pg_url,
    "query2_channel_engagement",
    properties=pg_properties
)

query3_result = spark.read.parquet("hdfs://namenode:9000/storage/hdfs/curated/query3_viral_golden_combinations")

query3_result.write.mode("overwrite").jdbc(
    pg_url,
    "query3_viral_golden_combinations",
    properties=pg_properties
)

query4_result = spark.read.parquet("hdfs://namenode:9000/storage/hdfs/curated/query4_problem_analysis")

query4_result.write.mode("overwrite").jdbc(
    pg_url,
    "query4_problem_analysis",
    properties=pg_properties
)

query5_result = spark.read.parquet("hdfs://namenode:9000/storage/hdfs/curated/query5_tag_viral_analysis")

query5_result.write.mode("overwrite").jdbc(
    pg_url,
    "query5_tag_viral_analysis",
    properties=pg_properties
)
spark.stop()