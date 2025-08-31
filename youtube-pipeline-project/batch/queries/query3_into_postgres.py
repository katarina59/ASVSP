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

query3_result = spark.read.parquet("hdfs://namenode:9000/storage/hdfs/curated/query3_viral_golden_combinations")

query3_result.write.mode("overwrite").jdbc(
    pg_url,
    "query3_viral_golden_combinations",
    properties=pg_properties
)

spark.stop()