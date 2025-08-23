from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.types import * # type: ignore

# =====================================================
# 🔥 KREIRANJE SPARK SESIJE
# =====================================================
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


query4_result = spark.read.parquet("hdfs://namenode:9000/storage/hdfs/curated/query4_problem_analysis")

query4_result.write.mode("overwrite").jdbc(
    pg_url,
    "query4_problem_analysis",
    properties=pg_properties
)

spark.stop()