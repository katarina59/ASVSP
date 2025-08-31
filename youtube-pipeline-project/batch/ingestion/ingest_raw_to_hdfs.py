import json
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col # type: ignore

spark = SparkSession.builder \
    .appName("CSV & JSON to AVRO") \
    .getOrCreate()


CAvideos_df = spark.read.option("header", True).csv("hdfs://namenode:9000/data/raw/CAvideos.csv")
CAvideos_df.write.format("avro").mode("overwrite").save("hdfs://namenode:9000/storage/hdfs/raw/avro/CAvideos")

DEvideos_df = spark.read.option("header", True).csv("hdfs://namenode:9000/data/raw/DEvideos.csv")
DEvideos_df.write.format("avro").mode("overwrite").save("hdfs://namenode:9000/storage/hdfs/raw/avro/DEvideos")

FRvideos_df = spark.read.option("header", True).csv("hdfs://namenode:9000/data/raw/FRvideos.csv")
FRvideos_df.write.format("avro").mode("overwrite").save("hdfs://namenode:9000/storage/hdfs/raw/avro/FRvideos")

GBvideos_df = spark.read.option("header", True).csv("hdfs://namenode:9000/data/raw/GBvideos.csv")
GBvideos_df.write.format("avro").mode("overwrite").save("hdfs://namenode:9000/storage/hdfs/raw/avro/GBvideos")

INvideos_df = spark.read.option("header", True).csv("hdfs://namenode:9000/data/raw/INvideos.csv")
INvideos_df.write.format("avro").mode("overwrite").save("hdfs://namenode:9000/storage/hdfs/raw/avro/INvideos")

JPvideos_df = spark.read.option("header", True).csv("hdfs://namenode:9000/data/raw/JPvideos.csv")
JPvideos_df.write.format("avro").mode("overwrite").save("hdfs://namenode:9000/storage/hdfs/raw/avro/JPvideos")

KRvideos_df = spark.read.option("header", True).csv("hdfs://namenode:9000/data/raw/KRvideos.csv")
KRvideos_df.write.format("avro").mode("overwrite").save("hdfs://namenode:9000/storage/hdfs/raw/avro/KRvideos")

MXvideos_df = spark.read.option("header", True).csv("hdfs://namenode:9000/data/raw/MXvideos.csv")
MXvideos_df.write.format("avro").mode("overwrite").save("hdfs://namenode:9000/storage/hdfs/raw/avro/MXvideos")

RUvideos_df = spark.read.option("header", True).csv("hdfs://namenode:9000/data/raw/RUvideos.csv")
RUvideos_df.write.format("avro").mode("overwrite").save("hdfs://namenode:9000/storage/hdfs/raw/avro/RUvideos")

USvideos_df = spark.read.option("header", True).csv("hdfs://namenode:9000/data/raw/USvideos.csv")
USvideos_df.write.format("avro").mode("overwrite").save("hdfs://namenode:9000/storage/hdfs/raw/avro/USvideos")

json_files = [
    "hdfs://namenode:9000/data/raw/CA_category_id.json",
    "hdfs://namenode:9000/data/raw/US_category_id.json",
    "hdfs://namenode:9000/data/raw/RU_category_id.json",
    "hdfs://namenode:9000/data/raw/DE_category_id.json",
    "hdfs://namenode:9000/data/raw/FR_category_id.json",
    "hdfs://namenode:9000/data/raw/GB_category_id.json",
    "hdfs://namenode:9000/data/raw/IN_category_id.json",
    "hdfs://namenode:9000/data/raw/JP_category_id.json",
    "hdfs://namenode:9000/data/raw/KR_category_id.json",
    "hdfs://namenode:9000/data/raw/MX_category_id.json"
]

for json_path in json_files:
    # Učitaj ceo fajl kao listu linija
    json_lines = spark.sparkContext.textFile(json_path).collect()
    json_str = "".join(json_lines)

    # Parsiraj JSON u Python objekat
    json_obj = json.loads(json_str)
    items = json_obj['items']

    rdd = spark.sparkContext.parallelize(items).map(lambda x: json.dumps(x))

    df = spark.read.json(rdd)

    df_selected = df.select(
        col("id").alias("category_id"),
        col("snippet.title").alias("title"),
        col("snippet.assignable").alias("assignable")
    )

    df_selected.show(truncate=False)

    filename = json_path.split("/")[-1].replace(".json", "")
    output_path = f"hdfs://namenode:9000/storage/hdfs/raw/avro/{filename}"

    df_selected.write.format("avro").mode("overwrite").save(output_path)
