from pyspark.sql import SparkSession # type: ignore

spark = SparkSession.builder \
    .appName("Golden Dataset Check") \
    .getOrCreate()

# Učitaj golden dataset iz HDFS-a
df = spark.read.parquet("hdfs://namenode:9000/storage/hdfs/processed/golden_dataset")

# Prikaži šemu (strukturu) podataka
print("Šema dataset-a:")
df.printSchema()

# Prikaz prvih 10 redova
print("Prvih 10 redova:")
df.show(10, truncate=False)

# Broj ukupnih redova
total_rows = df.count()
print(f"Ukupan broj redova: {total_rows}")

# Broj redova po regionu
print("Broj redova po regionu:")
df.groupBy("region").count().show()

spark.stop()
