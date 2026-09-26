from pyspark.sql import SparkSession # type: ignore

GOLDEN_DATASET_PATH = "hdfs://namenode:9000/storage/hdfs/processed/golden_dataset"


def main():
    spark = SparkSession.builder.appName("compute_watermark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    golden_df = spark.read.parquet(GOLDEN_DATASET_PATH)
    watermark = golden_df.agg({"trending_full_date": "max"}).first()[0]

    spark.stop()

    print(f"WATERMARK={watermark}")


if __name__ == "__main__":
    main()
