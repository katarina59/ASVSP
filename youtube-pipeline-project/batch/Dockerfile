FROM bitnami/spark:3.5.1

COPY ./ingestion/ingest_raw_to_hdfs.py /app/ingest_raw_to_hdfs.py

CMD ["spark-submit", "--packages", "org.apache.spark:spark-avro_2.12:3.5.1", "--conf", "spark.driver.bindAddress=0.0.0.0", "/app/ingest_raw_to_hdfs.py"]


