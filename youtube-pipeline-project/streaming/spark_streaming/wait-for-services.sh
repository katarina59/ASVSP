#!/bin/bash
set -e

echo "Čekam da servisi budu spremni..."

# Čekaj Kafka
echo "Čekam Kafka..."
while ! nc -z kafka 9092; do
  sleep 5
done
echo "Kafka spreman!"


# Čekaj Spark Master
echo "Čekam Spark Master..."
while ! nc -z spark-master 7077; do
  sleep 5
done
echo "Spark Master spreman!"

echo "Svi servisi spremni - pokrećem Spark Streaming..."

# Pokreni Spark aplikaciju
spark-submit \
  --master $SPARK_MASTER_URL \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  --conf spark.sql.streaming.checkpointLocation=/opt/spark_apps/checkpoint \
  youtube_streaming_analytics.py
