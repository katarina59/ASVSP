FROM bitnami/spark:3.4

COPY . /app

WORKDIR /app

CMD ["spark-submit", "--master", "local[*]", "spark_streaming_job.py"]
