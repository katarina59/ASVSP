FROM apache/airflow:2.8.1-python3.10
USER root
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean && \
    echo "JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))" >> /etc/environment
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow
RUN pip install --no-cache-dir \
    --timeout=300 \
    flask-session>=0.6.0 \
    apache-airflow-providers-apache-spark==4.1.3 \
    pyspark==3.3.0