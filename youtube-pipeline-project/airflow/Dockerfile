# Dockerfile
FROM apache/airflow:2.10.2

USER root

# Install Java and Hadoop client
RUN apt-get update && apt-get install -y \
    default-jdk \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Download and install Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz && \
    tar -xzf hadoop-3.3.4.tar.gz -C /opt/ && \
    rm hadoop-3.3.4.tar.gz && \
    ln -s /opt/hadoop-3.3.4 /opt/hadoop

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

USER airflow