FROM bitnami/spark:3.5.0

# Instalacija potrebnih Java dependency-ja
USER root

# Instaliraj curl i ostale alate
# RUN install_packages curl wget bash netcat-traditional


# Instaliraj Python dependencije
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Preuzmi potrebne JAR fajlove
# RUN curl -o /opt/bitnami/spark/jars/kafka-clients-3.6.1.jar \
#     https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar && \
#     curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
#     https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
#     curl -o /opt/bitnami/spark/jars/postgresql-42.7.0.jar \
#     https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.0/postgresql-42.7.0.jar && \
#     curl -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
#     https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
#     curl -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar \
#     https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar



# # Kopiraj aplikaciju i skriptu u /opt/spark_apps
# COPY . /opt/spark_apps

# # Omogući izvršenje wait-for-services.sh
# RUN chmod +x /opt/spark_apps/wait-for-services.sh

# # Vrati se na spark korisnika
USER 1001

# # Radni direktorijum
# WORKDIR /opt/spark_apps

# Pokretanje
# CMD ["./wait-for-services.sh"]
