FROM bitnami/spark:3.5.0

# Instalacija potrebnih Java dependency-ja
USER root

# Instaliraj curl i ostale alate
RUN install_packages curl wget bash

# Preuzmi potrebne JAR fajlove
RUN curl -o /opt/bitnami/spark/jars/kafka-clients-3.6.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar

RUN curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

RUN curl -o /opt/bitnami/spark/jars/postgresql-42.7.0.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.0/postgresql-42.7.0.jar

RUN curl -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

RUN curl -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Vrati se na spark korisnika
USER 1001

# Kopiraj aplikaciju
COPY --chown=1001:1001 . /opt/spark_apps/

# Wait script za čekanje dependency-ja
COPY --chown=1001:1001 wait-for-services.sh /opt/spark_apps/
RUN chmod +x /opt/spark_apps/wait-for-services.sh

# Pokretanje
WORKDIR /opt/spark_apps
CMD ["./wait-for-services.sh"]