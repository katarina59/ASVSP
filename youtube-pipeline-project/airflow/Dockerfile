FROM apache/airflow:2.10.2


# Instaliramo MySQL provider i driver.
# mysqlclient je brz (C‑ekstenzija). Alternativa je mysql-connector-python (pure Python), ali je sporiji.
RUN pip install --no-cache-dir \
apache-airflow-providers-mysql \
mysqlclient