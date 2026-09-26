from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from datetime import timedelta
import pendulum # type: ignore

start_date = pendulum.datetime(2025, 8, 1, tz="UTC")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=60),
    'start_date': start_date,
}

RESULT_TABLES = ["query4_combined_analysis"]

MOVE_STAGING_SQL = " ".join(
    f"BEGIN; "
    f"INSERT INTO real_time_data_queries.{t} SELECT * FROM real_time_data_queries.{t}_staging; "
    f"TRUNCATE real_time_data_queries.{t}_staging; "
    f"COMMIT;"
    for t in RESULT_TABLES
)


with DAG(
    dag_id="near_realtime_query4",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    catchup=False,
    start_date=start_date,
) as dag:

    run_near_realtime_query = BashOperator(
        task_id="run_near_realtime_query",
        bash_command="""
        if docker exec spark-master pgrep -f youtube_streaming_query4.py > /dev/null; then
            echo "near_realtime_query4: Spark job već radi, preskačem pokretanje"
        else
            docker exec -d spark-master /spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --driver-memory 768m \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.6 \
                /opt/spark_apps/youtube_streaming_query4.py
            echo "near_realtime_query4: Spark job pokrenut"
        fi
        """,
    )

    persist_results = BashOperator(
        task_id="persist_results",
        bash_command=f'docker exec postgres psql -U airflow -d airflow -c "{MOVE_STAGING_SQL}"',
    )

    run_near_realtime_query >> persist_results
