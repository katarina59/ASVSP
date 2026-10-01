from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import BranchPythonOperator # type: ignore
from airflow.operators.empty import EmptyOperator # type: ignore
from datetime import timedelta
import pendulum # type: ignore
import subprocess
import logging

start_date = pendulum.datetime(2025, 8, 1, tz="UTC")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=60),
    'start_date': start_date,
}

RESULT_TABLES = ["query2_category_trending", "query2_performance_vs_historical"]
SPARK_SCRIPT = "youtube_streaming_query2.py"

MOVE_STAGING_SQL = " ".join(
    f"BEGIN; "
    f"INSERT INTO real_time_data_queries.{t} SELECT * FROM real_time_data_queries.{t}_staging; "
    f"TRUNCATE real_time_data_queries.{t}_staging; "
    f"COMMIT;"
    for t in RESULT_TABLES
)


def check_spark_job_alive(**_context):
    result = subprocess.run(
        ["docker", "exec", "spark-master", "pgrep", "-f", SPARK_SCRIPT],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        logging.info("near_realtime_query2: Spark job je živ (pid=%s)", result.stdout.strip())
        return "job_already_running"
    logging.warning("near_realtime_query2: Spark job NIJE aktivan, pokrećem ponovo")
    return "start_spark_job"


with DAG(
    dag_id="near_realtime_query2",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    start_date=start_date,
    max_active_runs=1,
) as dag:

    check_alive = BranchPythonOperator(
        task_id="check_spark_job_alive",
        python_callable=check_spark_job_alive,
    )

    start_spark_job = BashOperator(
        task_id="start_spark_job",
        bash_command=f"""
        docker exec -d spark-master /spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 768m \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.6 \
            /opt/spark_apps/{SPARK_SCRIPT}
        echo "near_realtime_query2: Spark job pokrenut"
        """,
    )

    job_already_running = EmptyOperator(task_id="job_already_running")

    persist_results = BashOperator(
        task_id="persist_results",
        bash_command=f'docker exec postgres psql -U airflow -d airflow -c "{MOVE_STAGING_SQL}"',
        trigger_rule="none_failed_min_one_success",
    )

    check_alive >> [start_spark_job, job_already_running] >> persist_results
