import subprocess

from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import ShortCircuitOperator # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from datetime import timedelta
import pendulum # type: ignore

start_date = pendulum.datetime(2025, 8, 1, tz="UTC")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "start_date": start_date,
}

# Ista dd.MM.yy -> date logika kao u transform_data_common.py, prevedena u SQL,
# da bi filter_new_data mogao da proveri ima li novih redova bez pokretanja Sparka.
NEW_ROWS_COUNT_SQL = (
    "SELECT COUNT(*) FROM sink_data.sink_videos "
    "WHERE to_date("
    "split_part(trending_date,'.',2) || '.' || split_part(trending_date,'.',3) || '.' || split_part(trending_date,'.',1), "
    "'DD.MM.YY') > '{watermark}'::date;"
)


def filter_new_data(**context):
    ti = context["ti"]
    xcom_val = ti.xcom_pull(task_ids="compute_watermark")
    watermark = xcom_val.split("=", 1)[1].strip()

    result = subprocess.run(
        [
            "docker", "exec", "postgres", "psql", "-U", "airflow", "-d", "airflow",
            "-t", "-c", NEW_ROWS_COUNT_SQL.format(watermark=watermark),
        ],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"psql upit za filter_new_data nije uspeo: {result.stderr.strip()}")

    count = int(result.stdout.strip() or 0)
    print(f"Novih redova u sink_data iznad watermarka {watermark}: {count}")
    return count > 0


with DAG(
    dag_id="incremental_watermark_check",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False,
    start_date=start_date,
) as dag:

    compute_watermark = BashOperator(
        task_id="compute_watermark",
        bash_command=(
            "set -o pipefail && docker exec spark-master "
            "/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--executor-memory 1G "
            "--driver-memory 1G "
            "/opt/airflow/jobs/transformation/compute_watermark.py | tail -n 1"
        ),
    )

    filter_new_data_task = ShortCircuitOperator(
        task_id="filter_new_data",
        python_callable=filter_new_data,
    )

    trigger_transform_data = BashOperator(
        task_id="trigger_transform_data",
        bash_command=(
            "docker exec spark-master "
            "/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--executor-memory 2G "
            "--driver-memory 2G "
            "--conf spark.executor.memoryOverhead=512m "
            "--conf spark.sql.shuffle.partitions=2 "
            "--conf spark.executor.cores=1 "
            "--packages org.postgresql:postgresql:42.7.6 "
            "--py-files /opt/airflow/jobs/transformation/transform_data_common.py "
            "/opt/airflow/jobs/transformation/transform_data.py "
            "--mode incremental "
            "--watermark \"{{ ti.xcom_pull(task_ids='compute_watermark').split('=')[1] }}\""
        ),
    )

    trigger_batch_query = TriggerDagRunOperator(
        task_id="trigger_batch_query",
        trigger_dag_id="batch_query",
        wait_for_completion=False,
    )

    compute_watermark >> filter_new_data_task >> trigger_transform_data >> trigger_batch_query
