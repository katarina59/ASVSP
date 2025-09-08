from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.dummy import DummyOperator # type: ignore
from datetime import timedelta
import pendulum # type: ignore

start_date = pendulum.datetime(2025, 8, 1, tz="UTC")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    'start_date': start_date,
}


with DAG(
    dag_id="youtube_pipeline_streaming_query1",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=start_date,
) as dag:
    

    query1 = BashOperator(
    task_id="query1",
    bash_command=(
        "docker exec spark-master "
        "spark-submit "
        "--master spark://spark-master:7077 "
        "--deploy-mode client "
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.6 "
        "/opt/spark_apps/youtube_streaming_query1.py"
    ),
)



    final_task = DummyOperator(task_id="final_task")


    query1 >> final_task