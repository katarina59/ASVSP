from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from datetime import timedelta
import pendulum # type: ignore

start_date = pendulum.datetime(2025, 8, 1, tz="UTC")

QUERY_NUMBERS = [1, 2, 3, 4, 5]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "start_date": start_date,
}

with DAG(
    dag_id="near_realtime_stop_all",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=start_date,
    tags=["manual", "streaming-control"],
) as dag:

    for n in QUERY_NUMBERS:
        BashOperator(
            task_id=f"stop_query{n}",
            bash_command=f"""
            docker exec spark-master pkill -TERM -f youtube_streaming_query{n}.py || echo "near_realtime_query{n}: Spark job nije bio aktivan"
            airflow dags pause near_realtime_query{n}
            echo "near_realtime_query{n}: Spark job zaustavljen i DAG pauziran"
            """,
        )
