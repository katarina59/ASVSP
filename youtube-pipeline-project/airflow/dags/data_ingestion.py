from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
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
    dag_id="data_ingestion",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=start_date,
) as dag:

    check_raw_files = BashOperator(
        task_id="check_raw_files",
        bash_command="""
        if docker exec namenode hdfs dfs -test -e /data/raw/; then
            echo "Found raw files"
        else
            echo "No raw files found, exiting" && exit 1
        fi
        """,
    )

    raw_data_ingestion = BashOperator(
        task_id="raw_data_ingestion",
        bash_command="""
        docker exec namenode hdfs dfs -mkdir -p /storage/hdfs/raw
        docker exec namenode hdfs dfs -cp -f "/data/raw/*.csv" /storage/hdfs/raw/
        docker exec namenode hdfs dfs -cp -f "/data/raw/*.json" /storage/hdfs/raw/
        """,
    )

    validate_ingest = BashOperator(
        task_id="validate_ingest",
        bash_command="""
        FILE_COUNT=$(docker exec namenode hdfs dfs -ls /storage/hdfs/raw | wc -l)
        if [ "$FILE_COUNT" -lt 1 ]; then
            echo "Ingest failed: no files in HDFS raw folder" && exit 1
        else
            echo "Ingest OK"
        fi
        """,
    )

    transform_data = BashOperator(
        task_id="transform_data",
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
            "--py-files /opt/airflow/jobs/transformation/transform_data_common.py "
            "/opt/airflow/jobs/transformation/transform_data.py --mode initial"
        ),
    )

    validate_transform = BashOperator(
        task_id="validate_transform",
        bash_command="""
        ROW_COUNT=$(docker exec namenode hdfs dfs -cat /storage/hdfs/processed/golden_dataset/part-* | wc -l)
        if [ "$ROW_COUNT" -lt 1 ]; then
            echo "Transform failed: no rows in golden dataset" && exit 1
        else
            echo "Transform OK ($ROW_COUNT rows)"
        fi

        """,
    )

    trigger_dag2 = TriggerDagRunOperator(
        task_id="trigger_dag2",
        trigger_dag_id="batch_query",
        wait_for_completion=False,
    )

    check_raw_files >> raw_data_ingestion >> validate_ingest

    validate_ingest >> transform_data >> validate_transform

    validate_transform >> trigger_dag2
