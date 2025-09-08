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
    dag_id="youtube_pipeline_batch_data",
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


    ingest_raw = BashOperator(
    task_id="ingest_raw",
    bash_command=(
        "docker exec spark-master "
        "spark-submit "
        "--master spark://spark-master:7077 "
        "--deploy-mode client "
        "--packages org.apache.spark:spark-avro_2.12:3.5.1 "
        "/opt/airflow/jobs/ingestion/ingest_raw_to_hdfs.py"
    ),
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

    
    transform_golden = BashOperator(
        task_id="transform_golden",
        bash_command=(
            "docker exec spark-master "
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--packages org.apache.spark:spark-avro_2.12:3.5.1 "
            "/opt/airflow/jobs/transformation/transform_to_golden_dataset.py"
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


    spark_batch_analystics = BashOperator(
        task_id="spark_batch_analystics",
        bash_command=(
            "docker exec spark-master "
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--executor-memory 2G "
            "--driver-memory 2G "
            "--conf spark.executor.cores=2 "
            "--packages org.postgresql:postgresql:42.7.6 "
            "/opt/airflow/jobs/transformation/spark_batch_analystics_datalake.py"
        ),
    )


    final_task = DummyOperator(task_id="final_task")


    check_raw_files >> ingest_raw >> validate_ingest
    
    validate_ingest >> transform_golden >> validate_transform

    validate_transform >> spark_batch_analystics >> final_task