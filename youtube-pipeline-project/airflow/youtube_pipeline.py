import os
from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.operators.dummy import DummyOperator # type: ignore
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}


# SQL_DIR = "/opt/airflow/sql"



with DAG(
    dag_id="youtube_pipeline_batch_data",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # # --- CHECK RAW FILES ---
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

    # # # --- DROP EXISTING VIEWS ---
    # drop_views = BashOperator(
    #     task_id="drop_views",
    #     bash_command=(
    #         # "PGPASSWORD=citus psql "
    #         # "-h citus_coordinator "
    #         # "-U citus "
    #         # "-d youtube_dw "
    #         "docker exec citus_coordinator "
    #         "psql -U citus -d youtube_dw "
    #         "-c \"DROP VIEW IF EXISTS vw_q01_avg_views_comments CASCADE; "
    #         "DROP VIEW IF EXISTS vw_q02_top_engagement_channels CASCADE; "
    #         "DROP VIEW IF EXISTS vw_q03_gold_combinations CASCADE; "
    #         "DROP VIEW IF EXISTS vw_q04_problem_types CASCADE; "
    #         "DROP VIEW IF EXISTS vw_q05_tags_core_metrics CASCADE; "
    #         "DROP VIEW IF EXISTS vw_q06_magic_tags CASCADE; "
    #         "DROP VIEW IF EXISTS vw_q07_speed_to_viral CASCADE; "
    #         "DROP VIEW IF EXISTS vw_q08_desc_thumb_performance CASCADE; "
    #         "DROP VIEW IF EXISTS vw_q09_seasonal_launch CASCADE; "
    #         "DROP VIEW IF EXISTS vw_q10_channel_top_video CASCADE;\""
    #     ),
    # )



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

    # # --- VALIDATION AFTER INGEST ---
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


    # # --- VALIDATION AFTER TRANSFORM ---
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
            "/opt/airflow/jobs/transformation/spark_batch_analystics_datalake.py"
        ),
    )
    
    # golden_to_star = BashOperator(
    #     task_id="golden_to_star",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--packages org.postgresql:postgresql:42.6.0 "
    #         "/opt/airflow/jobs/star_schema/golden_to_star_citus.py"
    #     ),
    # )
    

    #  # SQL taskovi sa BashOperator i psql
    # sql_tasks = []
    
    # # Lista SQL fajlova (dodaj sve svoje SQL fajlove ovde)
    # sql_files = [
    #     "q_01_avg_views_comments.sql",
    #     "q_02_top_engagement_channels.sql",
    #     "q_03_gold_combinations.sql",
    #     "q_04_problem_types.sql",
    #     "q_05_tags_core_metrics.sql",
    #     "q_06_magic_tags.sql",
    #     "q_07_speed_to_viral.sql",
    #     "q_08_desc_thumb_performance.sql",
    #     "q_09_seasonal_launch.sql",
    #     "q_10_channel_top_video.sql"
    # ]
    
    # for sql_file in sql_files:
    #     task = BashOperator(
    #         task_id=f"run_{sql_file.replace('.sql', '').replace('-', '_').replace(' ', '_')}",
    #         bash_command=(
    #             f"PGPASSWORD=citus psql "
    #             f"-h citus_coordinator "
    #             f"-U citus "
    #             f"-d youtube_dw "
    #             f"-f /opt/airflow/sql/{sql_file}"
    #         ),
    #     )
    #     sql_tasks.append(task)

    # Opcioni DummyOperator da se svi paralelni SQL taskovi završe pre nečeg daljeg
    final_task = DummyOperator(task_id="final_task")


    check_raw_files >> ingest_raw >> validate_ingest
    
    validate_ingest >> transform_golden >> validate_transform

    # validate_transform >> golden_to_star

    validate_transform >> spark_batch_analystics >> final_task
    
    # # Svi SQL taskovi se pokreću nakon golden_to_star
    # for sql_task in sql_tasks:
    #     golden_to_star >> sql_task >> final_task