import os
from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.dummy import DummyOperator # type: ignore
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain, cross_downstream # type: ignore

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
    # check_raw_files = BashOperator(
    #     task_id="check_raw_files",
    #     bash_command='docker exec namenode hdfs dfs -test -e /data/raw/ || echo "Path not found"',
    # )

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
        "/opt/airflow/jobs/ingestion/ingest_raw_to_hdfs.py"
    ),
)


    # # --- VALIDATION AFTER INGEST ---
    # validate_ingest = BashOperator(
    #     task_id="validate_ingest",
    #     bash_command="""
    #     FILE_COUNT=$(docker exec namenode hdfs dfs -ls /storage/hdfs/raw | wc -l)
    #     if [ "$FILE_COUNT" -lt 1 ]; then
    #         echo "Ingest failed: no files in HDFS raw folder" && exit 1
    #     else
    #         echo "Ingest OK"
    #     fi
    #     """,
    # )

    
    transform_golden = BashOperator(
        task_id="transform_golden",
        bash_command=(
            "docker exec spark-master "
            "spark-submit "
            "/opt/airflow/jobs/transformation/transform_to_golden_dataset.py"
        ),
    )


    # # --- VALIDATION AFTER TRANSFORM ---
    # validate_transform = BashOperator(
    #     task_id="validate_transform",
    #     bash_command="""
    #     ROW_COUNT=$(docker exec namenode hdfs dfs -cat /storage/hdfs/processed/golden_dataset/part-* | wc -l)
    #     if [ "$ROW_COUNT" -lt 1 ]; then
    #         echo "Transform failed: no rows in golden dataset" && exit 1
    #     else
    #         echo "Transform OK ($ROW_COUNT rows)"
    #     fi

    #     """,
    # )


    spark_batch_analystics = BashOperator(
        task_id="spark_batch_analystics",
        bash_command=(
            "docker exec spark-master "
            "spark-submit "
            "/opt/airflow/jobs/transformation/spark_batch_analystics_datalake.py"
        ),
    )

    # spark_batch_query1 = BashOperator(
    #     task_id="spark_batch_query1",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2g "
    #         "--total-executor-cores 4 "
    #         "/opt/airflow/jobs/analytics/spark_batch_query1.py"
    #     ),
    # )

    # spark_batch_query2 = BashOperator(
    #     task_id="spark_batch_query2",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2g "
    #         "--total-executor-cores 4 "
    #         "/opt/airflow/jobs/analytics/spark_batch_query2.py"
    #     ),
    # )

    # spark_batch_query3 = BashOperator(
    #     task_id="spark_batch_query3",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2g "
    #         "--total-executor-cores 4 "
    #         "/opt/airflow/jobs/analytics/spark_batch_query3.py"
    #     ),
    # )

    # spark_batch_query4 = BashOperator(
    #     task_id="spark_batch_query4",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2g "
    #         "--total-executor-cores 4 "
    #         "/opt/airflow/jobs/analytics/spark_batch_query4.py"
    #     ),
    # )

    # spark_batch_query5 = BashOperator(
    #     task_id="spark_batch_query5",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2g "
    #         "--total-executor-cores 4 "
    #         "/opt/airflow/jobs/analytics/spark_batch_query5.py"
    #     ),
    # )

    # spark_batch_query6 = BashOperator(
    #     task_id="spark_batch_query6",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2g "
    #         "--total-executor-cores 4 "
    #         "/opt/airflow/jobs/analytics/spark_batch_query6.py"
    #     ),
    # )

    # spark_batch_query7 = BashOperator(
    #     task_id="spark_batch_query7",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2g "
    #         "--total-executor-cores 4 "
    #         "/opt/airflow/jobs/analytics/spark_batch_query7.py"
    #     ),
    # )

    # spark_batch_query8 = BashOperator(
    #     task_id="spark_batch_query8",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2g "
    #         "--total-executor-cores 4 "
    #         "/opt/airflow/jobs/analytics/spark_batch_query8.py"
    #     ),
    # )

    # spark_batch_query9 = BashOperator(
    #     task_id="spark_batch_query9",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2g "
    #         "--total-executor-cores 4 "
    #         "/opt/airflow/jobs/analytics/spark_batch_query9.py"
    #     ),
    # )

    # spark_batch_query10 = BashOperator(
    #     task_id="spark_batch_query10",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2g "
    #         "--total-executor-cores 4 "
    #         "/opt/airflow/jobs/analytics/spark_batch_query10.py"
    #     ),
    # )

    # spark_batch_query_bonus3_final_report = BashOperator(
    #     task_id="spark_batch_query_bonus3_final_report",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2g "
    #         "--total-executor-cores 4 "
    #         "/opt/airflow/jobs/analytics/spark_batch_query_bonus3_final_report.py"
    #     ),
    # )
    
    
    # queries_into_postgres = BashOperator(
    #     task_id="queries_into_postgres",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2G "
    #         "--driver-memory 2G "
    #         "--conf spark.executor.cores=2 "
    #         "--packages org.postgresql:postgresql:42.7.6 "
    #         "/opt/airflow/jobs/queries/queries_into_postgres.py"
    #     ),
    # )

    # query1_into_postgres = BashOperator(
    #     task_id="query1_into_postgres",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2G "
    #         "--driver-memory 2G "
    #         "--conf spark.executor.cores=2 "
    #         "--packages org.postgresql:postgresql:42.7.6 "
    #         "/opt/airflow/jobs/queries/query1_into_postgres.py"
    #     ),
    # )
    
    # query2_into_postgres = BashOperator(
    #     task_id="query2_into_postgres",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2G "
    #         "--driver-memory 2G "
    #         "--conf spark.executor.cores=2 "
    #         "--packages org.postgresql:postgresql:42.7.6 "
    #         "/opt/airflow/jobs/queries/query2_into_postgres.py"
    #     ),
    # )

    # query3_into_postgres = BashOperator(
    #     task_id="query3_into_postgres",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2G "
    #         "--driver-memory 2G "
    #         "--conf spark.executor.cores=2 "
    #         "--packages org.postgresql:postgresql:42.7.6 "
    #         "/opt/airflow/jobs/queries/query3_into_postgres.py"
    #     ),
    # )

    # query4_into_postgres = BashOperator(
    #     task_id="query4_into_postgres",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2G "
    #         "--driver-memory 2G "
    #         "--conf spark.executor.cores=2 "
    #         "--packages org.postgresql:postgresql:42.7.6 "
    #         "/opt/airflow/jobs/queries/query4_into_postgres.py"
    #     ),
    # )

    # query5_into_postgres = BashOperator(
    #     task_id="query5_into_postgres",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--deploy-mode client "
    #         "--executor-memory 2G "
    #         "--driver-memory 2G "
    #         "--conf spark.executor.cores=2 "
    #         "--packages org.postgresql:postgresql:42.7.6 "
    #         "/opt/airflow/jobs/queries/query5_into_postgres.py"
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


    # check_raw_files >> ingest_raw >> validate_ingest
    
    # validate_ingest >> transform_golden >> validate_transform

    # validate_transform >> spark_batch_analystics >> final_task

    # validate_transform >> golden_to_star


    # SEKVENCIJALNO IZVRSAVANJE DATAPIPELINE
    # validate_transform >> spark_batch_analystics >> [query1_into_postgres,
    #                                                  query2_into_postgres,
    #                                                  query3_into_postgres,
    #                                                  query4_into_postgres,
    #                                                  query5_into_postgres] >>final_task


    # PARALELNO IZVRSAVANJE DATAPIPELINE
    # validate_transform >> [spark_batch_query1,
    #                        spark_batch_query2,
    #                        spark_batch_query3] 
                           
                           
    # # Zatim veza između listi
    # cross_downstream(
    # [spark_batch_query1, spark_batch_query2, spark_batch_query3],
    # [spark_batch_query4, spark_batch_query5, spark_batch_query6])

    # cross_downstream(
    # [spark_batch_query4, spark_batch_query5, spark_batch_query6],
    # [spark_batch_query7, spark_batch_query8, spark_batch_query9, spark_batch_query10])
                           
    # [spark_batch_query7, spark_batch_query8, spark_batch_query9, spark_batch_query10] >> spark_batch_query_bonus3_final_report
    

    # spark_batch_query_bonus3_final_report >> [query1_into_postgres,
    #                                           query2_into_postgres,
    #                                           query3_into_postgres,
    #                                           query4_into_postgres,
    #                                           query5_into_postgres] >>final_task
                                                        
    # # Svi SQL taskovi se pokreću nakon golden_to_star
    # for sql_task in sql_tasks:
    #     golden_to_star >> sql_task >> final_task




#ovo je verzija:

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
        "docker exec spark_master "
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
            "docker exec spark_master "
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
            "docker exec spark_master "
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