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
    "start_date": start_date,
}


with DAG(
    dag_id="batch_query",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=start_date,
) as dag:

    check_golden_dataset = BashOperator(
        task_id="check_golden_dataset",
        bash_command="""
        if docker exec namenode hdfs dfs -test -e /storage/hdfs/processed/golden_dataset/; then
            echo "Golden dataset found"
        else
            echo "Golden dataset not found — pokreni data_ingestion DAG prvo" && exit 1
        fi
        """,
    )

    ensure_schemas = BashOperator(
        task_id="ensure_schemas",
        bash_command="docker exec -i postgres psql -U airflow -d airflow < /opt/airflow/sql/schema_setup.sql",
    )

    run_batch_queries = BashOperator(
        task_id="run_batch_queries",
        bash_command=(
            "docker exec spark-master "
            "/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--executor-memory 3G "
            "--driver-memory 2G "
            "--conf spark.executor.memoryOverhead=768m "
            "--conf spark.sql.shuffle.partitions=8 "
            "--conf spark.executor.cores=1 "
            "--packages org.postgresql:postgresql:42.7.6 "
            "/opt/airflow/jobs/transformation/spark_batch_analystics_datalake.py"
        ),
    )

    validate_queries = BashOperator(
        task_id="validate_queries",
        bash_command="""
        for TABLE in query1_category_region_analysis query2_channel_engagement query3_viral_golden_combinations_proba query4_problem_analysis_proba_2 query5_tag_viral_analysis query6_advanced_tag_recommendations query7_fastest_viral_channels query8_content_optimization query9_optimal_launch_timing query10_top_channels_mega_hits query11_trending_persistence query12_tags_by_region_category query13_title_patterns query14_publish_heatmap; do
            ROW_COUNT=$(docker exec postgres psql -U airflow -d airflow -t -c "SELECT COUNT(*) FROM batch_data_queries.$TABLE;" 2>/dev/null | tr -d ' ')
            if [ -z "$ROW_COUNT" ] || [ "$ROW_COUNT" -lt 1 ]; then
                echo "Validacija neuspešna: tabela batch_data_queries.$TABLE je prazna ili ne postoji" && exit 1
            else
                echo "batch_data_queries.$TABLE OK ($ROW_COUNT redova)"
            fi
        done
        """,
    )

    final_task = DummyOperator(task_id="final_task")

    check_golden_dataset >> ensure_schemas >> run_batch_queries >> validate_queries >> final_task
