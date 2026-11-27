"""Daily immutable JSON ingestion with task-level retries and date backfills."""
from datetime import datetime, timedelta, timezone
import os

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="pipelinex_retail",
    schedule="@daily",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1),
                  "retry_exponential_backoff": True, "execution_timeout": timedelta(minutes=30)},
) as dag:
    run_lakehouse = BashOperator(
        task_id="run_lakehouse",
        bash_command='python -m pipelinex.spark_engine "/data/source/$BATCH.json" "$LAKEHOUSE" "$BATCH"',
        env={"BATCH": "{{ ds_nodash }}", "LAKEHOUSE": os.environ.get("PIPELINEX_LAKEHOUSE", "/data/lakehouse")},
        append_env=True,
    )
