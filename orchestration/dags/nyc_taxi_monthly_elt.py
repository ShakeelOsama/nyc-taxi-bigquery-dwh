"""
Monthly NYC taxi ELT on Cloud Composer (Airflow 2.x).

TLC publishes each month's file with roughly a two-month lag and occasionally republishes
the previous month, so every run (re)loads the two most recent published months. Because
raw loads are per-partition WRITE_TRUNCATE and dbt reprocesses only months whose
_ingested_at moved, re-running any DAG run is safe and cheap.

Backfill a range:  airflow dags backfill nyc_taxi_monthly_elt -s 2023-03-01 -e 2025-03-01
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

PROJECT = Variable.get("gcp_project", default_var="your-gcp-project")
BUCKET = Variable.get("tlc_landing_bucket", default_var=f"{PROJECT}-nyc-tlc-landing")
REPO = "/home/airflow/gcs/data/nyc-taxi-dwh"
DBT = f"cd {REPO}/dbt && dbt --no-use-colors"
FLAGS = "--target prod --profiles-dir ."

# months to load for this run: the month published ~2 months ago, plus the one before it
LATEST = "{{ (data_interval_start - macros.dateutil.relativedelta.relativedelta(months=2)).strftime('%Y-%m') }}"
PREVIOUS = "{{ (data_interval_start - macros.dateutil.relativedelta.relativedelta(months=3)).strftime('%Y-%m') }}"


def _tlc_file_published(ym: str) -> bool:
    import requests
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ym}.parquet"
    return requests.head(url, timeout=30).status_code == 200


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "email": ["data-eng@example.com"],
}

with DAG(
    dag_id="nyc_taxi_monthly_elt",
    description="NYC TLC yellow taxi: TLC CDN -> GCS -> raw -> cleaned -> business",
    schedule="0 6 5 * *",                          # 5th of each month, 06:00 UTC
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args=default_args,
    tags=["nyc-taxi", "bigquery", "dbt"],
) as dag:

    wait_for_file = PythonSensor(
        task_id="wait_for_tlc_file",
        python_callable=_tlc_file_published,
        op_kwargs={"ym": LATEST},
        mode="reschedule",
        poke_interval=6 * 60 * 60,                  # check every 6h, for up to 10 days
        timeout=10 * 24 * 60 * 60,
    )

    ingest = BashOperator(
        task_id="ingest_raw",
        bash_command=(
            f"python {REPO}/ingestion/load_tlc_month.py --project {PROJECT} --bucket {BUCKET} "
            f"--months {PREVIOUS} {LATEST}"
        ),
        execution_timeout=timedelta(hours=1),
    )

    deps = BashOperator(task_id="dbt_deps", bash_command=f"{DBT} deps && {DBT} seed {FLAGS}")

    freshness = BashOperator(task_id="source_freshness", bash_command=f"{DBT} source freshness {FLAGS}")

    cleaned = BashOperator(
        task_id="build_cleaned",
        bash_command=f"{DBT} build --select path:models/cleaned {FLAGS}",
    )

    business = BashOperator(
        task_id="build_business",
        bash_command=f"{DBT} build --select path:models/business path:tests {FLAGS}",
    )

    wait_for_file >> ingest >> deps >> freshness >> cleaned >> business
