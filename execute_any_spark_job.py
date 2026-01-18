from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from airflow.sensors.python import PythonSensor
import boto3
import json

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="execute_any_spark_job",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["http","spark","spark_gateway","execute_any_spark_job"],
) as dag:

    submit_spark_job1 = SimpleHttpOperator(
        task_id="submit_spark_job1",
        http_conn_id="employee3",
        endpoint="/submit-job",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "jar": "s3a://spark-jobs/tet.py",        # your API still uses "jar"
            "args": []              # optional args
        }),
        response_filter=lambda response: response.json(),
        log_response=True,
    )