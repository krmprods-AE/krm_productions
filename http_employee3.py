from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
import json

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="http_operator_employee3_2_Different_Spark_Jobs",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["http","spark","spark_gateway","employee3","S3","2_jobs"],
) as dag:

    submit_spark_job1 = SimpleHttpOperator(
        task_id="submit_spark_job",
        http_conn_id="employee3",
        endpoint="/submit-job",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "jar": "s3a://spark-jobs/app_read_join.py",        # your API still uses "jar"
            "args": []              # optional args
        }),
        response_filter=lambda response: response.json(),
        log_response=True,
    )

    submit_spark_job2 = SimpleHttpOperator(
        task_id="submit_spark_job",
        http_conn_id="employee3",
        endpoint="/submit-job",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "jar": "s3a://spark-jobs/1_csv_write.py",        # your API still uses "jar"
            "args": []              # optional args
        }),
        response_filter=lambda response: response.json(),
        log_response=True,
    )

    submit_spark_job1 >> submit_spark_job2
