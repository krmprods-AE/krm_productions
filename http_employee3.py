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
def parquet_exists():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="mycustomuser",
        aws_secret_access_key="pakekfoeo3030d3*(&&&(*!",
    )

    response = s3.list_objects_v2(
        Bucket="analytics",
        Prefix="joined_orders/"
    )

    if "Contents" not in response:
        return False

    return any(obj["Key"].endswith(".parquet") for obj in response["Contents"])

with DAG(
    dag_id="http_operator_employee3_2_Different_Spark_Jobs",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["http","spark","spark_gateway","employee3","S3","2_jobs","sensor","wait"],
) as dag:

    submit_spark_job1 = SimpleHttpOperator(
        task_id="submit_spark_job1",
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
        task_id="submit_spark_job2",
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
    wait_for_parquet = PythonSensor(
    task_id="wait_for_parquet_files",
    python_callable=parquet_exists,
    poke_interval=10,
    timeout=300,
    mode="reschedule",
        )

    submit_spark_job1 >> wait_for_parquet >> submit_spark_job2
