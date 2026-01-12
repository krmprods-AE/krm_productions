from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

with DAG(
    dag_id="spark_submit_simple",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="spark-fetch-correct",
        application="s3a://spark-jobs/job.py",  # or s3a://... if you want
        conn_id="spark_standalone",
        conn_id="spark_standalone",
        deploy_mode="client",
        name="airflow-spark-job",
        verbose=True,

        # 🔑 THIS IS THE KEY LINE
        spark_binary="/opt/spark/bin/spark-submit",
    )
