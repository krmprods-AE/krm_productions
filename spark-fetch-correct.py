from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark_submit_simple",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="spark-fetch-correct",
        application="s3a://spark-jobs/job.py",
        conn_id="spark_standalone",          # MUST be Spark
        deploy_mode="client",
        name="airflow-spark-job",
        verbose=True,
        spark_binary="/opt/spark/bin/spark-submit",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "mycustomuser",
            "spark.hadoop.fs.s3a.secret.key": "mypassword",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
    )
