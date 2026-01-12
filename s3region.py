from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark-region-config",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_job = SparkSubmitOperator(
    task_id="spark-fetch-correct",
    application="s3a://spark-jobs/job.py",
    conn_id="spark_standalone",
    deploy_mode="client",
    name="airflow-spark-job",
    verbose=True,
    spark_binary="/opt/spark/bin/spark-submit",
    conf={
        #"spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        #"spark.hadoop.fs.s3a.path.style.access": "true",
        #"spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

        "spark.hadoop.fs.s3a.access.key": "mycustomuser",
        "spark.hadoop.fs.s3a.secret.key": "pakekfoeo3030d3*(&&&(*!",
        "spark.hadoop.fs.s3a.aws.credentials.provider":
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

        # ✅ MinIO compatibility (region/signing)
        "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
        "spark.hadoop.fs.s3a.region": "us-east-1",
    },
)

