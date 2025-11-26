from datetime import datetime
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable


BUCKET = "example"
DAG_ID = "s3_minio_sensor_always"


def detect_new_file(**context):
    hook = S3Hook(aws_conn_id="minio_s3")
    files = hook.list_keys(bucket_name=BUCKET)

    if not files:
        return False  

    files = sorted(files)
    newest = files[-1]

    last_processed = Variable.get("last_processed_file", default_var=None)

    if last_processed != newest:
        context["ti"].xcom_push(key="new_file", value=newest)
        return True

    return False


def process_new_file(**context):
    new_file = context["ti"].xcom_pull(key="new_file", task_ids="wait_for_new_file")
    print(f"Detected NEW file: {new_file}")
    Variable.set("last_processed_file", new_file)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,     # Run manually only once
    catchup=False,
    tags=["sensor", "minio"],
) as dag:

    wait_for_new_file = PythonSensor(
        task_id="wait_for_new_file",
        python_callable=detect_new_file,
        poke_interval=20,
        timeout=60 * 30,    # 30 minutes before failing
        mode="poke",
    )

    process_file = PythonOperator(
        task_id="process_file",
        python_callable=process_new_file,
    )

    # 🔄 TRIGGER ITSELF TO RUN AGAIN
    restart = TriggerDagRunOperator(
        task_id="restart_dag",
        trigger_dag_id=DAG_ID,
        reset_dag_run=True,
        wait_for_completion=False,
    )

    wait_for_new_file >> process_file >> restart
