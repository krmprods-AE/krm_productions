from datetime import datetime
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.models import Variable


BUCKET = "example"


def detect_new_file(**context):
    hook = S3Hook(aws_conn_id="minio_s3")
    files = hook.list_keys(bucket_name=BUCKET, prefix="", delimiter=None)

    if not files:
        return False   # no files yet

    # Sort files by name (or we can sort by last modified)
    files = sorted(files)

    # Get last processed file stored in Airflow
    last_processed = Variable.get("last_processed_file", default_var=None)

    # Find the newest file
    newest_file = files[-1]

    # If we have never processed any file yet
    if last_processed is None:
        context["ti"].xcom_push(key="new_file", value=newest_file)
        return True

    # Compare: is this actually newer than the previous one?
    if newest_file != last_processed:
        context["ti"].xcom_push(key="new_file", value=newest_file)
        return True

    return False



def process_new_file(**context):
    """Process the newly detected file."""
    new_file = context["ti"].xcom_pull(key="new_file", task_ids="wait_for_new_file")
    print(f"Detected NEW file: {new_file}")

    # Save new file as "last processed"
    Variable.set("last_processed_file", new_file)



with DAG(
    dag_id="s3_minio_sensor_always",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["minio", "sensor", "new-file"],
) as dag:

    wait_for_new_file = PythonSensor(
        task_id="wait_for_new_file",
        python_callable=detect_new_file,
        poke_interval=20,
        timeout=60 * 10,
        mode="poke", # xwris provide_context=True, to sensor mono to python operator        
    )

    process_file = PythonOperator(
        task_id="process_file",
        python_callable=process_new_file,
        provide_context=True,
    )

    wait_for_new_file >> process_file
