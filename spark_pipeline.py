from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="spark_http_pipeline",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    submit_spark_job = SimpleHttpOperator(
        task_id="submit_spark_job",
        http_conn_id="spark_gateway_http",
        endpoint="/submit-job",
        method="POST",
        headers={"Content-Type": "application/json"},
        data="""
        {
            "jar": "process_data.py", 
            "args": []
        }
        """, # "jar": "process_data.py", this must be inside the spark gateway's target directory
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )

    submit_spark_job