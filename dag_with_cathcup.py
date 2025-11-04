from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args = { # to default_args einai ena dicitonary edw mporwi na exei opoiodhpote onoma
    'owner': 'kat2',
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}



with DAG(
    dag_id ='dag_with_catchup_v002',
    default_args=default_args,
    description='this is our first python operator dag',
    start_date =datetime(2025,5,28),
    schedule_interval='@daily',
    catchup=False # twra epeidi ekana to cathcup = false den trexei apo prin. omws ama thelw mporw na kanw "backfill" kai na trejw kai gi a prohgoyumenes
    #hmeromhnies. Mpainw sto docker tou scheduler me -it bash kai dinw thn entolh airflow dags backfill -s 2025-05-28 -e 2025-06-05 dag_with_catchup_v002
    # opote ama kanw refresh blepw oti etreje gia tis prohgoumenes hmeromhnies
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo DAG WITH CATHUP"
    )
    task1
# the firsti execution is on 29 5. it is today 5 june 2025 so i see the cathcups but why didnt it execute on 28/5/2025?

# To summarize:

#     start_date defines when your DAG starts tracking intervals, not when the first run appears.

#     With @daily, the first actual run appears on start_date + 1 day.

#     If you want the first run to show up as 2025-05-28, you'd have to set start_date=datetime(2025, 5, 27).

# CATHUP = FALSE
# ok. and then after what i did was to catchup=False. so i see 2 runs. one for 4/6/2025 yesterday and one for 5/6/25 today. since the run time 
# is 03:00 as i see and i run it in 15:54 why did not it run only for today 5/6/25 ?

# oooh so because it found a matcing condition for schedule = @ daily that is why it run it 2 times
# ChatGPT said: Exactly — you’ve got it
