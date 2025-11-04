from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = { 
    'owner': 'kat2',
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}
with DAG(
    dag_id ='dog_postgres_v02',
    default_args=default_args,
    description='this is our first dag',
    start_date =datetime(2025,6,1),
    schedule_interval='35,40 14,13  * * *'
    #catchup=True # to cathup shmainei na ekteleitai oses fores eprepe na ektelestei me bash to start date p tou xw orisei mexri shmera analoga to 
    #schedule interval
) as dag:
    task1 = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'postgres_localhost',
        sql = """ create table if not exists dag_runs (
        dt date,
        dag_id character varying,
        primary key (dt,dag_id)
        )
        """)
        
    task2 = PostgresOperator(
        task_id = 'insert_into_table',
        postgres_conn_id = 'postgres_localhost',
        sql = """ insert into dag_runs (dt,dag_id) values ('{{ ds }}','{{ run_id }}')
        -- ON CONFLICT (dt, dag_id) DO UPDATE
        -- SET dt = EXCLUDED.dt; 
        """)
    task3 = PostgresOperator(
        task_id = 'delet_table',
        postgres_conn_id = 'postgres_localhost',
        sql = """ delete from dag_runs where dt = '{{ ds }}' and  dag_id = '{{ run_id }}' """

    )



# PROSOXH  ON CONFLICT (dt, dag_id) DO UPDATE kai SET dt = EXCLUDED.dt;  ta ebala giati trexw polles fores to dag. opote h hmeromhnia jana
# mpainei i idia kai epeidh einai part tou primary key, den mporw na xw duplicate hmeromhnia. opote t lew oti otan breis thn idia hmeromhnia
# kane update me thn neoterh timh tou allou merous tou primary key

#  -----> autos omws gia auton ton logo s leei oti genika sto airflow einai kalo na kaneis delete prin kaneis insert data logw tou oti ektelountai isws
#  jana kai jana otan kaneis clear ta tasks tou airflos. g auto ftiaxnei to task 3 p kanei delete       


        # ds einai h date p egine execution to dag kai run_id to id tou dag ekinh th mera 
        # kai ta duo dinontai  automata kai mporw na to kanw access bazontas thn ekastote metablith se {{}}
    # AYTA mporw na ta dw an psaksw airflow macros kai paw edw https://airflow.apache.org/docs/apache-airflow/2.8.4/templates-ref.html
    
    task1 >> task3 >> task2

#     aggrigated 
# insert into m1 
# select  a,b,sum(c) from d1 where date like "filtering on current month" !not totally three columns a,b,c 
# a,b are dimensions and c is the factor

# historic
# lets assume that i have a daily granularity table that daily keeps only current date info
#  and the next day the info transfers to the history table and overwritted in the daily table
