#import libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.untils.dates import days_ago

#difine DAG arguments
default_args = {
    'owner': 'duchop',
    'start_date': days_ago(0),
    'email': ['duchop0974@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#define DAG
dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='ETL Server Access Log Processing',
    schedule_interval=timedelta(days=1)
)

#define tasks

download = BashOperator(
    task_id='download',
    bash_command='curl "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt" -o web-server-access-log.txt',
    dag=dag
)

extract = BashOperator(
    task_id= 'extract',
    bash_command='cut -d "#" -f 1,4 < web-server-access-log.txt > /home/project/airflow/dags/extracted-data.txt',
    dag=dag
)

transform= BashOperator(
    task_id='transform',
    bash_command='tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/captitalized.txt',
    dag=dag
)

load=BashOperator(
    task_id='load',
    bash_command='zip log.zip /home/project/airflow/dags/captitalized.txt',
    dag=dag
)

#task pipeline

download >> extract >> transform >> load