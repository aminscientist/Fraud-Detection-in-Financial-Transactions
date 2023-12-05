import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from includes.modules.test import hello

args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(days=-1),  # make start date in the past
}

dag = DAG(
    dag_id='crm-elastic-dag',
    default_args=args,
    schedule='@daily'  # make this workflow happen every day
)

with dag:
    hello_world = PythonOperator(
        task_id='hello',
        python_callable=hello,
        # provide_context=True
    )