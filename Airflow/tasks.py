from datetime import datetime, timedelta
from airflow import DAG
#from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Define DAG arguments
default_args = {
    'owner': 'aminscientist',
    'start_date': datetime(2023, 12, 5),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create DAG
dag = DAG(
    'fraud_detection_workflow',
    default_args=default_args,
    description='Orchestration of fraud detection process',
    schedule=timedelta(days=1),  # Daily execution
    is_paused_upon_creation=False,
)

# Task 1: Data Generation
task_generate_data = BashOperator(
    task_id='generate_data',
    bash_command='python C:\\Users\\Youcode\\Desktop\\fraud_detection_financial_transactions\\Fraud-Detection-in-Financial-Transactions\\data_generator\\data_generator.py',
    dag=dag,
)

# Task 2: API
task_api = BashOperator(
    task_id='api',
    bash_command='python C:\\Users\\Youcode\\Desktop\\fraud_detection_financial_transactions\\Fraud-Detection-in-Financial-Transactions\\api.py',
    dag=dag,
)

# Task 3: Load Data
task_load_data = BashOperator(
    task_id='load_data',
    bash_command='jupyter nbconvert --to notebook --execute C:\\Users\\Youcode\\Desktop\\fraud_detection_financial_transactions\\Fraud-Detection-in-Financial-Transactions\\load\\load.ipynb',
    dag=dag,
)

# Task 4: Fraud Detection with HiveQL
task_fraud_detection = BashOperator(
    task_id='fraud_detection',
    bash_command='jupyter nbconvert --to notebook --execute C:\\Users\\Youcode\\Desktop\\fraud_detection_financial_transactions\\Fraud-Detection-in-Financial-Transactions\\fraud_detection\\fraud_detection_HiveQL.ipynb',
    dag=dag,
)

# Set task dependencies
task_generate_data >> task_api
task_api >> task_load_data
task_load_data >> task_fraud_detection