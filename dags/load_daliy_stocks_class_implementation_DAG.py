from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['personal email'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}
dag = DAG(dag_id='load_daliy_stocks_class',
          default_args=default_args,
          schedule='0 05 * * *',
          start_date=datetime(2020, 1, 1),
          catchup=False
          )
t1_bash = """
            python /opt/airflow/etl/load_stocks_from_api_class.py
            """

t1 = BashOperator(
    task_id='load_stocks_from_api_class',
    bash_command=t1_bash,
    dag=dag)
