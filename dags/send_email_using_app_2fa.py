
##a new 2FA is needed to be added to google account for an app (name it airflow)
##an http connection is needed to be added to Airflow with the app user and pass
##connection name is needed to be configured in the send_email_task = EmailOperator(


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG('email_notification_dag', 
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    start = DummyOperator(task_id='start')
    
    # Define your tasks here
    # For example, a PythonOperator representing your main task
    task1 = PythonOperator(
        task_id='task1',
        python_callable=my_task_function
    )
    
    end = DummyOperator(task_id='end')
    
    # Define EmailOperator to send email on failure
    send_email_task = EmailOperator(
        task_id='send_email_on_failure',
        to='recipient@example.com',  # Email address where you want to send the notification
        subject='Airflow DAG Failed - {{ dag.dag_id }}',
        html_content='The Airflow DAG {{ dag.dag_id }} has failed on {{ execution_date }}.',
        mime_charset='utf-8',
        smtp_mail_from='your_email@gmail.com',  # Sender email address
        smtp_conn_id='your_smtp_connection',  # Airflow Connection containing email credentials
        trigger_rule='one_failed'  # Trigger the task if any upstream task fails
    )

    # Define the task dependencies
    start >> task1 >> end
    [task1, end] >> send_email_task  # Trigger email task only if any upstream task fails
