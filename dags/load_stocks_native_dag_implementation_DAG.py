from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
import urllib.parse
import requests
import json
import pandas as pd
import csv
from alpha_vantage.timeseries import TimeSeries
import os
import sys
sys.path.append('/opt/airflow/etl')
from stocks_queries import truncate_mrr_stocks, load_dim_stocks, create_view_dim_stocks, select_from_vw_statement
  

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['personal email'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(dag_id='load_stocks_test_dag_implementation',
          default_args=default_args,
          schedule_interval="0 05 * * *",
          start_date=datetime(2020, 1, 1),
          catchup=False
          )

# Instantiate StocksETL class parameters
script_name = str(os.path.basename(__file__))
api_key = 'personal api key'
web_hook_url = 'personal webhook'
slack_msg_body_success = "{} process finished successfully at: {}".format(script_name, datetime.now().strftime('%d/%m/%Y : %H:%M:%S'))
slack_msg_success = {'text': slack_msg_body_success}


csv_stocks_file_path = '/opt/airflow/files/data/stocks.csv'
dwh_staging_connection_id = BaseHook.get_connection('dwh_staging_config')
database = 'public'
postgres_mrr_table_name = 'mrr_stocks'
postgres_dim_table_name = 'dim_stocks'
postgres_view = 'vw_dim_stocks'

# Create a PostgreSQL engine
dwh_staging_config = {
    'user': dwh_staging_connection_id.login,
    'password': dwh_staging_connection_id.password,
    'host': dwh_staging_connection_id.host,
    'port': dwh_staging_connection_id.port,
    'database': dwh_staging_connection_id.schema,
}
pg_engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s/dwh' % (
    dwh_staging_config.get('user'),
    urllib.parse.quote_plus(dwh_staging_config.get('password')),
    dwh_staging_config.get('host'),
    dwh_staging_config.get('port')
))

# Read CSV
def read_csv_task_func():
    company_symbol_list = []
    with open(csv_stocks_file_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the header
        for row in csvreader:
            company_symbol = row[0]
            company_symbol_list.append(company_symbol)
    return company_symbol_list

read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv_task_func,
    dag=dag,
)

# Get company info and load mrr table
def get_company_info_and_stock_data_and_load_to_mrr(**kwargs):
    company_symbol_list = kwargs['task_instance'].xcom_pull(task_ids='read_csv')
    
    for company_symbol in company_symbol_list:
        #for each company symbol - if the request fails, an exception is raised
        try:
            #Make the API request
            endpoint = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={company_symbol}&apikey={api_key}'
            response = requests.get(endpoint)
            # Check if the request was successful
            if response.status_code == 200:
                # Get company information
                company_info = response.json()
                ts = TimeSeries(key=api_key, output_format='pandas')
                data, _ = ts.get_daily(symbol=company_symbol, outputsize='compact')
                data.index = pd.to_datetime(data.index)
                end_date = data.index.max()
                start_date = end_date - timedelta(days=7)
                data['symbol'] = company_symbol
                data_last_week = data[(data.index >= start_date) & (data.index <= end_date)]
                if company_info:
                    company_name = company_info.get('Name', 'N/A')
                    # Create DataFrames for stock data and company information
                    stock_data_df = data_last_week[['1. open', '4. close']].reset_index()
                    stock_data_df['symbol'] = company_symbol
                    stock_data_df.rename(columns={'1. open': 'open', '4. close': 'close'}, inplace=True)
                    company_info_df = pd.DataFrame({'symbol': [company_symbol], 'company_name': [company_name]})
                    # Merge the DataFrames on the 'symbol' column
                    merged_df = pd.merge(stock_data_df, company_info_df, on='symbol')
                    final_df = merged_df[['date', 'company_name', 'symbol', 'open', 'close']]
                    
                    print('starting loading data to public.mrr_stocks for symbol {} at: {}'.format(company_symbol, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
                    final_df.to_sql(
                        name=postgres_mrr_table_name,
                        con=pg_engine,
                        schema=database,
                        if_exists='append',
                        index=False,
                        index_label=None,
                        method=None
                    )
                    print('starting loading data to public.mrr_stocks for symbol {} at: {}\n'.format(company_symbol, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))

            else:
                return None
        except Exception as e:
            print(f"Error processing symbol {company_symbol}: {e}")
            print(f"Failed to fetch company information for symbol {company_symbol}. Status code: {response.status_code}")

get_company_info_and_stock_data_and_load_to_mrr_task = PythonOperator(
    task_id='get_company_info_and_stock_data_and_load_to_mrr',
    python_callable=get_company_info_and_stock_data_and_load_to_mrr,
    provide_context=True,  # Allows passing context (e.g., XCom values) to the function
    dag=dag,
)

# Truncate mrr table
def truncate_mrr_stocks_task_func():
    print('starting truncating data from public.mrr_stocks at: {}'.format(datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
    with pg_engine.begin() as conn:
        conn.execute(text(truncate_mrr_stocks))
    print('finished truncating data from public.mrr_stocks at: {}\n'.format(datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))

truncate_mrr_stocks_task = PythonOperator(
    task_id='truncate_mrr_stocks',
    python_callable=truncate_mrr_stocks_task_func,
    dag=dag,
)

# Load dim stocks
def load_dim_stocks_task_func():
    print('starting loading data to public.dim_stocks at: {}'.format(datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
    with pg_engine.begin() as conn:
         conn.execute(text(load_dim_stocks))
         print('finished loading data to public.dim_stocks at: {}\n'.format(datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))

load_dim_stocks_task = PythonOperator(
    task_id='load_dim_stocks',
    python_callable=load_dim_stocks_task_func,
    provide_context=True,
    dag=dag,
)

# Create view function
def create_view_dim_stocks_task_func():
    print('creating view public.vw_dim_stocks at: {}'.format(datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
    with pg_engine.begin() as conn:
        conn.execute(text(create_view_dim_stocks))
    print('view was created\n')


def fetch_data_from_view():
    print('starting fetching data from view {} at: {}'.format(postgres_view, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
    with pg_engine.connect() as conn:
        results = conn.execute(text(select_from_vw_statement))
    print('finished fetching data from view {} at: {}\n'.format(postgres_view, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
    
    #calculate trend: fetch changes with at least 10% change in stock rate compared to last day
    if results.rowcount > 0:
        df = pd.DataFrame(results.fetchall(), columns=results.keys())
        message = df.to_markdown(index=False)
        slack_msg = {'text': message}
        requests.post(web_hook_url, data=json.dumps(slack_msg))
        print('slack message was sent for an increase with more than 10% in at least one of the stocks')
    else:
        print('no siginificant change was done on the last date')

fetch_data_from_view_and_send_alert_task = PythonOperator(
    task_id='fetch_data_from_view',
    python_callable=load_dim_stocks_task_func,
    provide_context=True,
    dag=dag,
)


def send_slack_message_on_success():
    requests.post(web_hook_url, data=json.dumps(slack_msg_success))

send_slack_message_on_success_task = PythonOperator(
    task_id='send_slack_message_on_success',
    python_callable=send_slack_message_on_success,
    provide_context=True,
    dag=dag,
)
   


read_csv_task >> truncate_mrr_stocks_task >> get_company_info_and_stock_data_and_load_to_mrr_task >> load_dim_stocks_task >> fetch_data_from_view_and_send_alert_task>>send_slack_message_on_success_task