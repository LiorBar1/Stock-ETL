from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from airflow.hooks.base import BaseHook
from stocks_queries import truncate_mrr_stocks, load_dim_stocks, create_view_dim_stocks, select_from_vw_statement
from sqlalchemy import create_engine, text
import csv
import urllib.parse
from datetime import timedelta, datetime
import requests
import json
import psycopg2
import os 

script_name = str(os.path.basename(__file__))

## slack configuration
web_hook_url = 'personal channel webhook for process success' #please view readme file
web_hook_url_failed_process = 'personal channel webhook for process failure' #please view readme file
# the slack message body as variable
slack_msg_body_success = "{} process finished successfully at: {}".format(script_name, datetime.now().strftime('%d/%m/%Y : %H:%M:%S'))
slack_msg_body_failure = "{} process failed at: {}" .format(script_name, datetime.now().strftime('%d/%m/%Y : %H:%M:%S'))
slack_msg_success = {'text': slack_msg_body_success}
slack_msg_failure = {'text': slack_msg_body_failure}


api_key = 'personal_api_key' #please view readme file
company_symbole_list = [] 
csv_stocks_file_path = '/opt/airflow/files/data/stocks.csv'
dwh_staging_connection_id = 'dwh_staging_config'
dwh_staging_connection_id = BaseHook.get_connection(dwh_staging_connection_id)
database='public'

#import postgres connections data from Airflow
dwh_staging_config = {
    'user': dwh_staging_connection_id.login,
    'password': dwh_staging_connection_id.password,
    'host': dwh_staging_connection_id.host,
    'port': dwh_staging_connection_id.port,
    'database': dwh_staging_connection_id.schema,
}

postgres_mrr_table_name='mrr_stocks'
postgres_dim_table_name='dim_stocks'
postgres_view='vw_dim_stocks'
pg_engine= create_engine('postgresql+psycopg2://%s:%s@%s:%s/dwh' % (dwh_staging_config.get('user'), urllib.parse.quote_plus(dwh_staging_config.get('password')), dwh_staging_config.get('host'), dwh_staging_config.get('port')))
sql_alchemy_conn = pg_engine.connect()


def get_company_info(api_key, company_symbol):
    # Define the API endpoint for company overview
    endpoint = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={company_symbol}&apikey={api_key}'

    # Make the API request
    response = requests.get(endpoint)
    
    # Check if the request was successful
    if response.status_code == 200:
        company_info = response.json()
        return company_info
    else:
        print(f"Failed to fetch company information. Status code: {response.status_code}")
        return None

def get_stocks_data(api_key, company_symbol):

    # Initialize the TimeSeries object with your API key
    ts = TimeSeries(key=api_key, output_format='pandas')

    # Get stock data for the last week
    data, _ = ts.get_daily(symbol=company_symbol, outputsize='compact')
    data.index = pd.to_datetime(data.index)

    end_date = data.index.max()
    start_date = end_date - timedelta(days=7)
    data['symbol']=company_symbol
    data_last_week = data[(data.index >= start_date) & (data.index <= end_date)]
    # Get company information
    company_info = get_company_info(api_key, company_symbol)

    if company_info:
        # Extract relevant information
        company_name = company_info.get('Name', 'N/A')

        # Create DataFrames for stock data and company information
        stock_data_df = data_last_week[['1. open', '4. close']].reset_index()
        stock_data_df['symbol']=company_symbol
        stock_data_df.rename(columns = {'1. open':'open', '4. close': 'close'}, inplace = True)
        company_info_df = pd.DataFrame({'symbol': [company_symbol], 'company_name': [company_name]})
        # Merge the DataFrames on the 'symbol' column
        merged_df = pd.merge(stock_data_df, company_info_df, on='symbol')
        final_df = merged_df[['date', 'company_name', 'symbol', 'open', 'close']]
        return final_df
    else:
        return None


def load_mrr_stock(stocks_df):
    stocks_df.to_sql(name=postgres_mrr_table_name, con=pg_engine, schema=database, if_exists='append',index=False, index_label=None, method=None)



#4. calculate trend: fetch changes with at least 10% change in stock rate compared to last day
print('starting fetching data from view {} at: {}'.format(postgres_view, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
trans = sql_alchemy_conn.begin()
results=sql_alchemy_conn.execute(text(select_from_vw_statement))
print('finished fetching data from view {} at: {}'.format(postgres_view, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
trans.commit()


try:

    print('starting truncating table {} at: {}'.format(postgres_mrr_table_name, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
    trans = sql_alchemy_conn.begin()
    print('finished truncating table {} at: {}'.format(postgres_mrr_table_name, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
    trans.commit()

    for company_symbol in company_symbole_list:

        try:
            print('starting fetching data for {} at: {}'.format(company_symbole_list, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
            df=get_stocks_data(api_key, company_symbol)
            print('finished fetching data for {} at: {}'.format(company_symbole_list, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))

            print('starting loading data to table {} at: {}'.format(postgres_mrr_table_name, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
            load_mrr_stock(df)
            print('finished truncating data to table {} at: {}'.format(postgres_mrr_table_name, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))

        except Exception as e:
            print(e, 'stock symbol: ', company_symbol)

    print('creating view {} at: {}'.format(postgres_view, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
    trans = sql_alchemy_conn.begin()
    sql_alchemy_conn.execute(text(create_view_dim_stocks))
    print('finished creating view {} at: {}'.format(postgres_view, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
    trans.commit()

    print('starting loadind table {} at: {}'.format(postgres_dim_table_name, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
    trans = sql_alchemy_conn.begin()
    results=sql_alchemy_conn.execute(text(load_dim_stocks))
    print('finished loadind table {} at: {}'.format(postgres_dim_table_name, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
    trans.commit()

    if results.rowcount > 0:
        df = pd.DataFrame(results.fetchall(), columns=results.keys())
        message = df.to_markdown(index=False)
        # Define your Slack message payload
        slack_msg_success = {
            'text': message}
        # send success slack message if there was an increase of at least x percent from previouse to the last day
        requests.post(web_hook_url, data=json.dumps(slack_msg_success))

except psycopg2.Error as error1:
    print("Failed execute record to database rollback: {}".format(error1))
    #requests.post(web_hook_url_failed_process, data=json.dumps(slack_msg_failure))



