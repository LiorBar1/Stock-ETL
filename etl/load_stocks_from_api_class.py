from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from airflow.hooks.base import BaseHook
from stocks_queries import truncate_mrr_stocks, load_dim_stocks, create_view_dim_stocks, select_from_vw_statement
from sqlalchemy import create_engine, text
import urllib.parse
import requests
import json
import psycopg2
import os
from datetime import timedelta, datetime
import csv

class StocksETL:

    def __init__(self, api_key, web_hook_url, web_hook_url_failed_process, csv_stocks_file_path, dwh_staging_connection_id):
        self.api_key = api_key
        self.web_hook_url = web_hook_url
        self.web_hook_url_failed_process = web_hook_url_failed_process
        self.csv_stocks_file_path = csv_stocks_file_path
        self.dwh_staging_connection_id = dwh_staging_connection_id
        self.database = 'public'
        self.postgres_mrr_table_name = 'mrr_stocks'
        self.postgres_dim_table_name = 'dim_stocks'
        self.postgres_view = 'vw_dim_stocks'
        self.pg_engine = self._create_pg_engine()
        self.company_symbol_list = self.read_csv()

    def _create_pg_engine(self):
        dwh_staging_config = {
            'user': self.dwh_staging_connection_id.login,
            'password': self.dwh_staging_connection_id.password,
            'host': self.dwh_staging_connection_id.host,
            'port': self.dwh_staging_connection_id.port,
            'database': self.dwh_staging_connection_id.schema,
        }
        return create_engine('postgresql+psycopg2://%s:%s@%s:%s/dwh' % (
            dwh_staging_config.get('user'),
            urllib.parse.quote_plus(dwh_staging_config.get('password')),
            dwh_staging_config.get('host'),
            dwh_staging_config.get('port')
        ))

    def read_csv(self):
        company_symbol_list = []
        with open(self.csv_stocks_file_path, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)  ## Skip the header
            for row in csvreader:
                company_symbol = row[0]
                company_symbol_list.append(company_symbol)
        return company_symbol_list

    def get_company_info(self, company_symbol):
        endpoint = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={company_symbol}&apikey={self.api_key}'
        response = requests.get(endpoint)
        if response.status_code == 200:
            company_info = response.json()
            return company_info
        else:
            print(f"Failed to fetch company information. Status code: {response.status_code}")
            return None

    def get_stocks_data(self, company_symbol):
        ts = TimeSeries(key=self.api_key, output_format='pandas')
        data, _ = ts.get_daily(symbol=company_symbol, outputsize='compact')
        data.index = pd.to_datetime(data.index)
        end_date = data.index.max()
        start_date = end_date - timedelta(days=7)
        data['symbol'] = company_symbol
        data_last_week = data[(data.index >= start_date) & (data.index <= end_date)]
        company_info = self.get_company_info(company_symbol)
        if company_info:
            company_name = company_info.get('Name', 'N/A')
            stock_data_df = data_last_week[['1. open', '4. close']].reset_index()
            stock_data_df['symbol'] = company_symbol
            stock_data_df.rename(columns={'1. open': 'open', '4. close': 'close'}, inplace=True)
            company_info_df = pd.DataFrame({'symbol': [company_symbol], 'company_name': [company_name]})
            merged_df = pd.merge(stock_data_df, company_info_df, on='symbol')
            final_df = merged_df[['date', 'company_name', 'symbol', 'open', 'close']]
            return final_df
        else:
            return None

    def load_mrr_stock(self, stocks_df):

        print('starting loading data to public.mrr_stocks at: {}'.format(datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
        stocks_df.to_sql(
            name=self.postgres_mrr_table_name,
            con=self.pg_engine,
            schema=self.database,
            if_exists='append',
            index=False,
            index_label=None,
            method=None
        )
        print('starting loading data to public.mrr_stocks at: {}\n'.format(datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))

    def truncate_mrr_stocks(self):
        print('starting truncating data from public.mrr_stocks at: {}'.format(datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
        with self.pg_engine.begin() as conn:
            conn.execute(text(truncate_mrr_stocks))
            print('finished truncating data from public.mrr_stocks at: {}\n'.format(datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))

    def create_view_dim_stocks(self):   
        print('creating view public.vw_dim_stocks at: {}'.format(datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
        with self.pg_engine.begin() as conn:
            conn.execute(text(create_view_dim_stocks))
            print('view was created\n')
        
    def load_dim_stocks(self):
        print('starting loading data to public.dim_stocks at: {}'.format(datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
        with self.pg_engine.begin() as conn:
            results = conn.execute(text(load_dim_stocks))
            print('finished loading data to public.dim_stocks at: {}\n'.format(datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
        return results
        
        
    def send_slack_alert(self, message):
        slack_msg = {'text': message}
        requests.post(self.web_hook_url, data=json.dumps(slack_msg))

    def fetch_data_from_view(self):
        select_from_vw_statement = 'select * from vw_dim_stocks where date=current_date-2 and last_day_change_percentage>=10;'
        
        print('starting fetching data from view {} at: {}'.format(self.postgres_view, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))
        with self.pg_engine.connect() as conn:
            results = conn.execute(text(select_from_vw_statement))
        print('finished fetching data from view {} at: {}\n'.format(self.postgres_view, datetime.now().strftime('%d/%m/%Y : %H:%M:%S')))

        if results.rowcount > 0:
            df = pd.DataFrame(results.fetchall(), columns=results.keys())
            message = df.to_markdown(index=False)
            self.send_slack_alert(message)
            print('slack message was sent for an increase with more than 10% in at least one of the stocks')

    def run_etl_process(self):
        try:
            
            self.truncate_mrr_stocks()
        
            for company_symbol in self.company_symbol_list:
                try:
                    stocks_df = self.get_stocks_data(company_symbol)          
                    self.load_mrr_stock(stocks_df)
                    
                except Exception as e:
                    print(e, 'stock symbol: ', company_symbol)

            self.load_dim_stocks()
            self.create_view_dim_stocks()
            self.fetch_data_from_view
           

        except psycopg2.Error as error1:
            print("Failed execute record to database rollback: {}".format(error1))
            # requests.post(self.web_hook_url_failed_process, data=json.dumps(slack_msg_failure))


# Instantiate StocksETL class
stocks_etl = StocksETL(api_key='your_api_key',
                      web_hook_url='your_web_hook_url',
                      web_hook_url_failed_process='your_web_hook_url_failed_process',
                      csv_stocks_file_path='/opt/airflow/files/data/stocks.csv',
                      dwh_staging_connection_id=BaseHook.get_connection('dwh_staging_config'))

# Run ETL process
stocks_etl.run_etl_process()

