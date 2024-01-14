FROM apache/airflow:2.8.0

ARG DWH_ENV
ENV DWH_ENV $DWH_ENV

# Upgrade pip
RUN python -m pip install --upgrade pip

# Install necessary packages and dependencies one by one
RUN python -m pip install --user --no-cache-dir db-dtypes
RUN python -m pip install --user --no-cache-dir alpha-vantage
RUN python -m pip install --user --no-cache-dir psycopg2-binary
RUN python -m pip install --user --no-cache-dir mysql-connector
RUN python -m pip install --user --no-cache-dir mysql-connector-python
RUN python -m pip install --user --no-cache-dir simple_salesforce
RUN python -m pip install --user --no-cache-dir pandas
RUN python -m pip install --user --no-cache-dir pandabase
RUN python -m pip install --user --no-cache-dir SQLAlchemy
RUN python -m pip install --user --no-cache-dir google-analytics-data
RUN python -m pip install --user --no-cache-dir pybigquery
RUN python -m pip install --user --no-cache-dir pymysql
RUN python -m pip install --user --no-cache-dir sqlalchemy
RUN python -m pip install --user --no-cache-dir loguru
RUN python -m pip install --user --no-cache-dir pyathena
RUN python -m pip install --user --no-cache-dir pandas-gbq
RUN python -m pip install --user --no-cache-dir google-oauth
RUN python -m pip install --user --no-cache-dir --upgrade google-auth
RUN python -m pip install --user --no-cache-dir google-cloud-bigquery 
RUN python -m pip install --user --no-cache-dir google-cloud-storage==1.29.0
RUN python -m pip install --user --no-cache-dir python-dateutil
RUN python -m pip install --user --no-cache-dir boto3
RUN python -m pip install --user --no-cache-dir times
RUN python -m pip install --user --no-cache-dir google-api-python-client
RUN python -m pip install --user --no-cache-dir google-auth-oauthlib
RUN python -m pip install --user --no-cache-dir oauth2client
RUN python -m pip install --user --no-cache-dir 'six==1.13' --force-reinstall

