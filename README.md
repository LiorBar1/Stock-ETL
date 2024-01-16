
# Stocks Tracking

Stocks Tracking
Daily ETL process for tracking stock changes. Automatically alerts for stocks with more than a 10% daily change.

* Python code consumes a CSV file containing stock symbols.
* For each stock, an API request is sent, and the last 7 days' stock rates are fetched.
* Data is loaded into a staging table in the PostgreSQL database and then into a dimension table.
* Stock change percentage is calculated, compared to the last day.
* Stocks data with more than a 10% change is sent to a designated Slack channel.

Error handling:

* For each stock, a try-except is managed. When stock data fails to be fetched, an error is sent to a designated Slack channel.
* For DB loading issues, an exception is raised.

Schedule:
The process is scheduled by the Airflow scheduler and runs daily at 5 AM (UTC). 
## Tech Stack

**Tools:** Docker, Docker-Compose, Apache Airflow


![Logo](https://g.foolcdn.com/editorial/images/761015/stock-market-data-with-uptrend-vector.jpg)


## Installation

Docker desktop install

https://docker.com/products/docker-desktop/

* create airflow home directory, containing the following folders:
dags, etl, logs, plugins, files/data, config
* Place the following configurations files within etl folder:
Yaml + Dockerfile (see attached files)
* build airflow image from Dockerfile:
  bash: docker build -t {image_name} 
* init the database:
* bash: docker-compose up airflow-init
* load docker-compose:
  bash: docker-compose up
* airflow UI - http://localhost:8080/


    
## API Reference

```http
  API Key
```

| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `api_key` | `string` | **Required**. Generate via https://www.alphavantage.co/support/#api-key |

#### Get item

```http
  Slack Webhook
```

| Parameter | Type     | Description                       |
| :-------- | :------- | :-------------------------------- |
| `webhook`      | `string` | **Required**. Create via Slack UI https://api.slack.com/messaging/webhooks |






## Screenshots

source data for API requests

![image](https://github.com/LiorBar1/Stock-ETL/assets/88455916/d495d95d-4e18-4f80-ba83-94398414e925)

Airflow webserver DAG

![image](https://github.com/LiorBar1/Stock-ETL/assets/88455916/4b1df1e4-d20c-4eb2-9322-8a6a51dfc1c7)

Stocks data loaded to the database

![image](https://github.com/LiorBar1/Stock-ETL/assets/88455916/0854f0ab-9a90-4db9-a216-b90190c64001)

More then 10% change Slack alert (test example only)

![image](https://github.com/LiorBar1/Stock-ETL/assets/88455916/7aeb2486-f8ec-409f-ae1f-844ff697fc1f)





