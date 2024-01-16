
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

![image](https://github.com/Liorba1982/Stock-ETL/assets/88455916/c4bf484b-e455-41fa-9464-ee102e4eac30)

Stocks data loaded to the database

![image](https://github.com/Liorba1982/Stock-ETL/assets/88455916/3ee86618-8cf7-4e14-b8e5-4935fd231646)

![image](https://github.com/Liorba1982/Stock-ETL/assets/88455916/46d63fb1-1b2b-4d0e-94a3-c2c0aa168343)

![image](https://github.com/Liorba1982/Stock-ETL/assets/88455916/41c3dc81-b0d0-4b8e-ac6e-e80ee636820a)

More then 10% change

![image](https://github.com/Liorba1982/Stock-ETL/assets/88455916/0e6bb2af-2582-4303-a7cc-75dc0dd3d969)

Slack alert

![image](https://github.com/Liorba1982/Stock-ETL/assets/88455916/4625b33d-0154-48cf-aef4-0ea06e0faa5c)





