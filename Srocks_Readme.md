
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

Yaml + Dockerfile (see attached files)



    
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

![image](https://github.com/Liorba1982/Stocks-ETL/assets/88455916/3ee86618-8cf7-4e14-b8e5-4935fd231646)

![image](https://github.com/Liorba1982/Stocks-ETL/assets/88455916/46d63fb1-1b2b-4d0e-94a3-c2c0aa168343)



