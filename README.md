# Data2Jobs Interview Solution

* Postgres Database Creation/Connection
* Downloading from S3, Loading Data and Transforming & Persisting in a Difference Schema within Postgres

## Code and Resources Used 
**Python Version:** 3.10

**Packages:** airflow, pandas, boto3


## Postgres Database Creation/Connection
Creating the tables within their respective schemas:

*	bebenyam5327_staging: orders, reviews, shipment_deliveries
*  bebenyam5327_analytics: agg_public_holiday, agg_shipments, best_performing_product

## Downloading from S3, Loading Data and Transforming & Persisting in a Difference Schema within Postgres
From the provided bucket:

*	downloaded three files: orders.csv, reviews.csv, shipment_deliveries.csv
*   ingested the contents into the staging schema into their respective tables
*   tansformed data to answer three data questions on public holidays, shipments and best perfoming product
*   wrote the transformed data into the analytics schema into theie respective tables


## How to Run the Script
1. Clone the repository:
```
git clone git@github.com:realonbebeto/ecom-data-eng.git
```
2. Change directory:
```
cd airflow
```
3. Create an .env file with the following environment parameters for accesing the airflow db for logging and volume setup:
* DB_USERNAME=real_value
* DB_PASSWORD=real_calue
* DB_HOST=real_value
* DB_PORT=real_value
* DB_NAME=real_value
* AIRFLOW_SQL_DB=real_value
* CELERY_SQL_DB=real_value
* HOST_VOLUME=real_value
* CONTAINER_VOLUME=real_value
```
touch .env
```
4. Setup airflow db via docker compose:
```
docker compose --env-file .env up airflow-init
```
5. Start the airflow services:
```
docker compose --env-file .env up -d
```
7. Log on to `localhost:8000` with airflow as both the username and password. 
   
8. Run init_db_setup_v1 on the airflow UI to create the tables
   
9.  Activate ecom_dag_v1 on the airflow UI to schedule the pipeline. The schedule interval option is provided in code to customize for needs.
