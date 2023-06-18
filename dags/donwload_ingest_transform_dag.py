import os

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from app.from_s3 import download_files
from app.to_wh_staging import orders_to_staging, reviews_to_staging, shipments_to_staging


from app.agg_transformations import agg_public_holiday, agg_shipments, best_performing_product

BUCKET = "d2b-internal-assessment-bucket"
FILES = ["orders.csv", "reviews.csv", "shipment_deliveries.csv"]
START_DATE = "2021-01-01"
END_DATE = "2022-09-06"


default_args = {
    "owner": "bebeto",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def donwload_parquetize_upload_dag(
    dag,
    local_csv_path_template
):
    with dag:
        s3_download_dataset_task = PythonOperator(
            task_id="s3_download_dataset_task",
            python_callable=download_files,
            op_kwargs={
                "bucket_name": BUCKET,
                "files": FILES
            },
        )

        orders_to_staging_task = PythonOperator(
            task_id="orders_to_staging_task",
            python_callable=orders_to_staging,
            op_kwargs={
                "dir": "data",
            },
        )

        reviews_to_staging_task = PythonOperator(
            task_id="reviews_to_staging_task",
            python_callable=reviews_to_staging,
            op_kwargs={
                "dir": "data",
            },
        )


        shipments_to_staging_task = PythonOperator(
            task_id="shipments_to_staging_task",
            python_callable=shipments_to_staging,
            op_kwargs={
                "dir": "data",
            },
        )

        agg_public_holiday_task = PythonOperator(
            task_id="agg_public_holiday_task",
            python_callable=agg_public_holiday,

        )

        agg_shipments_task = PythonOperator(
            task_id="agg_shipments_task",
            python_callable=agg_shipments,

        )

        best_performing_product_task = PythonOperator(
            task_id="best_performing_product_task",
            python_callable=best_performing_product,

        )

        rm_csv_task = BashOperator(
            task_id="rm_csv_task",
            bash_command=f"rm {local_csv_path_template}"
        )

        s3_download_dataset_task >> [orders_to_staging_task, 
                                     reviews_to_staging_task, 
                                     shipments_to_staging_task] >> [agg_public_holiday_task,
                                                                    agg_shipments_task,
                                                                    best_performing_product_task] >> rm_csv_task
