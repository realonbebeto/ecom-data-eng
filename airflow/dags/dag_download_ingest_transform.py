import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from from_s3 import download_files
from to_wh_staging import orders_to_staging, reviews_to_staging, shipments_to_staging


from agg_transformations import agg_public_holiday, agg_shipments, best_performing_product

BUCKET = "d2b-internal-assessment-bucket"
FILES = ["orders.csv", "reviews.csv", "shipment_deliveries.csv"]
DIR = os.getcwd()

default_args = {
    "owner": "bebeto",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def download_ingest_trans_dag(
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
        with TaskGroup('Staging_Tasks', dag=dag) as staging_group:
            orders_to_staging_task = PythonOperator(
                task_id="orders_to_staging_task",
                python_callable=orders_to_staging
            )

            reviews_to_staging_task = PythonOperator(
                task_id="reviews_to_staging_task",
                python_callable=reviews_to_staging
            )


            shipments_to_staging_task = PythonOperator(
                task_id="shipments_to_staging_task",
                python_callable=shipments_to_staging,
            )
        
        with TaskGroup('Analytics_Tasks', dag=dag) as analytic_group:
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

        # Set task dependencies for parallel execution
        s3_download_dataset_task >> staging_group >> analytic_group >> rm_csv_task


ecom_dag = DAG(
    dag_id="ecom_dag_v1",
    schedule_interval=None, # Can be configured to run depending on the s3 dumping time interval
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['data2jobs'],
)


download_ingest_trans_dag(ecom_dag, os.path.join(DIR, "tmp_data/*.csv"))