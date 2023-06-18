
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

from app.dim_dates import dimension_dates

START_DATE = "2021-01-01"
END_DATE = "2022-09-06"

default_args = {
    "owner": "bebeto",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def db_setup_dag(dag):
    with dag:
        create_bebeto_staging_schema_task = PostgresOperator(
            task_id="create_bebeto_staging_schema_task",
            postgres_conn_id="postgres_default",
            sql="sql/bebeto_staging.sql",
        )

        create_bebeto_analytics_schema_task = PostgresOperator(
            task_id="create_bebeto_analytics_schema_task",
            postgres_conn_id="postgres_default",
            sql="sql/bebeto_analytics.sql",
        )

        create_if_common_schema_task = PostgresOperator(
            task_id="create_if_common_schema_task",
            postgres_conn_id="postgres_default",
            sql="sql/if_common.sql",
        )

        create_dim_dates_task = PostgresOperator(
            task_id="create_dim_dates_task",
            postgres_conn_id="postgres_default",
            sql="sql/dim_dates.sql",
        )

        create_orders_table_task = PostgresOperator(
            task_id="create_orders_table_task",
            postgres_conn_id="postgres_default",
            sql="sql/orders.sql",
        )

        create_reviews_table_task = PostgresOperator(
            task_id="create_reviews_table_task",
            postgres_conn_id="postgres_default",
            sql="sql/reviews.sql",
        )

        create_shipments_table_task = PostgresOperator(
            task_id="create_shipments_table_task",
            postgres_conn_id="postgres_default",
            sql="sql/shipments.sql",
        )

        create_agg_public_hol_task = PostgresOperator(
            task_id="create_agg_public_hol_task",
            postgres_conn_id="postgres_default",
            sql="sql/agg_pub_hol.sql",
        )

        create_agg_shipments_task = PostgresOperator(
            task_id="create_agg_shipments_task",
            postgres_conn_id="postgres_default",
            sql="sql/agg_shipments.sql",
        )

        create_best_product_task = PostgresOperator(
            task_id="create_best_product_task",
            postgres_conn_id="postgres_default",
            sql="sql/best_product.sql",
        )

        ingest_dim_dates = PythonOperator(
                        task_id="best_performing_product_task",
                        python_callable=dimension_dates,
                        op_kwargs={
                            "start_date": START_DATE,
                            "end_date": END_DATE
                        }
        )

        [create_if_common_schema_task,
         create_bebeto_staging_schema_task, 
         create_bebeto_analytics_schema_task ] >> [create_dim_dates_task,
                                                   create_orders_table_task,
                                                   create_reviews_table_task,
                                                   create_shipments_table_task] >> [create_agg_public_hol_task, 
                                                                                    create_agg_shipments_task, 
                                                                                    create_best_product_task] >> ingest_dim_dates


init_db_setup = DAG(
    dag_id="init_db_setup_v1",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

db_setup_dag(init_db_setup)