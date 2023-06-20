from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

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
        with TaskGroup('Table_Tasks', dag=dag) as table_group:
            create_orders_table_task = PostgresOperator(
                task_id="create_orders_table_task",
                postgres_conn_id="main_db",
                sql="sql/create_orders.sql",
            )

            create_reviews_table_task = PostgresOperator(
                task_id="create_reviews_table_task",
                postgres_conn_id="main_db",
                sql="sql/create_reviews.sql",
            )

            create_shipments_table_task = PostgresOperator(
                task_id="create_shipments_table_task",
                postgres_conn_id="main_db",
                sql="sql/create_shipments.sql",
            )

            create_agg_public_hol_task = PostgresOperator(
                task_id="create_agg_public_hol_task",
                postgres_conn_id="main_db",
                sql="sql/create_agg_pub_hol.sql",
            )

            create_agg_shipments_task = PostgresOperator(
                task_id="create_agg_shipments_task",
                postgres_conn_id="main_db",
                sql="sql/create_agg_shipments.sql",
            )

            create_best_product_task = PostgresOperator(
                task_id="create_best_product_task",
                postgres_conn_id="main_db",
                sql="sql/create_best_product.sql",
            )

        table_group


init_db_setup = DAG(
    dag_id="init_db_setup_v1",
    schedule_interval=None,
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['data2jobs'],
)

db_setup_dag(init_db_setup)