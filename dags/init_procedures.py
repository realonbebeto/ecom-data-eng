
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

from app.dim_dates import dimension_dates


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
        [create_if_common_schema_task,
         create_bebeto_staging_schema_task, 
         create_bebeto_analytics_schema_task ] >> [create_dim_dates_task,
                                                   create_orders_table_task,
                                                   create_reviews_table_task,
                                                   create_shipments_table_task] >> [create_agg_public_hol_task, 
                                                                                    create_agg_shipments_task, 
                                                                                    create_best_product_task]