from airflow.providers.postgres.hooks.postgres import PostgresHook

def return_engine(conn_id: str):
    return PostgresHook(postgres_conn_id=conn_id).get_sqlalchemy_engine()