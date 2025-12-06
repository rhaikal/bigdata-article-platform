import os
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_article_platform")

LOG_DIR = "/opt/airflow/data/logs"
LOG_PATHS = {
    'users': f"{LOG_DIR}/users",
    'articles': f"{LOG_DIR}/articles",
    'activity': f"{LOG_DIR}/activity",
    'subscriptions': f"{LOG_DIR}/subscriptions",
}

BASIC_SCROLL_LIMIT = int(os.getenv("BASIC_SCROLL_LIMIT", 40))

def get_db_connection():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
