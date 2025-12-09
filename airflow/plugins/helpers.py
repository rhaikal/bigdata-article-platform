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

HDFS_DATA_DIR = os.getenv("HDFS_DATA_DIR", "/data")

HADOOP_SSH_PASSWORD = os.getenv("SSH_PASSWORD", "Hadoop@123")
HADOOP_SSH_PREFIX = (
    f"sshpass -p '{HADOOP_SSH_PASSWORD}' ssh "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null "
    "root@hadoop-client"
)
HADOOP_SCP_PREFIX = (
    f"sshpass -p '{HADOOP_SSH_PASSWORD}' scp "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null "
)

def get_db_connection():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
