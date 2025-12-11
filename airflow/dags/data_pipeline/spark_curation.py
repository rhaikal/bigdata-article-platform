from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from helpers import HADOOP_SSH_PREFIX

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_curation',
    tags=['article-platform', 'curation'],
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as hive_curation_dag:
    run_hive_curation = BashOperator(
        task_id='run_hive_curation',
        bash_command=f"{HADOOP_SSH_PREFIX} '/opt/spark/bin/spark-submit /opt/spark/script/hive_curation.py'",
    )
    
    run_hbase_curation = BashOperator(
        task_id='run_hbase_curation',
        bash_command=f"{HADOOP_SSH_PREFIX} '/opt/spark/bin/spark-submit /opt/spark/script/hbase_curation.py'",
    )
    
    run_hive_curation >> run_hbase_curation