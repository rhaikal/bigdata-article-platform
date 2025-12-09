from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

def set_end_timestamp(**kwargs):
    logical_date = kwargs.get('logical_date')
    end_timestamp = logical_date.strftime('%Y-%m-%d %H:%M:%S')
    Variable.set("ingestion_end_timestamp", end_timestamp)
    print(f"Set ingestion_end_timestamp to DAG run date: {end_timestamp}")

with DAG(
    'combined_ingestion',
    tags=['article-platform', 'ingestion'],
    schedule='*/30 * * * *',
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id='start_ingestion')
    
    # Set end timestamp from DAG parameters
    set_timestamp = PythonOperator(
        task_id='set_end_timestamp',
        python_callable=set_end_timestamp,
    )
    
    # Trigger sqoop_ingestion DAG
    trigger_sqoop = TriggerDagRunOperator(
        task_id='trigger_sqoop_ingestion',
        trigger_dag_id='sqoop_ingestion',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        deferrable=False,
        execution_timeout=timedelta(hours=2),
    )
    
    # Trigger flume_ingestion DAG
    trigger_flume = TriggerDagRunOperator(
        task_id='trigger_flume_ingestion',
        trigger_dag_id='flume_ingestion',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        deferrable=False,
        execution_timeout=timedelta(hours=2),
    )
    
    end = EmptyOperator(task_id='end_ingestion')
    
    # Set up the workflow
    start >> set_timestamp >> trigger_sqoop >> trigger_flume >> end