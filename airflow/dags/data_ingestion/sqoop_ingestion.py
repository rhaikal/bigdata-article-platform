from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from helpers import HDFS_DATA_DIR, HADOOP_SSH_PREFIX
import subprocess
import json
import os

default_args = {
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

TABLES = [
    {
        'name': 'users',
        'incremental_column': 'join_date',
        'merge_key': 'user_id',
        'hdfs_path': f'{HDFS_DATA_DIR}/raw/sql/users',
        'columns': ['user_id', 'username', 'join_date', 'is_member']
    },
    {
        'name': 'articles',
        'incremental_column': 'publish_date', 
        'merge_key': 'article_id',
        'hdfs_path': f'{HDFS_DATA_DIR}/raw/sql/articles',
        'columns': ['article_id', 'author_id', 'title', 'publish_date', 'category_id', 'premium']
    },
    {
        'name': 'categories',
        'incremental_column': None,
        'merge_key': None,
        'hdfs_path': f'{HDFS_DATA_DIR}/raw/sql/categories',
        'columns': ['category_id', 'name']
    },
    {
        'name': 'subscriptions',
        'incremental_column': 'start_date',
        'merge_key': 'subscription_id', 
        'hdfs_path': f'{HDFS_DATA_DIR}/raw/sql/subscriptions',
        'columns': ['subscription_id', 'user_id', 'start_date', 'end_date', 'is_active']
    }
]

def get_postgres_config():
    return {
        'host': 'postgres-app',
        'port': '5432',
        'database': os.getenv('POSTGRES_APP_DB', 'article_db'),
        'username': os.getenv('POSTGRES_APP_USER', 'article_user'),
        'password': os.getenv('POSTGRES_APP_PASSWORD', 'Art1cl3_PlatfOrm')
    }

def get_last_import_date(table_name):
    try:
        last_import = Variable.get(f"last_import_{table_name}")
        value = json.loads(last_import).get('last_value')
        return value if value and value != 'None' else '2000-01-01 00:00:00'
    except:
        return '2000-01-01 00:00:00'

def get_end_timestamp():
    try:
        end_timestamp = Variable.get("ingestion_end_timestamp")
        return end_timestamp
    except:
        return None


def get_latest_value_from_hdfs(hdfs_path, incremental_column, column_names=None):
    try:
        if incremental_column == None:
            return None
        
        result = subprocess.run(
            [HADOOP_SSH_PREFIX, f'"hdfs dfs -cat {hdfs_path}/*"'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Error reading HDFS: {result.stderr}")
            return None
        
        lines = result.stdout.strip().split('\n')
        if not lines:
            return None
        
        if not column_names:
            print("Column names must be specified to locate the incremental column.")
            return None
        
        col_index = column_names.index(incremental_column)
        last_row = lines[-1]
        last_value = last_row.strip().split(',')[col_index]
        
        return last_value
    except Exception as e:
        print(f"Error extracting latest value from HDFS: {str(e)}")
        return None

def set_last_import(context, table_name, last_value):
    try:
        Variable.set(
            f"last_import_{table_name}",
            json.dumps({'last_value': last_value})
        )
    except Exception as e:
        print(f"Error setting variable last_import_{table_name}: {str(e)}")

def generate_sqoop_command(table_config):
    conn = get_postgres_config()
    end_timestamp = get_end_timestamp()
    
    temp_bindir = "/tmp/sqoop-bindir"
    temp_outdir = "/tmp/sqoop-gen"
    table_name = table_config['name']
    class_name = table_name
    jar_file_path = f"{temp_bindir}/{class_name}.jar"

    conn_args = f"--connect jdbc:postgresql://{conn['host']}:{conn['port']}/{conn['database']} --username {conn['username']} --password '{conn['password']}'"

    codegen_cmd = f"sqoop codegen --table {table_name} --class-name {class_name} --bindir {temp_bindir} --outdir {temp_outdir} {conn_args}"

    import_cmd = f"sqoop import -libjars {jar_file_path} {conn_args} --table {table_name} --class-name {class_name} -m 1 --target-dir {table_config['hdfs_path']}"

    if table_config.get('incremental_column'):
        last_value = get_last_import_date(table_config['name'])
        
        import_cmd += f" --incremental lastmodified --check-column {table_config['incremental_column']} --last-value \\\"{last_value}\\\""
        
        if end_timestamp:
            import_cmd += f" --where \\\"{table_config['incremental_column']} <= '{end_timestamp}'\\\""
        
        import_cmd += f" --merge-key {table_config['merge_key']}"
    else:
        import_cmd += " --delete-target-dir"
    
    chained_cmd = f"{codegen_cmd} && {import_cmd}"

    return chained_cmd.strip()

with DAG(
    'sqoop_ingestion',
    tags=['article-platform', 'ingestion'],
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    schedule=None,
) as dag:    
    setup_hdfs = BashOperator(
        task_id='setup_hdfs_structure',
        bash_command=f"""
        {HADOOP_SSH_PREFIX} \
        "hdfs dfs -mkdir -p {HDFS_DATA_DIR}/raw/sql/users && \
        hdfs dfs -mkdir -p {HDFS_DATA_DIR}/raw/sql/articles && \
        hdfs dfs -mkdir -p {HDFS_DATA_DIR}/raw/sql/categories && \
        hdfs dfs -mkdir -p {HDFS_DATA_DIR}/raw/sql/subscriptions"
        """
    )

    sqoop_tasks = []

    for table in TABLES:
        task_id = f"import_{table['name']}"
        
        command = generate_sqoop_command(table) 

        import_task = BashOperator(
            task_id=task_id,
            bash_command=f"{HADOOP_SSH_PREFIX} \"{command}\"",
            execution_timeout=timedelta(minutes=30),
            on_success_callback=lambda context: set_last_import(
                context,
                table['name'],
                get_latest_value_from_hdfs(
                    table['hdfs_path'],
                    table['incremental_column'],
                    table.get('columns')
                )
            )
        )
        
        sqoop_tasks.append(import_task)
        
    verify_import = BashOperator(
        task_id='verify_imports',
        bash_command=f"""
        {HADOOP_SSH_PREFIX} \
        "for table in users articles categories subscriptions; do \
            hdfs dfs -test -e {HDFS_DATA_DIR}/raw/sql/\\$table/_SUCCESS || exit 1; \
        done"
        """,
        trigger_rule='all_success'
    )
    
    setup_hdfs >> sqoop_tasks
    sqoop_tasks >> verify_import
