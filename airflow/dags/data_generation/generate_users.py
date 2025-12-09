from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers import get_db_connection, LOG_PATHS
from faker import Faker
import random
import json

fake = Faker()

def generate_users():
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT COUNT(*) FROM users")
        count = cur.fetchone()[0]

        log_entry = {
            "timestamp": None,
            "event_type": "user_created",
            "users": []
        }

        # Ensure at least 5 users exist
        if count < 5:
            for _ in range(5 - count):
                current_time = datetime.now()
                current_time_isoformat = current_time.isoformat()
                
                username = fake.unique.first_name()
                is_member = random.random() < 0.2
                cur.execute(
                    "INSERT INTO users (username, join_date, is_member) VALUES (%s, %s, %s) RETURNING user_id",
                    (username, current_time, is_member)
                )
                user_id = cur.fetchone()[0]
                log_entry["users"].append({
                    "user_id": user_id,
                    "username": username,
                    "join_date": current_time_isoformat,
                    "is_member": is_member
                })
        
        # 50% chance to add 1-2 additional users
        if random.random() < 0.5:
            for _ in range(random.randint(1, 2)):
                current_time = datetime.now()
                current_time_isoformat = current_time.isoformat()
                
                username = fake.unique.first_name()
                cur.execute(
                    "INSERT INTO users (username, join_date, is_member) VALUES (%s, %s, false) RETURNING user_id",
                    (username, current_time)
                )
                user_id = cur.fetchone()[0]
                log_entry["users"].append({
                    "user_id": user_id,
                    "username": username,
                    "join_date": current_time_isoformat,
                    "is_member": False
                })
        
        conn.commit()
        
        # Log all created users
        if len(log_entry["users"]) > 0:
            current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_entry["timestamp"] = current_timestamp
            with open(f"{LOG_PATHS['users']}/users_{current_timestamp}.json", "w") as f:
                json.dump(log_entry, f)
                    
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'generate_users',
    tags=['article-platform', 'generation'],
    default_args=default_args,
    schedule='*/5 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    gen_users = PythonOperator(
        task_id='generate_users',
        python_callable=generate_users,
        execution_timeout=timedelta(minutes=5)
    )