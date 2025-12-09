from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers import get_db_connection, LOG_PATHS
import json
import os

def get_user_activity(user_id):
    activities = []
    try:
        for filename in os.listdir(LOG_PATHS['activity']):
            if filename.startswith('activity_'):
                with open(os.path.join(LOG_PATHS['activity'], filename)) as f:
                    content = f.read()
                    if content.strip().startswith('['):
                        events = json.loads(content)
                        for event in events:
                            if event.get('user_id') == user_id:
                                activities.append(event)
                    else:
                        for line in content.splitlines():
                            if line.strip():
                                event = json.loads(line)
                                if event.get('user_id') == user_id:
                                    activities.append(event)
    except FileNotFoundError:
        pass
    return activities

def generate_subscriptions():
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT u.user_id 
            FROM users u
            LEFT JOIN subscriptions s ON u.user_id = s.user_id AND s.is_active = true
            WHERE u.is_member = false
            AND s.subscription_id IS NULL
        """)
        candidates = [row[0] for row in cur.fetchall()]
        
        for user_id in candidates:
            current_time = datetime.now()
            activities = get_user_activity(user_id)
            premium_reads = [
                a for a in activities 
                if a.get('content', {}).get('premium') is True
                and datetime.fromisoformat(a['timestamp']) > current_time - timedelta(hours=24)
            ]
            
            non_premium_reads = [
                a for a in activities
                if a.get('content', {}).get('premium') is False
                and datetime.fromisoformat(a['timestamp']) > current_time - timedelta(hours=24)
            ]
            
            if len(premium_reads) >= 3 or len(non_premium_reads) >= 8:
                existing_subs = [
                    f for f in os.listdir(LOG_PATHS['subscriptions'])
                    if f.startswith(f"{user_id}_")
                ]
                    
                if not existing_subs:
                    start_date = current_time
                    end_date = start_date + timedelta(days=30)
                    
                    try:
                        cur.execute(
                            """INSERT INTO subscriptions 
                            (user_id, start_date, end_date, is_active) 
                            VALUES (%s, %s, %s, true)
                            RETURNING subscription_id""",
                            (user_id, start_date, end_date)
                        )
                        subscription_id = cur.fetchone()[0]
                        
                        cur.execute(
                            "UPDATE users SET is_member = true WHERE user_id = %s",
                            (user_id,)
                        )
                        
                        log_entry = {
                            "subscription_id": subscription_id,
                            "user_id": user_id,
                            "start_date": start_date.isoformat(),
                            "trigger_articles": [
                                {
                                    "article_id": a['article_id'],
                                    "read_time": a['timestamp'],
                                    "scroll_depth": a['engagement']['scroll_depth_pct']
                                } for a in (premium_reads[:3] if len(premium_reads) >= 3 else non_premium_reads[:3])
                            ]
                        }
                        
                        with open(f"{LOG_PATHS['subscriptions']}/{user_id}_{start_date.strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
                            json.dump(log_entry, f)
                        
                        conn.commit()
                        
                    except Exception as e:
                        conn.rollback()
                        raise
                        
    finally:
        cur.close()
        conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'generate_subscriptions',
    tags=['article-platform', 'generation'],
    default_args=default_args,
    schedule='*/20 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    
    gen_subs = PythonOperator(
        task_id='generate_subscriptions',
        python_callable=generate_subscriptions,
        execution_timeout=timedelta(minutes=10),
    )