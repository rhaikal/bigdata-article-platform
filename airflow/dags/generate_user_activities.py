from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers import get_db_connection, LOG_PATHS, BASIC_SCROLL_LIMIT
import random
import json

def generate_user_activities():
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get active users
        cur.execute("SELECT user_id, is_member FROM users ORDER BY RANDOM() LIMIT 50")
        users = cur.fetchall()
        
        if not users:
            print("No users available")
            return
        
        # Get recent articles
        cur.execute("""
            SELECT a.article_id, a.premium, c.name as category 
            FROM articles a
            JOIN categories c ON a.category_id = c.category_id
            ORDER BY RANDOM() LIMIT 100
        """)
        articles = cur.fetchall()
        
        if not articles:
            print("No articles available")
            return
        
        activities = []
        for user_id, is_member in users:
            # Each user reads 1-3 articles
            num_articles = random.randint(1, min(3, len(articles)))
            for article_id, premium, category in random.sample(articles, num_articles):
                if premium and not is_member:
                    scroll_depth = random.randint(10, BASIC_SCROLL_LIMIT)
                    effective_read = scroll_depth
                    paywall_triggered = scroll_depth >= BASIC_SCROLL_LIMIT * 0.8
                else:
                    scroll_depth = random.randint(30, 100)
                    effective_read = scroll_depth
                    paywall_triggered = False
                
                activities.append({
                    "timestamp": datetime.now().isoformat(),
                    "user_id": user_id,
                    "article_id": article_id,
                    "content": {
                        "category": category,
                        "premium": premium,
                        "accessible_pct": BASIC_SCROLL_LIMIT if (premium and not is_member) else 100
                    },
                    "engagement": {
                        "scroll_depth_pct": scroll_depth,
                        "effective_read_pct": effective_read,
                        "paywall_triggered": paywall_triggered
                    }
                })
        
        # Write activities to log
        if activities:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            with open(f"{LOG_PATHS['activity']}/activity_{timestamp}.json", "w") as f:
                for activity in activities:
                    f.write(json.dumps(activity) + "\n")
            
    finally:
        cur.close()
        conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'generate_user_activities',
    tags=['article-platform', 'generation'],
    default_args=default_args,
    schedule='* * * * *',  # Every minute
    catchup=False,
    max_active_runs=1
) as dag:
    
    gen_activity = PythonOperator(
        task_id='generate_user_activity',
        python_callable=generate_user_activities,
        execution_timeout=timedelta(minutes=2),
    )