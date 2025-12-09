from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers import get_db_connection, LOG_PATHS
from faker import Faker
import random
import json

fake = Faker()

def generate_articles():
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Check if we have users
        cur.execute("SELECT COUNT(*) FROM users")
        if cur.fetchone()[0] == 0:
            print("No users available, skipping article generation")
            return
        
        log_entry = {
            "timestamp": None,
            "event_type": "articles_generated",
            "articles": []
        }
        
        # Generate 1-3 articles per run
        for _ in range(random.randint(1, 3)):
            cur.execute("SELECT EXISTS(SELECT 1 FROM users WHERE is_member = true)")
            has_premium_users = cur.fetchone()[0]
            premium = has_premium_users and random.random() > 0.7
            
            # Get author
            query = """
                SELECT user_id FROM users 
                WHERE is_member = true 
                ORDER BY RANDOM() 
                LIMIT 1
            """ if premium else "SELECT user_id FROM users ORDER BY RANDOM() LIMIT 1"
            cur.execute(query)
            author_id = cur.fetchone()[0]
            
            # Get category
            cur.execute("SELECT category_id FROM categories ORDER BY RANDOM() LIMIT 1")
            category_id = cur.fetchone()[0]
            
            # Insert article
            publish_date = datetime.now()
            title = fake.sentence(nb_words=8)
            cur.execute(
                """INSERT INTO articles 
                (author_id, title, publish_date, category_id, premium) 
                VALUES (%s, %s, %s, %s, %s)
                RETURNING article_id""",
                (author_id, title, publish_date, category_id, premium)
            )
            article_id = cur.fetchone()[0]
            
            log_entry["articles"].append({
                "timestamp": publish_date.isoformat(),
                "article_id": article_id,
                "title": title,
                "premium": premium,
                "author_id": author_id,
                "category_id": category_id
            })
        
        conn.commit()
        
        # Log created articles
        if len(log_entry["articles"]) > 0:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_entry["timestamp"] = timestamp
            with open(f"{LOG_PATHS['articles']}/articles_{timestamp}.json", "w") as f:
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
    'generate_articles',
    tags=['article-platform', 'generation'],
    default_args=default_args,
    schedule='*/10 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    
    gen_articles = PythonOperator(
        task_id='generate_articles',
        python_callable=generate_articles,
        execution_timeout=timedelta(minutes=3),
    )