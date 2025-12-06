from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers import get_db_connection, LOG_PATHS
import os

def initialize_database():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # categories table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
        category_id SERIAL PRIMARY KEY,
        name VARCHAR(50) UNIQUE NOT NULL
    )
    """)
    
    categories = [
        'Data Engineering', 'Machine Learning', 'Web Development', 'DevOps',
        'Cloud Computing', 'Cybersecurity', 'Blockchain', 'IoT', 'AI Ethics',
        'Quantum Computing', 'Bioinformatics', 'Fintech', 'Edtech', 'Healthtech',
        'AR/VR', 'Game Dev', 'Embedded Systems', 'Computer Vision', 'NLP',
        'Time Series', 'Graph Theory', 'Cryptography', 'Networking', 'OS Concepts',
        'Compiler Design', 'Database Theory', 'Distributed Systems', 'Microservices',
        'Serverless', 'Containers', 'CI/CD', 'Testing', 'Project Management',
        'Technical Writing', 'Career Growth', 'Startups', 'Freelancing',
        'Open Source', 'Research', 'Ethical Hacking', 'UX Design', 'UI Patterns',
        'Frontend', 'Backend', 'Mobile Dev', 'Robotics', 'Data Science',
        'Big Data', 'Analytics', 'Business Intelligence'
    ]
    
    sql = "INSERT INTO categories (name) VALUES (%s) ON CONFLICT DO NOTHING"
    cur.executemany(sql, [(cat,) for cat in categories])
    
    # users table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
        user_id SERIAL PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        join_date TIMESTAMP NOT NULL,
        is_member BOOLEAN NOT NULL DEFAULT false
        )
    """)
    
    # articles table with trigger for premium validation
    cur.execute("""
    CREATE TABLE IF NOT EXISTS articles (
    article_id SERIAL PRIMARY KEY,
    author_id INT NOT NULL REFERENCES users(user_id),
    title VARCHAR(200) NOT NULL,
    publish_date TIMESTAMP NOT NULL,
    category_id SMALLINT NOT NULL REFERENCES categories(category_id),
    premium BOOLEAN NOT NULL
    )
    """)
    
    # Trigger function to validate premium articles
    cur.execute("""
        CREATE OR REPLACE FUNCTION validate_premium_author()
        RETURNS TRIGGER AS $$
        BEGIN
        IF NEW.premium AND NOT EXISTS (
        SELECT 1 FROM users
        WHERE user_id = NEW.author_id AND is_member = true
        ) THEN
        RAISE EXCEPTION 'Premium articles require member authors';
        END IF;
        RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    
    # Create trigger on articles table
    cur.execute("""CREATE TABLE IF NOT EXISTS subscriptions (
        subscription_id SERIAL PRIMARY KEY,
        user_id INT NOT NULL REFERENCES users(user_id),
        start_date TIMESTAMP NOT NULL,
        end_date TIMESTAMP NOT NULL,
        is_active BOOLEAN NOT NULL DEFAULT true,
        CONSTRAINT valid_dates CHECK (start_date < end_date),
        CONSTRAINT unique_active_sub EXCLUDE (user_id WITH =)
        WHERE (is_active = true)
    )
    """)
    
    # Create log directories
    for path in LOG_PATHS.values():
        os.makedirs(path, exist_ok=True)
        
    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'init_database',
    tags=['article-platform', 'generation'],
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    schedule=None,
) as dag:
    init_task = PythonOperator(
        task_id='initialize_database',
        python_callable=initialize_database,
        execution_timeout=timedelta(minutes=5)
    )
