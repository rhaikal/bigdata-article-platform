#!/bin/bash

BEELINE_CONN="jdbc:hive2://hive-server:10000"

# --- Database Creation ---
echo "1. Creating database article_platform..."
beeline -u ${BEELINE_CONN} -e "CREATE DATABASE IF NOT EXISTS article_platform;"

# --- Switch Context ---
BEELINE_CONN_DB="${BEELINE_CONN}/article_platform"

# --- Table Creation ---

# dim_categories
echo "2. Creating table dim_categories..."
beeline -u ${BEELINE_CONN_DB} -e "
    CREATE TABLE IF NOT EXISTS dim_categories (
        category_id INT,
        name STRING
    );
"

# dim_users
echo "3. Creating table dim_users..."
beeline -u ${BEELINE_CONN_DB} -e "
    CREATE TABLE IF NOT EXISTS dim_users (
        user_id INT,
        username STRING,
        join_date TIMESTAMP,
        is_member BOOLEAN
    );
"

# dim_articles
echo "4. Creating table dim_articles..."
beeline -u ${BEELINE_CONN_DB} -e "
    CREATE TABLE IF NOT EXISTS dim_articles (
        article_id INT,
        author_id INT,
        title STRING,
        publish_date TIMESTAMP,
        category_id INT,
        premium BOOLEAN
    );
"

# dim_subscriptions
echo "5. Creating table dim_subscriptions..."
beeline -u ${BEELINE_CONN_DB} -e "
    CREATE TABLE IF NOT EXISTS dim_subscriptions (
        subscription_id INT,
        user_id INT,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        is_active BOOLEAN
    );
"

# fact_user_activity
echo "6. Creating table fact_user_activity..."
beeline -u ${BEELINE_CONN_DB} -e "
    CREATE TABLE IF NOT EXISTS fact_user_activity (
        activity_ts TIMESTAMP,
        user_id INT,
        article_id INT,
        log_category STRING,
        log_premium BOOLEAN,
        accessible_pct INT,
        scroll_depth_pct INT,
        effective_read_pct INT,
        paywall_triggered BOOLEAN,
        sql_category_id INT
    ) PARTITIONED BY (log_date STRING);
"

# fact_subscription_triggers
echo "7. Creating table fact_subscription_triggers..."
beeline -u ${BEELINE_CONN_DB} -e "
    CREATE TABLE IF NOT EXISTS fact_subscription_triggers (
        subscription_id INT,
        user_id INT,
        start_date TIMESTAMP,
        trigger_article_id INT,
        read_ts TIMESTAMP,
        scroll_depth INT
    ) PARTITIONED BY (log_date STRING);
"

echo "Hive initialization complete."
exit 0