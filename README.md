# Big Data Article Platform

A big data platform for replication of article management and analytics service, implementing a complete data engineering pipeline with automated ETL workflows, distributed storage, and real-time analytics capabilities.

**Technology Stack:**
- **Apache Airflow** - Workflow orchestration and scheduling
- **Apache Hadoop** - Distributed storage (HDFS) and resource management (YARN)
- **Apache Hive** - Data warehousing with SQL interface
- **Apache HBase** - NoSQL database for real-time access
- **Apache Spark** - Distributed data processing
- **Apache Flume** - Log aggregation and streaming ingestion
- **Apache Sqoop** - Batch data transfer from RDBMS
- **Apache Zookeeper** - Distributed coordination
- **PostgreSQL** - Transactional database

## Project Structure

```
.
├── airflow/
│   ├── dags/
│   │   ├── data_generation/          # Synthetic data generators
│   │   │   ├── generate_users.py
│   │   │   ├── generate_articles.py
│   │   │   ├── generate_subscriptions.py
│   │   │   └── generate_user_activities.py
│   │   └── data_pipeline/            # ETL workflows
│   │       ├── sqoop_ingestion.py    # PostgreSQL → HDFS
│   │       ├── flume_ingestion.py    # Log streaming → HDFS
│   │       ├── spark_curation.py     # Data transformation
│   │       └── pipeline.py           # Orchestration DAG
│   ├── config/
│   ├── plugins/
│   └── requirements.txt
├── hadoop/
│   ├── cluster/                      # Hadoop cluster config
│   └── client/                       # Client tools (Spark, Flume, Sqoop)
├── hbase/
│   ├── conf/
│   └── init_db.sh                    # HBase schema initialization
├── hive/
│   ├── conf/
│   └── init_db.sh                    # Hive schema initialization
├── postgres/
│   └── app/
│       └── init_db.sql               # PostgreSQL schema
├── docker-compose.yaml
├── .env.example
└── README.md
```

## Installation

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- Minimum 8GB RAM allocated to Docker
- 20GB free disk space

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd bigdata-article-platform
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   ```
   Edit `.env` to customize credentials and settings.

3. **Start the platform**
   ```bash
   docker-compose up -d
   ```

4. **Verify services**
   ```bash
   docker-compose ps
   ```

5. **Access Airflow UI**
   - URL: http://localhost:8081
   - Username: `airflow`
   - Password: `"The value you assigned to _AIRFLOW_WWW_USER_PASSWORD in your .env file"`

### Service Endpoints

| Service | Port | URL | Description |
|---------|------|-----|-------------|
| Airflow Web UI | 8081 | http://localhost:8081 | Workflow management |
| Hadoop NameNode | 9870 | http://localhost:9870 | HDFS web interface |
| Hadoop ResourceManager | 8088 | http://localhost:8088 | YARN resource manager |

## Usage

### Running the Data Pipeline

1. **Generate synthetic data**
   - In the Airflow UI, enable the following DAGs to simulate continuous data generation:
      - `generate_users`
      - `generate_articles`
      - `generate_subscriptions`
      - `generate_user_activities`

2. **Execute ETL pipeline**
   - In the Airflow UI, first ensure that the following DAGs are enabled:
      - `sqoop_ingestion`
      - `flume_ingestion`
      - `spark_curation`
   - Enable the main `pipeline` DAG to automatically orchestrates the complete workflow
      - **Sqoop ingestion** — Transf*ers relational data from PostgreSQL (OLTP) to HDFS.
      - **Flume ingestion** — Streams log data into HDFS in near real-time.
      - **Spark curation** — Transforms and loads ingested data from HDFS into Hive (OLAP) and HBase (Real-Time Analytics)

### Querying Data

**Hive (Analytical Warehouse)**
```bash
docker exec -it <hive-server-container> beeline -u jdbc:hive2://localhost:10000
```

**HBase (Real-Time Store)**
```bash
docker exec -it <hbase-master-container> hbase shell
```

**HDFS (Data Lake Storage)**
```bash
docker exec -it <hadoop-namenode-container> hdfs dfs <dfs-command>
```

## Data Model

**PostgreSQL (OLTP)**
- `users` - User profiles
- `articles` - Article metadata
- `categories` - Article categories
- `subscriptions` - Premium subscriptions

**Hive (OLAP)**
- `dim_users`, `dim_articles`, `dim_categories`, `dim_subscriptions` - Dimension tables
- `fact_user_activity` - User reading behavior (partitioned by date)
- `fact_subscription_triggers` - Subscription conversion events

**HBase (Real-Time Analytics)**
- `active_users_by_date` - Daily active user metrics

## Stopping the Platform

**Graceful shutdown:**
```bash
docker-compose down
```

**Remove all data volumes:**
```bash
docker-compose down -v
```