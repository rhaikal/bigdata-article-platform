from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, explode, broadcast
from pyspark.sql.types import *
import os

spark = SparkSession.builder \
    .appName("OLAP_Data_Integration") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE article_platform")

HDFS_DATA_DIR = f"hdfs://hadoop-namenode{os.environ.get('HDFS_DATA_DIR', '/data')}"

def process_dimension(table_name, hdfs_path, schema, key_column):
    print(f"Processing dimension table: {table_name}")
    full_table_name = f"article_platform.{table_name}"

    new_df = spark.read.schema(schema).csv(f"{hdfs_path}/*")

    if table_name == "dim_users":
        new_df = new_df.withColumn("join_date", col("join_date").cast("timestamp"))
    elif table_name == "dim_articles":
        new_df = new_df.withColumn("publish_date", col("publish_date").cast("timestamp"))
    elif table_name == "dim_subscriptions":
        new_df = new_df.withColumn("start_date", col("start_date").cast("timestamp")) \
                       .withColumn("end_date", col("end_date").cast("timestamp"))

    if spark.catalog.tableExists(full_table_name):
        max_id = spark.sql(f"SELECT COALESCE(MAX({key_column}), 0) FROM {full_table_name}").collect()[0][0]        
        if max_id is not None:
            new_df = new_df.filter(col(key_column) > max_id)
        new_df.write.mode("append").insertInto(full_table_name)
    else:
        new_df.write.mode("overwrite").saveAsTable(full_table_name)

    print(f"Done: {table_name}")

user_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("username", StringType()),
    StructField("join_date", StringType()),
    StructField("is_member", BooleanType())
])

article_schema = StructType([
    StructField("article_id", IntegerType()),
    StructField("author_id", IntegerType()),
    StructField("title", StringType()),
    StructField("publish_date", StringType()),
    StructField("category_id", IntegerType()),
    StructField("premium", BooleanType())
])

subscription_schema = StructType([
    StructField("subscription_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("start_date", StringType()),
    StructField("end_date", StringType()),
    StructField("is_active", BooleanType())
])

process_dimension("dim_users", f"{HDFS_DATA_DIR}/raw/sql/users", user_schema, "user_id")
process_dimension("dim_articles", f"{HDFS_DATA_DIR}/raw/sql/articles", article_schema, "article_id")
process_dimension("dim_subscriptions", f"{HDFS_DATA_DIR}/raw/sql/subscriptions", subscription_schema, "subscription_id")

print("Loading dim_categories...")
categories_df = spark.read.csv(f"{HDFS_DATA_DIR}/raw/sql/categories/*", schema=StructType([
    StructField("category_id", IntegerType()),
    StructField("name", StringType())
]))
categories_df.write.mode("overwrite").saveAsTable("article_platform.dim_categories")
print("Done: dim_categories")

def process_fact_partition(table_name, base_path, process_func):
    print(f"Processing fact table: {table_name}")
    full_table_name = f"article_platform.{table_name}"

    existing_partitions = []
    if spark.catalog.tableExists(full_table_name):
        existing_partitions = [row.log_date for row in spark.sql(f"SELECT DISTINCT log_date FROM {full_table_name}").collect()]

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(base_path)
    for partition in fs.listStatus(path):
        if partition.isDirectory():
            date_str = partition.getPath().getName().split("=")[-1]
            if date_str not in existing_partitions:
                print(f"Processing new partition: {date_str}")
                df = spark.read.json(partition.getPath().toString())
                
                if df.rdd.isEmpty():
                    print(f"Skipping empty file for partition: {date_str}")
                    continue
                
                processed_df = process_func(df).withColumn("log_date", lit(date_str))
                processed_df.write.mode("append") \
                    .format("hive") \
                    .partitionBy("log_date") \
                    .saveAsTable(full_table_name)

def process_activity(df):
    df_parsed = df.select(
        col("timestamp").cast("timestamp").alias("activity_ts"),
        col("user_id").cast("int"),
        col("article_id").cast("int"),
        col("content.category").alias("log_category"),
        col("content.premium").alias("log_premium"),
        col("content.accessible_pct").cast("int").alias("accessible_pct"),
        col("engagement.scroll_depth_pct").cast("int").alias("scroll_depth_pct"),
        col("engagement.effective_read_pct").cast("int").alias("effective_read_pct"), 
        col("engagement.paywall_triggered").alias("paywall_triggered")
    )

    categories_df = broadcast(spark.table("article_platform.dim_categories"))

    return df_parsed.join(categories_df, df_parsed["log_category"] == categories_df["name"], "left") \
        .select(
            "activity_ts",
            "user_id", 
            "article_id",
            "log_category",
            "log_premium",
            "accessible_pct",
            "scroll_depth_pct", 
            "effective_read_pct",
            "paywall_triggered",
            col("category_id").alias("sql_category_id")
        )

def process_subscription(df):
    parsed_df = df.select(
        col("user_id").cast("int"),
        col("start_date").cast("timestamp"),
        col("trigger_articles")
    )

    exploded_df = parsed_df.select(
        "user_id", "start_date",
        explode("trigger_articles").alias("trigger")
    ).select(
        "user_id", "start_date",
        col("trigger.article_id").alias("trigger_article_id"),
        col("trigger.read_time").cast("timestamp").alias("read_ts"),
        col("trigger.scroll_depth").cast("int").alias("scroll_depth")
    )
    
    subscriptions_df = broadcast(spark.table("article_platform.dim_subscriptions"))
    
    return exploded_df.join(
        subscriptions_df,
        (exploded_df["user_id"] == subscriptions_df["user_id"]) &
        (exploded_df["start_date"] == subscriptions_df["start_date"]),
        "left"
    ).select(
        subscriptions_df["subscription_id"],
        exploded_df["user_id"],
        exploded_df["start_date"],
        exploded_df["trigger_article_id"],
        exploded_df["read_ts"],
        exploded_df["scroll_depth"]
    )

process_fact_partition("fact_user_activity", f"{HDFS_DATA_DIR}/raw/logs/activity", process_activity)
process_fact_partition("fact_subscription_triggers", f"{HDFS_DATA_DIR}/raw/logs/subscriptions", process_subscription)

print("All tasks completed successfully.")

spark.stop()