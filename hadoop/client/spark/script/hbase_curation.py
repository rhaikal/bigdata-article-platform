from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, date_sub, current_date
import happybase

def get_connection():
    return happybase.Connection(host='hbase-thrift', port=9091)

spark = SparkSession.builder \
    .appName("HiveToHBaseTransform") \
    .enableHiveSupport() \
    .getOrCreate()

check_data_df = spark.sql("""
    SELECT COUNT(*) as cnt 
    FROM article_platform.fact_user_activity f
    WHERE to_date(cast(log_date as string), 'yyyyMMdd') == current_date()
""").collect()[0]['cnt']

active_users_df = spark.sql("""
    SELECT log_date, user_id
    FROM article_platform.fact_user_activity
    WHERE to_date(cast(log_date as string), 'yyyyMMdd') >= current_date()
""").withColumn("row_key", concat_ws("#", col("log_date"), col("user_id")))

def write_active_users_to_hbase(data):
    connection = get_connection()
    table = connection.table('active_users_by_date')
    batch = table.batch(batch_size=1000)
    
    for row in data:
        row_key = row.row_key.encode('utf-8')
        batch.put(row_key, {b'activity:active': b'true'})
    
    batch.send()
    connection.close()

if check_data_df > 0:
    write_active_users_to_hbase(active_users_df.collect())

spark.stop()
