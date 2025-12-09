from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from helpers import LOG_DIR, HDFS_DATA_DIR, HADOOP_SSH_PREFIX, HADOOP_SCP_PREFIX

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

def get_end_timestamp():
    try:
        end_timestamp = Variable.get("ingestion_end_timestamp", default_var=None)
        return end_timestamp
    except:
        return None

def create_ingest_operator(task_id, log_type, conf_file):
    END_TIMESTAMP = get_end_timestamp()
    
    LOCAL_RAW_LOG_DIR = f"{LOG_DIR}/{log_type}"
    
    # Remote directories on the hadoop-client host
    REMOTE_FLUME_CONF_PATH = f"/opt/flume/conf/{conf_file}"
    REMOTE_INGEST_DIR = f"/tmp/article_platform/logs/{log_type}_ingest"
    
    # Escape the END_TIMESTAMP for safe shell insertion if it exists
    ESCAPED_END_TIMESTAMP = END_TIMESTAMP if END_TIMESTAMP else ""

    return BashOperator(
        task_id=f'ingest_{task_id}_logs',
        bash_command=f'''
            set -e
            
            # --- 1. LOCAL PREPARATION AND FILE TRANSFER (Airflow Host) ---

            # Local temporary directory for files to be ingested
            LOCAL_INGEST_DIR="/tmp/flume_transfer/{log_type}_ingest"
            
            echo "Starting local preparation for {log_type}..."
            
            # Clean and create local ingestion directory
            rm -rf "$LOCAL_INGEST_DIR"
            mkdir -p "$LOCAL_INGEST_DIR"
            
            # Check for source files locally
            JSON_FILES=$(ls {LOCAL_RAW_LOG_DIR}/*.json 2>/dev/null | wc -l)
            
            if [ "$JSON_FILES" -eq 0 ]; then
                echo "No .json files found locally for {log_type}, skipping ingestion"
                exit 0
            fi
            
            echo "Found $JSON_FILES local .json files for {log_type}. Applying timestamp filter if set: '{ESCAPED_END_TIMESTAMP}'"

            # Local Timestamp Filtering
            RAW_TIMESTAMP="{ESCAPED_END_TIMESTAMP}"
            
            if [ -n "$RAW_TIMESTAMP" ]; then
                # Formatting logic
                FORMATTED_TS=$(echo "$RAW_TIMESTAMP" | sed 's/\\([0-9]{{4}}\\)\\([0-9]{{2}}\\)\\([0-9]{{2}}\\)\\([0-9]{{2}}\\)\\([0-9]{{2}}\\)\\([0-9]{{2}}\\)/\\1-\\2-\\3 \\4:\\5:\\6/')
                END_TIMESTAMP_FILTER=$(date -d "$FORMATTED_TS" +%Y%m%d%H%M%S 2>/dev/null || echo "$RAW_TIMESTAMP")
                
                # Use globbing and a loop for file filtering and staging
                for f in {LOCAL_RAW_LOG_DIR}/*.json; do
                    [ -e "$f" ] || continue
                    fname=$(basename "$f")
                    if [[ "$fname" =~ ([0-9]{{8}})_([0-9]{{6}}) ]]; then
                        file_date="${{BASH_REMATCH[1]}}"
                        file_time="${{BASH_REMATCH[2]}}"
                        file_timestamp="${{file_date}}${{file_time}}"
                        if [ "$file_timestamp" -lt "$END_TIMESTAMP_FILTER" ]; then
                            cp "$f" "$LOCAL_INGEST_DIR/"
                            echo "INCLUDED: $fname (timestamp $file_timestamp < $END_TIMESTAMP_FILTER)"
                        else
                            echo "EXCLUDED: $fname (timestamp $file_timestamp >= $END_TIMESTAMP_FILTER)"
                        fi
                    fi
                done
            else
                # If no timestamp, stage all files
                cp {LOCAL_RAW_LOG_DIR}/*.json "$LOCAL_INGEST_DIR/" 2>/dev/null || true
                echo "No END_TIMESTAMP specified, all files staged for ingestion."
            fi

            # Check if any files were staged for transfer
            INGEST_FILES=$(ls "$LOCAL_INGEST_DIR"/*.json 2>/dev/null | wc -l)
            if [ "$INGEST_FILES" -eq 0 ]; then
                echo "No files passed filtering, skipping ingestion."
                exit 0
            fi
            
            echo "Transferring $INGEST_FILES staged files to hadoop-client..."
            
            # Transfer Staged log files
            {HADOOP_SSH_PREFIX} "rm -rf {REMOTE_INGEST_DIR} && mkdir -p {REMOTE_INGEST_DIR}"

            {HADOOP_SCP_PREFIX} \
                "$LOCAL_INGEST_DIR"/*.json root@hadoop-client:{REMOTE_INGEST_DIR}/

            if [ $? -ne 0 ]; then
                echo "File transfer failed, aborting ingestion."
                exit 1
            fi
            
            # --- 2. REMOTE EXECUTION (hadoop-client Host) ---
            
            echo "Transfer complete. Proceeding with remote Flume ingestion..."
            
            {HADOOP_SSH_PREFIX} '
            set -e
            
            # Setup remote paths
            REMOTE_CONF_FILE="{REMOTE_FLUME_CONF_PATH}"
            REMOTE_INGEST_DIR="{REMOTE_INGEST_DIR}"
            REMOTE_MODIFIED_CONF="/tmp/flume-{log_type}-modified.conf"
            REMOTE_HDFS_DIR="{HDFS_DATA_DIR}/raw/logs/{log_type}"
            
            # Flume requires JAVA_OPTS
            export JAVA_OPTS="-Xms512m -Xmx1g -XX:+UseG1GC"

            # Check for remote files after transfer
            REMOTE_JSON_FILES=$(ls "$REMOTE_INGEST_DIR"/*.json 2>/dev/null | wc -l)
            if [ "$REMOTE_JSON_FILES" -eq 0 ]; then
                echo "No .json files found remotely for {log_type}, exiting remote execution."
                exit 0
            fi
            
            echo "Starting Flume with $REMOTE_JSON_FILES files..."
            
            # Modify Flume conf to use the remote _ingest directory
            cp "$REMOTE_CONF_FILE" "$REMOTE_MODIFIED_CONF"
            sed -i "s|spoolDir *=.*|spoolDir = $REMOTE_INGEST_DIR|g" "$REMOTE_MODIFIED_CONF"
            
            # Start Flume
            flume-ng agent --conf /etc/flume --conf-file "$REMOTE_MODIFIED_CONF" --name agent -Dflume.root.logger=INFO,CONSOLE &
            
            FLUME_PID=$!
            echo "{log_type} Flume agent started with PID: $FLUME_PID"

            # Wait for Flume to finish processing
            echo "Waiting for Flume to process all files..."
            
            MAX_WAIT=1800  # 30 minutes timeout
            ELAPSED=0
            CHECK_INTERVAL=10
            STABLE_COUNT=0
            REQUIRED_STABLE_CHECKS=3  # Need 3 consecutive checks showing completion
            
            while [ $ELAPSED -lt $MAX_WAIT ]; do
                # Check both spool directory and HDFS temp files
                UNCOMPLETED_FILES=$(ls "$REMOTE_INGEST_DIR"/*.json 2>/dev/null | wc -l || echo 0)
                TMP_FILES=$(hdfs dfs -ls "$REMOTE_HDFS_DIR"/*/*.tmp 2>/dev/null | wc -l || echo 0)
                
                if [ "$UNCOMPLETED_FILES" -eq 0 ] && [ "$TMP_FILES" -eq 0 ]; then
                    STABLE_COUNT=$((STABLE_COUNT + 1))
                    echo "Processing appears complete (check $STABLE_COUNT/$REQUIRED_STABLE_CHECKS)..."
                    
                    if [ "$STABLE_COUNT" -ge "$REQUIRED_STABLE_CHECKS" ]; then
                        echo "Confirmed: all files processed successfully"
                        break
                    fi
                else
                    STABLE_COUNT=0
                    echo "Still processing {log_type} logs... ($UNCOMPLETED_FILES files in spool, $TMP_FILES .tmp files in HDFS)"
                fi
                
                sleep $CHECK_INTERVAL
                ELAPSED=$((ELAPSED + CHECK_INTERVAL))
            done
            
            if [ $ELAPSED -ge $MAX_WAIT ]; then
                echo "WARNING: Timeout reached after $MAX_WAIT seconds"
            fi
            
            # Give Flume extra time to gracefully close any remaining file handles
            echo "Allowing Flume time for final cleanup operations..."
            sleep 15
            
            # Graceful shutdown: send SIGTERM and wait
            echo "Sending graceful shutdown signal to Flume (PID: $FLUME_PID)..."
            if kill -0 $FLUME_PID 2>/dev/null; then
                kill -TERM $FLUME_PID
                
                # Wait for graceful shutdown (up to 30 seconds)
                SHUTDOWN_WAIT=0
                while [ $SHUTDOWN_WAIT -lt 30 ] && kill -0 $FLUME_PID 2>/dev/null; do
                    echo "Waiting for Flume to shutdown gracefully..."
                    sleep 2
                    SHUTDOWN_WAIT=$((SHUTDOWN_WAIT + 2))
                done
                
                # Force kill if still running
                if kill -0 $FLUME_PID 2>/dev/null; then
                    echo "Force killing Flume (graceful shutdown timed out)"
                    kill -9 $FLUME_PID 2>/dev/null || true
                else
                    echo "Flume shutdown gracefully"
                fi
            else
                echo "Flume process already terminated"
            fi
            
            # Wait for process to fully exit
            wait $FLUME_PID 2>/dev/null || true
            
            # Additional wait to ensure HDFS operations are flushed
            echo "Final wait for HDFS to complete any pending operations..."
            sleep 5
            
            # Final verification
            FINAL_TMP_COUNT=$(hdfs dfs -ls "$REMOTE_HDFS_DIR"/*/*.tmp 2>/dev/null | wc -l || echo 0)
            FINAL_SPOOL_COUNT=$(ls "$REMOTE_INGEST_DIR"/*.json 2>/dev/null | wc -l || echo 0)
            
            if [ "$FINAL_TMP_COUNT" -eq 0 ] && [ "$FINAL_SPOOL_COUNT" -eq 0 ]; then
                echo "SUCCESS: All files processed successfully on hadoop-client."
                
                # Cleanup remote ingest directory
                rm -rf "$REMOTE_INGEST_DIR"
                rm -f "$REMOTE_MODIFIED_CONF"
                
                exit 0
            else
                echo "ERROR: Some files remain unprocessed:"
                echo "  - .tmp files in HDFS: $FINAL_TMP_COUNT"
                echo "  - Unprocessed files in spool: $FINAL_SPOOL_COUNT"
                
                if [ "$FINAL_TMP_COUNT" -gt 0 ]; then
                    echo "Listing .tmp files:"
                    hdfs dfs -ls "$REMOTE_HDFS_DIR"/*/*.tmp 2>/dev/null || true
                fi
                
                exit 1
            fi
            '
            
            REMOTE_EXIT_CODE=$?
            
            if [ $REMOTE_EXIT_CODE -ne 0 ]; then
                echo "Remote execution failed with exit code $REMOTE_EXIT_CODE"
                exit $REMOTE_EXIT_CODE
            fi
            
            # --- 3. LOCAL CLEANUP (Airflow Host) ---
            
            echo "Remote ingestion complete. Starting local cleanup/marking..."
            
            # Replicate the completion marking logic locally
            RAW_TIMESTAMP="{ESCAPED_END_TIMESTAMP}"

            if [ -n "$RAW_TIMESTAMP" ]; then
                # Formatting logic
                FORMATTED_TS=$(echo "$RAW_TIMESTAMP" | sed 's/\\([0-9]{{4}}\\)\\([0-9]{{2}}\\)\\([0-9]{{2}}\\)\\([0-9]{{2}}\\)\\([0-9]{{2}}\\)\\([0-9]{{2}}\\)/\\1-\\2-\\3 \\4:\\5:\\6/')
                END_TIMESTAMP_MARKER=$(date -d "$FORMATTED_TS" +%Y%m%d%H%M%S 2>/dev/null || echo "$RAW_TIMESTAMP")
                
                # Check for files and move them to .COMPLETED
                for f in {LOCAL_RAW_LOG_DIR}/*.json; do
                    [ -e "$f" ] || continue
                    fname=$(basename "$f")
                    if [[ "$fname" =~ ([0-9]{{8}})_([0-9]{{6}}) ]]; then
                        file_date="${{BASH_REMATCH[1]}}"
                        file_time="${{BASH_REMATCH[2]}}"
                        file_timestamp="${{file_date}}${{file_time}}"
                        if [ "$file_timestamp" -lt "$END_TIMESTAMP_MARKER" ]; then
                            if [ ! -e "$f.COMPLETED" ]; then
                                mv "$f" "$f.COMPLETED"
                                echo "MARKED: $fname as completed."
                            fi
                        fi
                    fi
                done
            fi
            
            # Clean up local staging directory
            rm -rf "$LOCAL_INGEST_DIR"
            
            echo "{log_type} logs ingestion and cleanup complete!"
        ''',
        execution_timeout=timedelta(minutes=45),
    )


with DAG(
    'flume_ingestion',
    tags=['article-platform', 'ingestion'],
    default_args=default_args,
    catchup=False,
    max_active_runs=2,
    schedule=None,
) as dag:

    log_types = {
        'activity': 'activity',
        'users': 'users', 
        'articles': 'articles',
        'subs': 'subscriptions'
    }

    ingest_operators = []

    for task_id, log_type in log_types.items():
        ingest_operators.append(
            create_ingest_operator(
                task_id, 
                log_type, 
                f"flume-{log_type}.conf"
            )
        )

    check_hdfs = BashOperator(
        task_id='check_hdfs_results',
        bash_command=f'''
            {HADOOP_SSH_PREFIX} 'hdfs dfs -ls -R /data/raw/logs/'
        ''',
    )

    ingest_operators >> check_hdfs