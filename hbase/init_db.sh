#!/bin/bash

TABLE_NAME="active_users_by_date"
COLUMN_FAMILY="activity"

hbase shell <<EOF
  exists '${TABLE_NAME}'
  if ! exists '${TABLE_NAME}'
    create '${TABLE_NAME}', '${COLUMN_FAMILY}'
  end
  exit
EOF

if [ $? -eq 0 ]; then
    echo "HBase initialization script completed successfully."
    exit 0
else
    echo "HBase initialization script failed."
    exit 1
fi