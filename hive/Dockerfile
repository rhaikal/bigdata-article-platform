FROM apache/hive:4.0.0

USER root

RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
    
RUN mkdir -p /opt/hive/lib/ && \
    chown hive:hive /opt/hive/lib/

USER hive

RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -P /opt/hive/lib/