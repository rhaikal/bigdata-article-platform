FROM apache/airflow:3.1.3

USER root

# Install SSH client and sshpass
RUN apt-get update && \
    apt-get install -y openssh-client sshpass && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt