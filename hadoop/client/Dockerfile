FROM rockylinux:8

RUN dnf install -y java-1.8.0-openjdk-devel wget tar openssh-server which python36 python36-devel gcc gcc-c++ make && \
    dnf clean all

RUN alternatives --set python3 /usr/bin/python3.6

RUN python3 -m pip install cython
RUN python3 -m pip install happybase

RUN JAVA_HOME_PATH=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "export JAVA_HOME=${JAVA_HOME_PATH}" > /etc/environment && \
    echo ${JAVA_HOME_PATH} > /tmp/java_home_path

ENV HADOOP_VERSION=3.4.1
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_PREFIX=${HADOOP_HOME}
ENV HADOOP_COMMON_HOME=${HADOOP_HOME}
ENV HADOOP_HDFS_HOME=${HADOOP_HOME}
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV SQOOP_HOME=/opt/sqoop
ENV FLUME_VERSION=1.11.0
ENV FLUME_HOME=/opt/flume
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SQOOP_HOME/bin:$FLUME_HOME/
ENV SPARK_VERSION=3.5.6
ENV SPARK_HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt \
    && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

RUN wget -q https://archive.apache.org/dist/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz \
    && tar -xzf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz -C /opt \
    && mv /opt/sqoop-1.4.7.bin__hadoop-2.6.0 /opt/sqoop \
    && rm sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz

RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -P /opt/sqoop/lib/

RUN wget -q https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz \
    && tar -xzf apache-flume-${FLUME_VERSION}-bin.tar.gz -C /opt \
    && mv /opt/apache-flume-${FLUME_VERSION}-bin /opt/flume \
    && rm apache-flume-${FLUME_VERSION}-bin.tar.gz

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz

RUN JAVA_HOME_PATH=$(cat /tmp/java_home_path) && \
    echo "export JAVA_HOME=${JAVA_HOME_PATH}" > /etc/profile.d/hadoop.sh && \
    echo 'export HADOOP_HOME=/opt/hadoop' >> /etc/profile.d/hadoop.sh && \
    echo 'export HADOOP_PREFIX=${HADOOP_HOME}' >> /etc/profile.d/hadoop.sh && \
    echo 'export HADOOP_COMMON_HOME=${HADOOP_HOME}' >> /etc/profile.d/hadoop.sh && \
    echo 'export HADOOP_HDFS_HOME=${HADOOP_HOME}' >> /etc/profile.d/hadoop.sh && \
    echo 'export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop' >> /etc/profile.d/hadoop.sh && \
    echo 'export SQOOP_HOME=/opt/sqoop' >> /etc/profile.d/hadoop.sh && \
    echo 'export FLUME_HOME=/opt/flume' >> /etc/profile.d/hadoop.sh && \
    echo 'export SPARK_HOME=${SPARK_HOME}' >> /etc/profile.d/hadoop.sh && \
    echo 'export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SQOOP_HOME/bin:$FLUME_HOME/bin:$SPARK_HOME/bin' >> /etc/profile.d/hadoop.sh && \
    chmod +x /etc/profile.d/hadoop.sh

RUN echo 'source /etc/profile.d/hadoop.sh' >> /root/.bashrc

RUN ssh-keygen -A \
    && mkdir -p /var/run/sshd \
    && echo 'Port 22' > /etc/ssh/sshd_config \
    && echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config \
    && echo 'PasswordAuthentication yes' >> /etc/ssh/sshd_config \
    && echo 'Subsystem sftp internal-sftp' >> /etc/ssh/sshd_config 

RUN echo '#!/bin/bash' > /start.sh \
    && echo 'echo "root:${ROOT_PASSWORD:-Hadoop@123}" | chpasswd' >> /start.sh \
    && echo 'exec /usr/sbin/sshd -D' >> /start.sh \
    && chmod +x /start.sh

EXPOSE 22

ENTRYPOINT ["/start.sh"]