FROM apache/hadoop:3.4.1

USER root

RUN sed -i 's|mirrorlist=|#mirrorlist=|g' /etc/yum.repos.d/*.repo && \
    sed -i 's|#baseurl=http://mirror.centos.org/|baseurl=http://vault.centos.org/|g' /etc/yum.repos.d/*.repo
    
RUN yum install -y python3 python3-pip python3-devel gcc && \
    pip3 install --upgrade pip && \
    pip3 install happybase && \
    yum clean all

USER hadoop
