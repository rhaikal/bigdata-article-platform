#!/usr/bin/env bash

export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export PYSPARK_PYTHON=python3