#!/bin/bash
SPARK_HOME=/opt/spark20/spark-2.0.0/ \
HADOOP_CONF_DIR=/etc/hadoop/conf/ \
/opt/spark20/spark-2.0.0//bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-java-options "-Dhive.metastore.uris=thrift://hadoop-m2.rtk:9083" \
--queue DMC \
--driver-memory 4g --num-executors 10 --executor-cores 4 --executor-memory 30g \
--class "ideas.Main" \
./scala-spark-hadoop-admin-MOS-0-schema_N1-1.0.jar
