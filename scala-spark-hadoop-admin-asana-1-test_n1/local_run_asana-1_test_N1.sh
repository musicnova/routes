#!/bin/bash
# /*
#  * COMMAND FOR LOCAL MODE:
#  * -XX:+UseG1GC -Xmx40g -Xms40g
#  *
#  *  # Set SPARK_MEM if it isn't already set since we also use it for this process
#  * SPARK_MEM=${SPARK_MEM:-512m}
#  * export SPARK_MEM

#  * # Set JAVA_OPTS to be able to load native libraries and to set heap size
#  * JAVA_OPTS="$OUR_JAVA_OPTS"
#  * JAVA_OPTS="$JAVA_OPTS -Djava.library.path=$SPARK_LIBRARY_PATH"
#  * JAVA_OPTS="$JAVA_OPTS -Xms$SPARK_MEM -Xmx$SPARK_MEM"
#  */

# /*
#  * COMMAND FOR CLUSTER MODE:
# #!/bin/bash 
# SPARK_HOME=/opt/spark20/spark-2.0.0/ \ 
# HADOOP_CONF_DIR=/etc/hadoop/conf/ \ 
# /opt/spark20/spark-2.0.0//bin/spark-submit \ 
# --master yarn \ 
# --deploy-mode client \ 
# --driver-java-options "-Dhive.metastore.uris=thrift://hadoop-m2.rtk:9083" \ 
# --queue DMC \
# --driver-memory 4g --num-executors 10 --executor-cores 4 --executor-memory 30g \
# --class "ideas.Main" \ 
# ./scala-spark-hadoop-admin-asana-1-test_N1-1.0.jar
# */

java -jar -XX:+UseG1GC -Xmx40g -Xms40g ./scala-spark-hadoop-admin-asana-1-test_N1-1.0.jar

