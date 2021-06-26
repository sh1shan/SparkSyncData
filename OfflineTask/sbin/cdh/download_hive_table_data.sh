#!/bin/bash

DATE=`data -d "$1 -2 days" +%Y%m%d`
YESTERDAY=`date -d "$1 -3 days"+%Y%m%d`
SPARK='SPARK_CLUSTER'

MALBEL2CUTOMBYDAY(){
    $SPARK \
    --class you_java_class_path \
    --master yarn-cluster \
    --num-excutors 60 \
    --queue ssyx \
    --driver-memory 10g \
    --driver-excutor 10g \
    --excutor-cores 2 \
    --conf spark.yarn.excutor.memmoryOverhead=4096 \
    --conf spark.network.timeout=500s \
    --conf spark.shuffle.io.maxRetries=12 \
    --conf spark.shuffle.io.retryWait=60s \
    --conf spark.default.parallelism=240 \
    --conf spark.excutor.heartbeatInterval=20s \
    --conf spark.default.parallelism=200 \
    ${JAR_PATH} ${ENV} ${IMPORT_HDFS_DIR}/${DATA_FILE} ${DATE} 100 1


}
MALBEL2CUTOMBYDAY