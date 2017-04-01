#!/bin/bash

if [ $# -ne 2 ]; then
   echo "Usage: run-app [local|cluster|plain] [local file]"
   exit 1
fi

inputFullPath=$2
inputFileName=${inputFullPath##*/}
echo "inputFuleName=$inputFileName"

hdfs dfs -rm -f -r /input
hdfs dfs -mkdir -p /input
hdfs dfs -rm -f -r /output
hdfs dfs -mkdir -p /output
hdfs dfs -put $inputFullPath /input

if [ $1x = "cluster"x ]; then
    spark-submit --class com.spark.test.java.WordCountTest \
        --master yarn \
        --deploy-mode cluster \
        ./target/word-count-test-java-1.0.jar hdfs:///input/$inputFileName hdfs:///output/result 2>&1
elif [ $1x = "plain"x ]; then
    java -cp ./target/word-count-test-java-1.0.jar com.spark.test.java.PlainWordCount $inputFullPath 
else 
   spark-submit --class com.spark.test.java.WordCountTest \
       --master local[4] \
        ./target/word-count-test-java-1.0.jar hdfs:///input/$inputFileName hdfs:///output/result 2>&1
fi
