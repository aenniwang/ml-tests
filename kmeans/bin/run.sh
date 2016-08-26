#!/bin/sh

MAIN_CLASS=david.work.benchmark.BenchmarkKmeans
SPARK_JAR=../target/kmeans-1.0-SNAPSHOT-jar-with-dependencies.jar

PARA_RATING_FILE="/data/mllib/kmeans_data.txt"

spark-submit \
    --class $MAIN_CLASS  \
    --master yarn-client \
    $SPARK_JAR \
    $PARA_RATING_FILE \
    $PARA_MOVIE_FILE