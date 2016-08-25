#!/bin/sh

MAIN_CLASS=david.work.benchmark.BenchALS
SPARK_JAR=../target/sparkALS-1.0-SNAPSHOT-jar-with-dependencies.jar

#PARA_RATING_FILE="hdfs://10.238.145.51/data/als/movielen/ratings.dat"
#PARA_MOVIE_FILE="hdfs://10.238.145.51/data/als/movielen/movies.dat"
PARA_RATING_FILE="/data/als/movielen/ratings.dat"
PARA_MOVIE_FILE="/data/als/movielen/movies.dat"

spark-submit \
    --class $MAIN_CLASS  \
    --master yarn-client \
    $SPARK_JAR \
    $PARA_RATING_FILE \
    $PARA_MOVIE_FILE
