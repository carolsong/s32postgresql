#!/bin/bash

JARS=/usr/local/spark/lib/aws-java-sdk-1.7.4.jar
JARS=$JARS,/usr/local/spark/lib/hadoop-aws-2.7.1.jar
JARS=$JARS,/usr/local/spark/lib/postgresql-42.2.5.jar

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export POSTGRES_HOST='0.0.0.0'
export POSTGRES_DB='tonebnb'
export POSTGRES_PORT='5432'
export POSTGRES_USER='sa'
export POSTGRES_PWD='sa'

export PYSPARK_PYTHON=/usr/bin/python3

echo "PATH[spark.sh]: $PROJECT_DIR/spark.sh"

SPARK_MASTER=spark://54.190.134.137:7077
SPAKR_SUBMIT=/usr/local/spark/bin/spark-submit
PY_SPARK=$PROJECT_DIR/spark.py

$SPAKR_SUBMIT --master "$SPARK_MASTER" --jars "$JARS" "$PY_SPARK"