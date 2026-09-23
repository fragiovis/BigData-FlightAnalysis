#!/bin/bash

# Controllo che siano passati tutti i parametri richiesti
if [ $# -lt 3 ]; then
    echo "Uso:  bash run.sh <script_name> <dataset_tag> <master>"
    echo "Es:   bash run.sh job_1 flights_1 local[*]"
    exit 1
fi

# Definiamo i percorsi di Hadoop se non sono già nell'ambiente
if [ -z "$HADOOP_HOME" ]; then
    export HADOOP_HOME=$HOME/hadoop-3.4.1
fi

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$HOME/spark-3.5.5-bin-hadoop3

# Rimuove la cartella di output precedente su HDFS se già esistente per evitare conflitti
hdfs dfs -rm -r -f /user/$USER/spark-core/$1

$SPARK_HOME/bin/spark-submit \
    --master $3 \
    $1.py \
    -input hdfs://localhost:9000/user/$USER/data/$2.csv \
    -output hdfs://localhost:9000/user/$USER/spark-core/$1