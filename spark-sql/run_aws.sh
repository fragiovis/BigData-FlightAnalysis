#!/bin/bash

# Controllo che siano passati tutti i parametri richiesti
if [ $# -lt 3 ]; then
    echo "Uso:  bash run_aws.sh <script_name> <dataset_tag> <master>"
    echo "Es:   bash run_aws.sh job_1 flights_1 yarn"
    exit 1
fi

HDFS_BASE="/user/hadoop"

# Rimuove la cartella di output precedente su HDFS
hdfs dfs -rm -r -f $HDFS_BASE/spark-sql/$1

# Lancio globale
spark-submit \
    --master $3 \
    $1.py \
    -input $HDFS_BASE/data/$2.csv \
    -output $HDFS_BASE/spark-sql/$1