#!/bin/bash

if [ "$1" != "local[*]" ]; then
    echo "Error: Per la preparazione dati usa il master locale. Esegui: bash generate_data.sh local[*]"
    exit 1
fi

# Configurazione percorsi
export ROOT_DIR=$(cd ../ && pwd)
export SPARK_HOME=$HOME/spark-3.5.5-bin-hadoop3
SPARK_CMD="$SPARK_HOME/bin/spark-submit"

# Fattori di replica per le dimensioni maggiori (sovrascrivibili: REPLICAS="2" bash generate_data.sh local[*])
REPLICAS=${REPLICAS:-"2 5"}

echo -e "[SH] ========================================================"
echo -e "[SH] 1. AVVIO DEL PREPROCESSING (File Completo)"
echo -e "[SH] ========================================================"
$SPARK_CMD --master "local[*]" --driver-memory 2g preprocessing.py

if [ $? -ne 0 ]; then
    echo "[ERRORE] Il preprocessing è fallito. Interrompo la pipeline."
    exit 1
fi

echo -e "\n[SH] ========================================================"
echo -e "[SH] 2. GENERAZIONE DELLE PORZIONI (1%, 20%, 50%, 70%)"
echo -e "[SH] ========================================================"
$SPARK_CMD --master "local[*]" generate_portions.py --fractions "0.01 0.2 0.5 0.7"

if [ $? -ne 0 ]; then
    echo "[ERRORE] La generazione delle porzioni è fallita. Interrompo la pipeline."
    exit 1
fi

echo -e "\n[SH] ========================================================"
echo -e "[SH] 3. INGESTIONE DEI DATI IN HADOOP HDFS"
echo -e "[SH] ========================================================"
# Ogni dataset è una cartella: /user/$USER/data/<dataset>/. Spark e Hive leggono la cartella,
# quindi le repliche possono contenere più copie del file senza concatenarle.
DATA_DIR=/user/$USER/data
hdfs dfs -rm -r -f -skipTrash $DATA_DIR > /dev/null 2>&1
hdfs dfs -mkdir -p $DATA_DIR

for name in flights_1 flights_20 flights_50 flights_70 flights_cleaned; do
    echo "[SH] Caricamento di ${name}.csv..."
    hdfs dfs -mkdir -p $DATA_DIR/$name
    hdfs dfs -put -f ../data/processed/${name}.csv $DATA_DIR/$name/${name}.csv
done

# Repliche controllate per le dimensioni maggiori: N copie identiche del dataset completo,
# create con copie interne a HDFS (nessuno spazio occupato sul disco locale)
for n in $REPLICAS; do
    echo "[SH] Creazione della replica ${n}x (${n} copie del dataset completo)..."
    hdfs dfs -mkdir -p $DATA_DIR/flights_x$n
    for i in $(seq 1 $n); do
        hdfs dfs -cp $DATA_DIR/flights_cleaned/flights_cleaned.csv $DATA_DIR/flights_x$n/part-$(printf %02d $i).csv
    done
done

# Librerie di Spark su HDFS, usate dai job su YARN (spark.yarn.jars) al posto dell'upload a ogni esecuzione
if ! hdfs dfs -test -d /spark/jars; then
    echo "[SH] Caricamento delle librerie di Spark su HDFS (/spark/jars)..."
    hdfs dfs -mkdir -p /spark/jars
    hdfs dfs -put -f $SPARK_HOME/jars/*.jar /spark/jars/
fi

echo -e "\n[OK] TUTTI I DATASET SONO PRONTI SU HDFS!"
echo "Verifica locale:  ls -l ../data/processed/"
echo "Verifica Hadoop:  hdfs dfs -du -h $DATA_DIR"