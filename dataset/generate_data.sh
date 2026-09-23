#!/bin/bash

if [ "$1" != "local[*]" ]; then
    echo "Error: Per la preparazione dati usa il master locale. Esegui: bash generate_data.sh local[*]"
    exit 1
fi

# Configurazione percorsi
export ROOT_DIR=$(cd ../ && pwd)
export SPARK_HOME=$HOME/spark-3.5.5-bin-hadoop3
SPARK_CMD="$SPARK_HOME/bin/spark-submit"

echo -e "[SH] ========================================================"
echo -e "[SH] 1. AVVIO DEL PREPROCESSING (File Completo)"
echo -e "[SH] ========================================================"
$SPARK_CMD --master "local[*]" preprocessing.py

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
echo -e "[SH] 3. INGESTIONE REPERTORIO DATI IN HADOOP HDFS"
echo -e "[SH] ========================================================"
# Creazione cartella di destinazione su Hadoop
hdfs dfs -mkdir -p /user/$USER/data

echo "[SH] Caricamento del dataset completo..."
hdfs dfs -put -f ../data/processed/flights_cleaned.csv /user/$USER/data/flights_cleaned.csv

echo "[SH] Caricamento delle porzioni campionate..."
for pct in 1 20 50 70; do
    echo "     -> Inserimento di flights_${pct}.csv in HDFS..."
    hdfs dfs -put -f ../data/processed/flights_${pct}.csv /user/$USER/data/flights_${pct}.csv
done

echo -e "\n[OK] TUTTI I FILE SONO PRONTI E REPLICATI PERFETTAMENTE!"
echo "Verifica locale:  ls -l ../data/processed/"
echo "Verifica Hadoop:  hdfs dfs -ls /user/$USER/data/"