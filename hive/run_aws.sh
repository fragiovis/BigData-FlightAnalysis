#!/bin/bash

# Controllo che siano passati tutti i parametri richiesti
if [ $# -lt 3 ]; then
    echo "Uso:  bash run_aws.sh <script_name> <dataset_tag> <master>"
    echo "Es:   bash run_aws.sh job_1 flights_1 yarn"
    exit 1
fi

SCRIPT_NAME=$1
DATASET_TAG=$2

# Nel Cloud l'utente è rigidamente hadoop e i percorsi sono nativi senza porte
HDFS_BASE="/user/hadoop"
STAGING_PATH="$HDFS_BASE/hive_staging/$SCRIPT_NAME"
OUTPUT_PATH="$HDFS_BASE/hive/$SCRIPT_NAME"

echo "[HIVE AWS] Isolamento del dataset su HDFS per la Tabella Esterna..."
hdfs dfs -mkdir -p "$STAGING_PATH"
hdfs dfs -rm -f "$STAGING_PATH/*" 2>/dev/null
hdfs dfs -cp "$HDFS_BASE/data/$DATASET_TAG.csv" "$STAGING_PATH/"
hdfs dfs -rm -r -f "$OUTPUT_PATH" 2>/dev/null

echo "[HIVE AWS] Pre-compilazione del file SQL (Sostituzione variabili in Bash)..."

# Generiamo il file temporaneo HQL SENZA configurazioni locali/localhost
echo "SET hive.variable.substitute=true;" > "temp_aws_$SCRIPT_NAME.hql"
echo "SET mapreduce.framework.name=yarn;" >> "temp_aws_$SCRIPT_NAME.hql"

# Accodiamo la query originale sostituendo i tag di Staging e Output
sed -e "s|\${staging_path}|$STAGING_PATH|g" \
    -e "s|\${output_path}|$OUTPUT_PATH|g" \
    "$SCRIPT_NAME.hql" >> "temp_aws_$SCRIPT_NAME.hql"

echo "[HIVE AWS] Avvio esecuzione MapReduce tramite Beeline su EMR..."
# Su AWS ci si connette all'endpoint HiveServer2 locale pre-configurato
beeline -u "jdbc:hive2://localhost:10000/default" -n "hadoop" -f "temp_aws_$SCRIPT_NAME.hql"

# Pulizia finale dei file temporanei
echo "[HIVE AWS] Pulizia dello staging e dei file temporanei..."
hdfs dfs -rm -r -f "$STAGING_PATH" 2>/dev/null
rm -f "temp_aws_$SCRIPT_NAME.hql"