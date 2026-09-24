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
# La tabella esterna punta direttamente alla cartella del dataset (nessuna copia in staging)
INPUT_PATH="$HDFS_BASE/data/$DATASET_TAG"
OUTPUT_PATH="$HDFS_BASE/hive/$SCRIPT_NAME/aws/$DATASET_TAG"

hdfs dfs -rm -r -f "$OUTPUT_PATH" 2>/dev/null

echo "[HIVE AWS] Pre-compilazione del file SQL (Sostituzione variabili in Bash)..."

# Generiamo il file temporaneo HQL SENZA configurazioni locali/localhost
echo "SET hive.variable.substitute=true;" > "temp_aws_$SCRIPT_NAME.hql"
echo "SET mapreduce.framework.name=yarn;" >> "temp_aws_$SCRIPT_NAME.hql"

# Dataset senza la colonna dest (suffisso -8c): Hive legge per posizione, quindi la colonna
# va tolta anche dalla definizione della tabella esterna
DROP_DEST=""
if [[ "$DATASET_TAG" == *-8c ]]; then DROP_DEST="-e /dest[[:space:]]STRING,/d"; fi

# Accodiamo la query originale sostituendo i percorsi di input e output
sed -e "s|\${input_path}|$INPUT_PATH|g" \
    -e "s|\${output_path}|$OUTPUT_PATH|g" \
    $DROP_DEST \
    "$SCRIPT_NAME.hql" >> "temp_aws_$SCRIPT_NAME.hql"

echo "[HIVE AWS] Avvio esecuzione MapReduce tramite Beeline su EMR..."
# Su AWS ci si connette all'endpoint HiveServer2 locale pre-configurato
beeline -u "jdbc:hive2://localhost:10000/default" -n "hadoop" -f "temp_aws_$SCRIPT_NAME.hql"
BEELINE_RC=$?

if [ $BEELINE_RC -eq 0 ]; then
    echo -e "\n--- ANTEPRIMA RISULTATI HIVE $SCRIPT_NAME (TOP 10) ---"
    hdfs dfs -cat "$OUTPUT_PATH/*" 2>/dev/null | head -10
fi

# Pulizia finale dei file temporanei
echo "[HIVE AWS] Pulizia dei file temporanei..."
rm -f "temp_aws_$SCRIPT_NAME.hql"

exit $BEELINE_RC