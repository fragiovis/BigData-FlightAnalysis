#!/bin/bash

# Controllo che siano passati tutti i parametri richiesti
if [ $# -lt 3 ]; then
    echo "Uso:  bash run.sh <script_name> <dataset_tag> <master>"
    echo "Es:   bash run.sh job_1 flights_1 yarn"
    exit 1
fi

# 1. Configurazione Variabili d'Ambiente per l'esecuzione
if [ -z "$HADOOP_HOME" ]; then
    export HADOOP_HOME=$HOME/hadoop-3.4.1
fi
export HIVE_HOME=$HOME/apache-hive-4.0.0-bin
export PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH

# SUPER-PATCH JAVA 21 (Client locale)
JVM_FLAGS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.math=ALL-UNNAMED \
--add-opens=java.base/java.text=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.xml/jdk.xml.internal=ALL-UNNAMED"

export HADOOP_OPTS="$HADOOP_OPTS $JVM_FLAGS"
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS $JVM_FLAGS"
export HIVE_CLIENT_OPTS="$HIVE_CLIENT_OPTS $JVM_FLAGS"

# Stringa pulita in riga singola per i nodi del cluster YARN
JVM_FLAGS_ONELINE="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.xml/jdk.xml.internal=ALL-UNNAMED"

# ========================================================================
# 🛡️ INIEZIONE CHIRURGICA DEL CLASSPATH E DEI FLAG DENTRO HADOOP XML
# ========================================================================
REAL_MAPRED_SITE="$HADOOP_HOME/etc/hadoop/mapred-site.xml"

# Crea backup di sicurezza
cp "$REAL_MAPRED_SITE" "${REAL_MAPRED_SITE}.bak"

# Esportiamo le variabili nell'ambiente per passarle a Python senza problemi di escaping di caratteri speciali (* o :)
export JVM_FLAGS_ENV="$JVM_FLAGS_ONELINE"
export HADOOP_CP_ENV=$($HADOOP_HOME/bin/hadoop classpath)

echo "[HIVE] Applicazione della patch di rete per sbloccare i container YARN..."
python3 -c "
import xml.etree.ElementTree as ET
import os
import sys

file_path = '$REAL_MAPRED_SITE'
flags = os.environ['JVM_FLAGS_ENV']
classpath = os.environ['HADOOP_CP_ENV']

try:
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    properties_to_add = {
        'mapreduce.map.java.opts': flags,
        'mapreduce.reduce.java.opts': flags,
        'yarn.app.mapreduce.am.command-opts': flags,
        'mapreduce.application.classpath': classpath
    }
    
    for name, val in properties_to_add.items():
        # Rimuove duplicati vecchi
        for prop in root.findall('property'):
            n = prop.find('name')
            if n is not None and n.text == name:
                root.remove(prop)
        
        # Inserisce tag nativo pulito
        p = ET.SubElement(root, 'property')
        ET.SubElement(p, 'name').text = name
        ET.SubElement(p, 'value').text = val
        
    tree.write(file_path, encoding='utf-8', xml_declaration=True)
except Exception as e:
    print('Errore patch XML:', e)
    sys.exit(1)
"

# Sincronizza Hive con la configurazione aggiornata
cp $HADOOP_HOME/etc/hadoop/*.xml $HIVE_HOME/conf/ 2>/dev/null
# ========================================================================

rm -f derby.log

# CONTROLLO INTELLIGENTE METASTORE
if [ ! -d "metastore_db" ]; then
    echo "[HIVE] Inizializzazione dello schema Derby..."
    schematool -dbType derby -initSchema > /dev/null 2>&1
fi

# Traduzione dinamica della flag master
if [ "$3" == "local[*]" ]; then
    FRAMEWORK="local"
else
    FRAMEWORK="yarn"
fi

# 2. DEFINIZIONE DEI PERCORSI SU HDFS
INPUT_HDFS_FILE="/user/$USER/data/$2.csv"
STAGING_PATH="/user/$USER/hive_staging/$1"
OUTPUT_PATH="/user/$USER/hive/$1"

echo "[HIVE] Isolamento del dataset su HDFS per la Tabella Esterna..."
hdfs dfs -mkdir -p "$STAGING_PATH"
hdfs dfs -rm -f "$STAGING_PATH/*" 2>/dev/null
hdfs dfs -cp "$INPUT_HDFS_FILE" "$STAGING_PATH/"
hdfs dfs -rm -r -f "$OUTPUT_PATH" 2>/dev/null

echo "[HIVE] Pre-compilazione del file SQL (Sostituzione variabili in Bash)..."

# Generiamo il file temporaneo HQL
echo "SET fs.defaultFS=hdfs://localhost:9000;" > "temp_$1.hql"
echo "SET hive.variable.substitute=true;" >> "temp_$1.hql"
echo "SET mapreduce.framework.name=$FRAMEWORK;" >> "temp_$1.hql"

if [ "$FRAMEWORK" == "local" ]; then
    echo "SET mapreduce.jobtracker.staging.root.dir=/tmp/hadoop/staging;" >> "temp_$1.hql"
    echo "SET hive.exec.local.scratchdir=/tmp/hive/local-scratch;" >> "temp_$1.hql"
    echo "SET hive.exec.scratchdir=/tmp/hive/scratch;" >> "temp_$1.hql"
fi

# Accodiamo la query originale sostituendo i tag di Staging e Output
sed -e "s|\${staging_path}|$STAGING_PATH|g" \
    -e "s|\${output_path}|$OUTPUT_PATH|g" \
    "$1.hql" >> "temp_$1.hql"

echo "[HIVE] Avvio esecuzione MapReduce tramite Beeline per il file $1.hql ($3)..."

# Lancio lineare tramite Beeline
beeline -u jdbc:hive2:// -n "$USER" -f "temp_$1.hql"

# ========================================================================
# 🧹 RIPRISTINO DI SICUREZZA ASSOLUTO (AVVIENE ADESSO CON HADOOP ATTIVO)
# ========================================================================
echo "[HIVE] Ripristino dei file di configurazione originali di Hadoop..."
if [ -f "${REAL_MAPRED_SITE}.bak" ]; then
    mv "${REAL_MAPRED_SITE}.bak" "$REAL_MAPRED_SITE"
fi
cp $HADOOP_HOME/etc/hadoop/*.xml $HIVE_HOME/conf/ 2>/dev/null

hdfs dfs -rm -r -f "$STAGING_PATH" 2>/dev/null
rm -f "temp_$1.hql"