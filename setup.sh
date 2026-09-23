#!/bin/bash

# Configurazione percorsi sicuri
if [ -z "$HADOOP_HOME" ]; then
    export HADOOP_HOME=$HOME/hadoop-3.4.1
fi

if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
fi

if [ -z "$ROOT_DIR" ]; then
    export ROOT_DIR=$(pwd)
fi

echo "[SETUP] Arresto di eventuali istanze attive..."
$HADOOP_HOME/sbin/stop-all.sh

echo "[SETUP] Pulizia profonda dei file temporanei e PID..."
rm -rf /tmp/hadoop-*
rm -rf /tmp/hsperfdata_*

echo "[SETUP] Formattazione del NameNode..."
$HADOOP_HOME/bin/hdfs namenode -format -force

echo "[SETUP] Avvio del Magazzino Dati (HDFS)..."
$HADOOP_HOME/sbin/start-dfs.sh

echo "[SETUP] Avvio del Motore di Calcolo (YARN)..."
$HADOOP_HOME/sbin/start-yarn.sh

echo "[OK] Sistema Hadoop pronto, pulito e avviato al 100%!"