#!/bin/bash

# Controllo formale dell'argomento passato da terminale
if [ "$1" != "local[*]" ] && [ "$1" != "yarn" ] && [ "$1" != "aws" ]; then
    echo "Errore: Argomento master non valido o mancante."
    echo "Uso corretto: ./experiments.sh local[*]"
    echo "              ./experiments.sh yarn"
    echo "              ./experiments.sh aws  (Per l'esecuzione su AWS EMR)"
    exit 1
fi

# Se passiamo 'aws', indichiamo a benchmark.py l'ambiente reale (YARN) ma attiviamo il flag --aws
MASTER_TYPE="$1"
EXTRA_FLAG=""

if [ "$1" == "aws" ]; then
    MASTER_TYPE="yarn"
    EXTRA_FLAG="--aws"
    echo "================================================================="
    echo " AVVIO PIPELINE AUTOMATICA DI BENCHMARK - MODALITA': AWS EMR "
    echo "================================================================="
else
    echo "================================================================="
    echo " AVVIO PIPELINE AUTOMATICA DI BENCHMARK - MODALITA': ${1^^} "
    echo "================================================================="
fi

# Avvio del Benchmark passando l'eventuale flag --aws a Python
python3 benchmark.py job_1 "$MASTER_TYPE" $EXTRA_FLAG --fractions "0.01 0.2 0.5 0.7"
python3 benchmark.py job_2 "$MASTER_TYPE" $EXTRA_FLAG --fractions "0.01 0.2 0.5 0.7"

echo "================================================================="
echo " PIPELINE COMPLETATA. CONTROLLA I RISULTATI NELLA DIRECTORY logs/ "
echo "================================================================="