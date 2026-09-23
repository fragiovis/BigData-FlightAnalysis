#!/usr/bin/env python3
"""
generate_portions.py — Porzioni del dataset pulito per le dimensioni minori.

Ogni riga riceve una sola volta un numero casuale u in [0, 1) con seed fisso; la porzione del p%
contiene le righe con u < p. Le porzioni sono quindi annidate (1% ⊂ 20% ⊂ 50% ⊂ 70% ⊂ 100%) e
riproducibili: un dataset più grande contiene sempre tutti i voli di quelli più piccoli.

Le dimensioni maggiori (repliche 2× e 5×) vengono create direttamente su HDFS da generate_data.sh.
"""

import argparse
import os
import shutil

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Rilevamento della radice del progetto risalendo di un livello da dataset/
ROOT_DIR = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
SEED = 42

parser = argparse.ArgumentParser()
parser.add_argument("--fractions", type=str, help="Fractions to split the dataset")
args = parser.parse_args()
fractions = sorted(map(float, args.fractions.split()))

spark = SparkSession.builder.appName("flight-generate-portions").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

source_path = os.path.join(ROOT_DIR, "data", "processed", "flights_cleaned.csv")
print(f"[PYSPARK] Lettura del dataset pulito da: {source_path}")

# Tutte le colonne come stringhe: le porzioni riportano i valori esattamente come nel file pulito
df = spark.read.option("header", True).csv(f"file://{source_path}")
df = df.withColumn("_u", F.rand(seed=SEED)).persist(StorageLevel.MEMORY_AND_DISK)

for fraction in fractions:
    percentage = int(round(fraction * 100))
    print(f"[PYSPARK] Generazione della porzione del {percentage}%...")
    portion = df.filter(F.col("_u") < fraction).drop("_u")

    # Cartella temporanea locale per l'output di Spark, poi il file part-*.csv viene rinominato
    temp_dir = os.path.join(ROOT_DIR, "data", "processed", f"temp_{percentage}")
    portion.coalesce(1).write.option("header", True).mode("overwrite").csv(f"file://{temp_dir}")
    for file in os.listdir(temp_dir):
        if file.endswith(".csv"):
            os.replace(os.path.join(temp_dir, file),
                       os.path.join(ROOT_DIR, "data", "processed", f"flights_{percentage}.csv"))
    shutil.rmtree(temp_dir)
    print(f"[OK] Porzione {percentage}% salvata in data/processed/flights_{percentage}.csv")

df.unpersist()
spark.stop()
