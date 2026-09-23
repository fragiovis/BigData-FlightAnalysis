#!/usr/bin/env python3
import os
import argparse
import shutil
from pyspark.sql import SparkSession

# Rilevamento della radice del progetto Flight/ risalendo di un livello da dataset/
ROOT_DIR = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))

# Configurazione del parser degli argomenti
parser = argparse.ArgumentParser()
# CORREZIONE: Cambiato parse_argument in add_argument
parser.add_argument("--fractions", type=str, help="Fractions to split the dataset")
args = parser.parse_args()
fractions = list(map(float, args.fractions.split()))

spark = SparkSession.builder.appName("flight-generate-portions").getOrCreate()

source_path = os.path.join(ROOT_DIR, "data", "processed", "flights_cleaned.csv")
print(f"[PYSPARK] Lettura del dataset pulito da: {source_path}")

# Carichiamo il dataset pulito
df = spark.read.option("header", True).csv(f"file://{source_path}")

for fraction in fractions:
    percentage = int(fraction * 100)
    print(f"[PYSPARK] Generazione della porzione del {percentage}%...")

    # Campionamento casuale senza rimpiazzo
    df_portion = df.sample(withReplacement=False, fraction=fraction, seed=42)

    # Creiamo una cartella temporanea locale per l'output di Spark
    temp_dir = os.path.join(ROOT_DIR, "data", "processed", f"temp_{percentage}")
    df_portion.coalesce(1).write.option("header", True).mode("overwrite").csv(f"file://{temp_dir}")

    # Identifichiamo il file part-*.csv e lo rinominiamo in modo pulito
    for file in os.listdir(temp_dir):
        if file.endswith(".csv"):
            shutil_source = os.path.join(temp_dir, file)
            shutil_dest = os.path.join(ROOT_DIR, "data", "processed", f"flights_{percentage}.csv")
            os.replace(shutil_source, shutil_dest)

    # Pulizia totale dei residui di Spark
    shutil.rmtree(temp_dir)
    print(f"[OK] Porzione {percentage}% salvata in data/processed/flights_{percentage}.csv")

spark.stop()