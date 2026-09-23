#!/usr/bin/env python3
"""
job_1.py — Analisi Statistiche delle compagnie aeree.
Tecnologia: Spark SQL (Adattato per l'esecuzione distribuita ed HDFS)
"""

import argparse
import time
from pyspark.sql import SparkSession

def main():
    # 1. Parsing degli argomenti in linea con lo stile del run.sh (-input e -output)
    parser = argparse.ArgumentParser()
    parser.add_argument("-input", type=str, help="Path to input file")
    parser.add_argument("-output", type=str, help="Path to output folder")
    args = parser.parse_args()

    # 2. Inizializzazione della SparkSession 
    # (Agnostica: il master e la memoria vengono ereditati dal comando spark-submit)
    spark = SparkSession.builder \
        .appName("spark-sql#job-1") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    # Disattiviamo i log troppo invasivi per mantenere pulito il terminale
    spark.sparkContext.setLogLevel("ERROR")

    print(f"[JOB 1] Caricamento del dataset da HDFS: {args.input}")
    
    # Lettura del dataset preprocessato
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(args.input)

    # Creazione della vista temporanea per le query SQL
    df.createOrReplaceTempView("flights_data")

    # 3. Definizione della Query SQL per l'Analisi delle Compagnie (Ex Job 3.1)
    query_1 = """
    SELECT 
        op_unique_carrier AS compagnia,
        origin AS aeroporto_partenza,
        COUNT(*) AS numero_voli,
        MIN(arr_delay) AS ritardo_min_arrivo,
        MAX(arr_delay) AS ritardo_max_arrivo,
        ROUND(AVG(arr_delay), 2) AS ritardo_medio_arrivo,
        ROUND(AVG(cancelled), 4) AS tasso_cancellazione,
        CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(month))) AS mesi_operativi
    FROM 
        flights_data
    GROUP BY 
        op_unique_carrier, 
        origin
    ORDER BY 
        compagnia ASC, 
        numero_voli DESC
    """

    print("[JOB 1] Esecuzione della query SQL distribuita...")
    start_job = time.time()

    risultato_1 = spark.sql(query_1)

    # Mostriamo le prime 10 righe a schermo per verifica visiva immediata
    risultato_1.show(10, truncate=False)

    end_job = time.time()
    print(f"[JOB 1] Calcolo completato in {end_job - start_job:.2f} secondi.")

    # 4. Salvataggio dei risultati su HDFS (Fondamentale per consentire i benchmark)
    print(f"[JOB 1] Salvataggio dei risultati in HDFS: {args.output}")
    risultato_1.coalesce(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(args.output)

    spark.stop()

if __name__ == "__main__":
    main()