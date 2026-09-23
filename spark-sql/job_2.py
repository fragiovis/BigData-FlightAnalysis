#!/usr/bin/env python3
"""
job_2.py — Analisi 3.2: Report mensile ritardi e cause per aeroporto.
Tecnologia: Spark SQL puro con Window Functions
"""

import argparse
import time
from pyspark.sql import SparkSession

def main():
    # 1. Parsing degli argomenti in linea con run.sh
    parser = argparse.ArgumentParser()
    parser.add_argument("-input", type=str, help="Path to input file")
    parser.add_argument("-output", type=str, help="Path to output folder")
    args = parser.parse_args()

    # 2. Inizializzazione della SparkSession
    spark = SparkSession.builder \
        .appName("spark-sql#job-2") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    # Disattiviamo i log troppo invasivi
    spark.sparkContext.setLogLevel("ERROR")

    print(f"[JOB 2] Caricamento del dataset da HDFS: {args.input}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input)
    df.createOrReplaceTempView("flights_data")

    # 3. Definizione della Query SQL complessa (CTE + Window Functions)
    # Spiegazione: 
    # - La prima parte fa i calcoli per le tre fasce (basso, medio, alto) usando CASE WHEN.
    # - La seconda parte (CTE) unisce i codici di cancellazione e ritardo, li conta, li classifica con ROW_NUMBER e prende i primi 3.
    query_2 = """
    WITH stats_fasce AS (
        SELECT 
            origin AS aeroporto,
            month AS mese,
            -- Fascia Bassa (< 15 min)
            COUNT(CASE WHEN dep_delay < 15 THEN 1 END) AS voli_ritardo_basso,
            ROUND(AVG(CASE WHEN dep_delay < 15 THEN dep_delay END), 2) AS ritardo_medio_dep_basso,
            ROUND(AVG(CASE WHEN dep_delay < 15 THEN arr_delay END), 2) AS ritardo_medio_arr_basso,
            -- Fascia Media (15 - 60 min)
            COUNT(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN 1 END) AS voli_ritardo_medio,
            ROUND(AVG(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN dep_delay END), 2) AS ritardo_medio_dep_medio,
            ROUND(AVG(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN arr_delay END), 2) AS ritardo_medio_arr_medio,
            -- Fascia Alta (> 60 min)
            COUNT(CASE WHEN dep_delay > 60 THEN 1 END) AS voli_ritardo_alto,
            ROUND(AVG(CASE WHEN dep_delay > 60 THEN dep_delay END), 2) AS ritardo_medio_dep_alto,
            ROUND(AVG(CASE WHEN dep_delay > 60 THEN arr_delay END), 2) AS ritardo_medio_arr_alto
        FROM flights_data
        GROUP BY origin, month
    ),
    all_causes AS (
        -- Uniamo sia le cause di cancellazione che quelle di ritardo in un unico flusso
        SELECT origin AS aeroporto, month AS mese, cancellation_code AS causa FROM flights_data WHERE cancellation_code IS NOT NULL
        UNION ALL
        SELECT origin AS aeroporto, month AS mese, delay_code AS causa FROM flights_data WHERE delay_code IS NOT NULL
    ),
    counted_causes AS (
        -- Contiamo la frequenza di ciascuna causa per aeroporto e mese
        SELECT aeroporto, mese, causa, COUNT(*) AS frequenza
        FROM all_causes
        GROUP BY aeroporto, mese, causa
    ),
    ranked_causes AS (
        -- Classifichiamo le cause dalla più frequente alla meno frequente
        SELECT aeroporto, mese, causa,
               ROW_NUMBER() OVER (PARTITION BY aeroporto, mese ORDER BY frequenza DESC) as ranking
        FROM counted_causes
    ),
    top_3_causes AS (
        -- Raggruppiamo le prime 3 cause in una stringa separata da virgole (es: "C,W,N")
        SELECT aeroporto, mese, CONCAT_WS(',', COLLECT_LIST(causa)) AS top_3_cause
        FROM ranked_causes
        WHERE ranking <= 3
        GROUP BY aeroporto, mese
    )
    -- Join finale tra le statistiche delle fasce e le top 3 cause
    SELECT 
        f.*,
        COALESCE(c.top_3_cause, 'N/D') AS top_3_cause_ritardo_canc
    FROM stats_fasce f
    LEFT JOIN top_3_causes c ON f.aeroporto = c.aeroporto AND f.mese = c.mese
    ORDER BY f.aeroporto ASC, f.mese ASC
    """

    print("[JOB 2] Esecuzione della query SQL complessa in corso...")
    start_job = time.time()

    risultato_2 = spark.sql(query_2)
    
    # Mostriamo l'anteprima dei risultati a schermo
    risultato_2.show(10, truncate=False)

    end_job = time.time()
    print(f"[JOB 2] Calcolo completato in {end_job - start_job:.2f} secondi.")

    # 4. Salvataggio su HDFS
    print(f"[JOB 2] Salvataggio dei risultati in HDFS: {args.output}")
    risultato_2.coalesce(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(args.output)

    spark.stop()

if __name__ == "__main__":
    main()