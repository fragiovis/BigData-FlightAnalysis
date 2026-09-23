#!/usr/bin/env python3
"""
job_1.py — Analisi Statistiche delle compagnie aeree.
Tecnologia: Spark Core (RDD puro con gestione dinamica dei flussi)
"""

import argparse
import time
from pyspark.sql import SparkSession

def main():
    # 1. Parsing degli argomenti in linea con lo stile del run.sh
    parser = argparse.ArgumentParser()
    parser.add_argument("-input", type=str, help="Path to input file")
    parser.add_argument("-output", type=str, help="Path to output folder")
    args = parser.parse_args()

    # 2. Inizializzazione della SparkSession per estrarre lo SparkContext
    spark = SparkSession.builder \
        .appName("spark-core#job-1") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    print(f"[JOB 1 CORE] Lettura del file di testo da HDFS: {args.input}")
    raw_rdd = sc.textFile(args.input)

    # 3. Estrazione dinamica dell'header per evitare indici hardcoded
    header_line = raw_rdd.first()
    header_fields = header_line.split(",")
    
    # Troviamo la posizione di ogni colonna utile
    idx_carrier = header_fields.index("op_unique_carrier")
    idx_origin = header_fields.index("origin")
    idx_arr_delay = header_fields.index("arr_delay")
    idx_cancelled = header_fields.index("cancelled")
    idx_month = header_fields.index("month")

    # Filtriamo l'header dal dataset per tenere solo le righe di dati
    data_rdd = raw_rdd.filter(lambda line: line != header_line)

    # 4. Fase di MAP: Trasformiamo ogni riga in una struttura Chiave-Valore
    # Chiave: (compagnia, aeroporto_partenza)
    # Valore: (conteggio_volo, valid_delay_count, min_delay, max_delay, sum_delay, sum_cancelled, set(mese))
    def map_flight_data(line):
        fields = line.split(",")
        
        carrier = fields[idx_carrier]
        origin = fields[idx_origin]
        month = int(fields[idx_month])
        cancelled = int(float(fields[idx_cancelled]))
        
        # Gestione sicura del ritardo in arrivo (se il volo è cancellato, il dato potrebbe essere vuoto)
        try:
            arr_delay = float(fields[idx_arr_delay])
            valid_delay = 1
        except (ValueError, TypeError):
            arr_delay = 0.0
            valid_delay = 0

        return (
            (carrier, origin), 
            (1, valid_delay, arr_delay, arr_delay, arr_delay, cancelled, {month})
        )

    # 5. Fase di REDUCE: Aggreghiamo i valori per la stessa combinazione di Chiave
    def reduce_flight_stats(v1, v2):
        return (
            v1[0] + v2[0],                     # Numero voli totali
            v1[1] + v2[1],                     # Conteggio voli con ritardo valido
            min(v1[2], v2[2]) if v1[1]>0 and v2[1]>0 else (v1[2] if v1[1]>0 else v2[2]), # Min ritardo
            max(v1[3], v2[3]),                 # Max ritardo
            v1[4] + v2[4],                     # Somma ritardi (per media)
            v1[5] + v2[5],                     # Somma voli cancellati
            v1[6] | v2[6]                      # Unione dei set dei mesi operativi
        )

    print("[JOB 1 CORE] Elaborazione della pipeline RDD (Map-Reduce cumulativo)...")
    start_job = time.time()

    processed_rdd = data_rdd \
        .map(map_flight_data) \
        .reduceByKey(reduce_flight_stats) \
        .map(lambda x: (
            x[0][0],                                                       # compagnia
            x[0][1],                                                       # aeroporto_partenza
            x[1][0],                                                       # numero_voli
            x[1][2],                                                       # ritardo_min_arrivo
            x[1][3],                                                       # ritardo_max_arrivo
            round(x[1][4] / max(1, x[1][1]), 2),                           # ritardo_medio_arrivo
            round(x[1][5] / x[1][0], 4),                                   # tasso_cancellazione
            ",".join(map(str, sorted(list(x[1][6]))))                      # mesi_operativi
        )) \
        .sortBy(lambda x: (x[0], -x[2])) # Ordina per compagnia (ASC) e numero voli (DESC)

    # Stampiamo l'anteprima a schermo per verifica visiva
    print("\n--- ANTEPRIMA RISULTATI SPARK CORE (TOP 10) ---")
    for line in processed_rdd.take(10):
        print(line)
    
    end_job = time.time()
    print(f"[JOB 1 CORE] Calcolo completato in {end_job - start_job:.2f} secondi.")

   # 6. Salvataggio su HDFS con Header garantito in prima posizione
    print(f"[JOB 1 CORE] Salvataggio dei risultati in HDFS: {args.output}")
    
    # Creiamo l'header con un indice di ordinamento pari a 0 (la cima)
    header_str = "compagnia,aeroporto_partenza,numero_voli,ritardo_min_arrivo,ritardo_max_arrivo,ritardo_medio_arrivo,tasso_cancellazione,mesi_operativi"
    header_rdd = sc.parallelize([(0, header_str)])
    
    # Assocviamo ai dati un indice di ordinamento pari a 1
    data_rdd_mapped = processed_rdd.map(lambda x: (1, ",".join(map(str, x))))
    
    # Uniamo, ordiniamo per la chiave (0 o 1) in un unico part, e rimuoviamo l'indice temporaneo
    sc.union([header_rdd, data_rdd_mapped]) \
        .sortByKey(ascending=True, numPartitions=1) \
        .map(lambda x: x[1]) \
        .saveAsTextFile(args.output)

    spark.stop()

if __name__ == "__main__":
    main()