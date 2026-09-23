#!/usr/bin/env python3
"""
job_2.py — Report mensile ritardi e top 3 cause per aeroporto.
Tecnologia: Spark Core (RDD puri con doppia pipeline e Left Outer Join)
"""

import argparse
import time
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-input", type=str, help="Path to input file")
    parser.add_argument("-output", type=str, help="Path to output folder")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("spark-core#job-2") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    print(f"[JOB 2 CORE] Caricamento del dataset da HDFS: {args.input}")
    raw_rdd = sc.textFile(args.input)

    # 1. Estrazione dinamica dell'header e filtraggio
    header_line = raw_rdd.first()
    header_fields = header_line.split(",")
    
    idx_origin = header_fields.index("origin")
    idx_month = header_fields.index("month")
    idx_dep_delay = header_fields.index("dep_delay")
    idx_arr_delay = header_fields.index("arr_delay")
    idx_canc_code = header_fields.index("cancellation_code")
    idx_delay_code = header_fields.index("delay_code")

    data_rdd = raw_rdd.filter(lambda line: line != header_line) # Filtra header
    
    # Cache del dataset di dati pulito perché verrà letto da due pipeline diverse
    data_rdd.cache()

    print("[JOB 2 CORE] Elaborazione Fase 1: Calcolo statistiche fasce di ritardo...")
    start_job = time.time()

    # --- PIPELINE 1: STATISTICHE FASCE ---
    # Struttura del valore accumulato per la riduzione:
    # (basso_cnt, basso_dep_sum, basso_arr_sum, medio_cnt, medio_dep_sum, medio_arr_sum, alto_cnt, alto_dep_sum, alto_arr_sum)
    def map_fasce(line):
        fields = line.split(",")
        aeroporto = fields[idx_origin]
        mese = int(fields[idx_month])
        
        try:
            dep_delay = float(fields[idx_dep_delay])
        except (ValueError, TypeError):
            dep_delay = None
            
        try:
            arr_delay = float(fields[idx_arr_delay])
        except (ValueError, TypeError):
            arr_delay = 0.0

        # Inizializziamo i contatori a zero
        b_cnt, b_dep, b_arr = 0, 0.0, 0.0
        m_cnt, m_dep, m_arr = 0, 0.0, 0.0
        a_cnt, a_dep, a_arr = 0, 0.0, 0.0

        if dep_delay is not None:
            if dep_delay < 15:
                b_cnt, b_dep, b_arr = 1, dep_delay, arr_delay
            elif 15 <= dep_delay <= 60:
                m_cnt, m_dep, m_arr = 1, dep_delay, arr_delay
            else:
                a_cnt, a_dep, a_arr = 1, dep_delay, arr_delay

        return ((aeroporto, mese), (b_cnt, b_dep, b_arr, m_cnt, m_dep, m_arr, a_cnt, a_dep, a_arr))

    def reduce_fasce(v1, v2):
        return (
            v1[0]+v2[0], v1[1]+v2[1], v1[2]+v2[2],  # Basso
            v1[3]+v2[3], v1[4]+v2[4], v1[5]+v2[5],  # Medio
            v1[6]+v2[6], v1[7]+v2[7], v1[8]+v2[8]   # Alto
        )

    rdd_fasce_raw = data_rdd.map(map_fasce).reduceByKey(reduce_fasce)

    # Calcolo finale delle medie per ciascuna fascia
    def calcola_medie(x):
        k, v = x
        avg_dep_b = round(v[1]/v[0], 2) if v[0] > 0 else ""
        avg_arr_b = round(v[2]/v[0], 2) if v[0] > 0 else ""
        avg_dep_m = round(v[4]/v[3], 2) if v[3] > 0 else ""
        avg_arr_m = round(v[5]/v[3], 2) if v[3] > 0 else ""
        avg_dep_a = round(v[7]/v[6], 2) if v[6] > 0 else ""
        avg_arr_a = round(v[8]/v[6], 2) if v[6] > 0 else ""
        
        return (k, (v[0], avg_dep_b, avg_arr_b, v[3], avg_dep_m, avg_arr_m, v[6], avg_dep_a, avg_arr_a))

    rdd_fasce = rdd_fasce_raw.map(calcola_medie)

    print("[JOB 2 CORE] Elaborazione Fase 2: Calcolo classifica TOP 3 cause...")
    
    # --- PIPELINE 2: TOP 3 CAUSE ---
    # Estraiamo tutte le cause valide (sia cancellazione che ritardo)
    def map_cause(line):
        fields = line.split(",")
        aeroporto = fields[idx_origin]
        mese = int(fields[idx_month])
        canc = fields[idx_canc_code].strip()
        delay = fields[idx_delay_code].strip()
        
        out = []
        if canc and canc != "" and canc != "None":
            out.append(((aeroporto, mese, canc), 1))
        if delay and delay != "" and delay != "None":
            out.append(((aeroporto, mese, delay), 1))
        return out

    # Appiattiamo la lista, contiamo le occorrenze di ogni codice, e raggruppiamo per (aeroporto, mese)
    rdd_cause_counted = data_rdd.flatMap(map_cause).reduceByKey(lambda a, b: a + b)
    
    # Trasformiamo in: Key=(aeroporto, mese), Value=(codice, frequenza)
    rdd_cause_grouped = rdd_cause_counted.map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1]))).groupByKey()

    # Per ogni gruppo, ordiniamo i codici per frequenza decrescente e prendiamo i primi 3
    def estrai_top_3(x):
        key, valori = x
        lista_ordinata = sorted(list(valori), key=lambda v: v[1], reverse=True)
        top_3 = [v[0] for v in lista_ordinata[:3]]
        return (key, ",".join(top_3))

    rdd_top_3_cause = rdd_cause_grouped.map(estrai_top_3)

    print("[JOB 2 CORE] FUSIONE delle pipeline tramite Left Outer Join...")
    
    # --- FASE 3: JOIN FINALE E ORDINAMENTO ---
    # Uniamo le statistiche delle fasce con le top 3 cause (se presenti, altrimenti metti N/D)
    rdd_finale = rdd_fasce.leftOuterJoin(rdd_top_3_cause) \
        .map(lambda x: (
            x[0][0], # aeroporto
            x[0][1], # mese
            x[1][0][0], x[1][0][1], x[1][0][2], # info fascia bassa
            x[1][0][3], x[1][0][4], x[1][0][5], # info fascia media
            x[1][0][6], x[1][0][7], x[1][0][8], # info fascia alta
            x[1][1] if x[1][1] is not None else "N/D" # top 3 cause
        )) \
        .sortBy(lambda x: (x[0], x[1])) # Ordina per aeroporto e mese

    print("\n--- ANTEPRIMA RISULTATI SPARK CORE JOB 2 (TOP 5) ---")
    for line in rdd_finale.take(5):
        print(line)

    end_job = time.time()
    print(f"[JOB 2 CORE] Calcolo completato in {end_job - start_job:.2f} secondi.")

    # --- FASE 4: SALVATAGGIO CON HEADER GARANTITO ---
    print(f"[JOB 2 CORE] Salvataggio dei risultati in HDFS: {args.output}")
    
    header_str = "aeroporto,mese,voli_ritardo_basso,ritardo_medio_dep_basso,ritardo_medio_arr_basso,voli_ritardo_medio,ritardo_medio_dep_medio,ritardo_medio_arr_medio,voli_ritardo_alto,ritardo_medio_dep_alto,ritardo_medio_arr_alto,top_3_cause_ritardo_canc"
    header_rdd = sc.parallelize([(0, header_str)])
    
    data_rdd_mapped = rdd_finale.map(lambda x: (1, ",".join(map(str, x))))
    
    sc.union([header_rdd, data_rdd_mapped]) \
        .sortByKey(ascending=True, numPartitions=1) \
        .map(lambda x: x[1]) \
        .saveAsTextFile(args.output)

    data_rdd.unpersist()
    spark.stop()

if __name__ == "__main__":
    main()