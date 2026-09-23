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

    # Le due pipeline leggono entrambe data_rdd, ma l'RDD NON viene messo in cache: con la memoria
    # predefinita (1 GB) la cache dell'input più grande (replica 10x, 2 GB) esaurisce lo heap
    # (OutOfMemoryError), mentre rileggere i dati da HDFS costa poco e scala con l'input

    print("[JOB 2 CORE] Elaborazione Fase 1: Calcolo statistiche fasce di ritardo...")
    start_job = time.time()

    # --- PIPELINE 1: STATISTICHE FASCE ---
    # Per ogni fascia (basso, medio, alto) accumuliamo 4 valori:
    # (voli, somma_dep_delay, voli_con_arr_delay, somma_arr_delay)
    # Il conteggio separato degli arr_delay validi serve a escludere i valori mancanti
    # dalla media in arrivo, come fa AVG in Spark SQL e Hive.
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
            arr_delay = None

        fasce = [(0, 0.0, 0, 0.0)] * 3
        if dep_delay is not None:
            if dep_delay < 15:
                fascia = 0
            elif dep_delay <= 60:
                fascia = 1
            else:
                fascia = 2
            fasce[fascia] = (1, dep_delay, 0 if arr_delay is None else 1, arr_delay or 0.0)

        return ((aeroporto, mese), tuple(v for fascia in fasce for v in fascia))

    def reduce_fasce(v1, v2):
        return tuple(a + b for a, b in zip(v1, v2))

    rdd_fasce_raw = data_rdd.map(map_fasce).reduceByKey(reduce_fasce)

    # Calcolo finale delle medie per ciascuna fascia
    def calcola_medie(x):
        k, v = x
        out = []
        for i in (0, 4, 8):
            voli, dep_sum, arr_cnt, arr_sum = v[i:i + 4]
            out += [
                voli,
                round(dep_sum / voli, 2) if voli > 0 else None,
                round(arr_sum / arr_cnt, 2) if arr_cnt > 0 else None,
            ]
        return (k, tuple(out))

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

    # Per ogni gruppo, ordiniamo i codici per frequenza decrescente e prendiamo i primi 3.
    # A parità di frequenza vince il codice alfabeticamente minore, così il risultato è
    # deterministico e identico a quello di Spark SQL e Hive
    def estrai_top_3(x):
        key, valori = x
        lista_ordinata = sorted(valori, key=lambda v: (-v[1], v[0]))
        top_3 = [v[0] for v in lista_ordinata[:3]]
        return (key, "|".join(top_3))

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

    print("\n--- ANTEPRIMA RISULTATI SPARK CORE JOB 2 (TOP 10) ---")
    for line in rdd_finale.take(10):
        print(line)

    end_job = time.time()
    print(f"[JOB 2 CORE] Calcolo completato in {end_job - start_job:.2f} secondi.")

    # --- FASE 4: SALVATAGGIO CON HEADER GARANTITO ---
    print(f"[JOB 2 CORE] Salvataggio dei risultati in HDFS: {args.output}")
    
    header_str = "aeroporto,mese,voli_ritardo_basso,ritardo_medio_dep_basso,ritardo_medio_arr_basso,voli_ritardo_medio,ritardo_medio_dep_medio,ritardo_medio_arr_medio,voli_ritardo_alto,ritardo_medio_dep_alto,ritardo_medio_arr_alto,top_3_cause_ritardo_canc"
    header_rdd = sc.parallelize([(0, header_str)])
    
    # I valori mancanti (None) diventano campi vuoti, come nel CSV scritto da Spark SQL e Hive
    data_rdd_mapped = rdd_finale.map(lambda x: (1, ",".join("" if v is None else str(v) for v in x)))
    
    sc.union([header_rdd, data_rdd_mapped]) \
        .sortByKey(ascending=True, numPartitions=1) \
        .map(lambda x: x[1]) \
        .saveAsTextFile(args.output)

    spark.stop()

if __name__ == "__main__":
    main()