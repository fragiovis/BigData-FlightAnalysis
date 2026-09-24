"""
jobs_info.py — Descrizione dei job per la dashboard: cosa calcolano, significato delle colonne di output
e fasi dell'implementazione in ciascuna tecnologia.
"""

JOBS = {
    "job_1": {
        "titolo": "Job 1 — Statistiche delle compagnie aeree (traccia, punto 3.1)",
        "descrizione": (
            "Per **ogni compagnia aerea** e per **ogni aeroporto di partenza** da cui opera, calcola quanti voli "
            "ha effettuato, quanto sono arrivati in ritardo (minimo, massimo e medio), quanti sono stati cancellati "
            "e in quali mesi dell'anno la compagnia vola da quell'aeroporto."
        ),
        "regole": [
            "Una riga per coppia (compagnia, aeroporto), ordinata per compagnia e poi per numero di voli decrescente.",
            "I voli cancellati contano nel numero di voli e nel tasso di cancellazione, ma non hanno ritardo in "
            "arrivo: sono esclusi da minimo, massimo e media.",
            "Un ritardo negativo indica un arrivo in anticipo.",
        ],
        "colonne": [
            ("compagnia", "Codice IATA della compagnia (es. AA, DL, 9E)"),
            ("aeroporto_partenza", "Codice IATA dell'aeroporto di partenza"),
            ("numero_voli", "Voli della compagnia da quell'aeroporto nel periodo del dataset"),
            ("ritardo_min_arrivo", "Ritardo minimo in arrivo, in minuti (negativo = anticipo)"),
            ("ritardo_max_arrivo", "Ritardo massimo in arrivo, in minuti"),
            ("ritardo_medio_arrivo", "Ritardo medio in arrivo, in minuti (2 decimali)"),
            ("tasso_cancellazione", "Voli cancellati / voli totali (4 decimali)"),
            ("mesi_operativi", "Mesi con almeno un volo, in ordine crescente, separati da |"),
        ],
    },
    "job_2": {
        "titolo": "Job 2 — Report dei ritardi per aeroporto e mese (traccia, punto 3.2)",
        "descrizione": (
            "Per **ogni aeroporto di partenza** e per **ogni mese**, divide i voli in tre fasce in base al ritardo "
            "in partenza, calcola per ciascuna fascia il ritardo medio in partenza e in arrivo, e indica le **tre "
            "cause più frequenti** di ritardo o di cancellazione."
        ),
        "regole": [
            "Fasce di ritardo in partenza: **basso** meno di 15 minuti (anticipi compresi), **medio** da 15 a 60 "
            "minuti inclusi, **alto** oltre 60 minuti.",
            "I voli cancellati non hanno ritardo, quindi non rientrano in nessuna fascia; contano però tra le cause, "
            "con il loro codice di cancellazione.",
            "Cause: `CANC_*` per le cancellazioni e `DELAY_*` per la causa principale del ritardo (quella con più "
            "minuti attribuiti). A parità di frequenza vince il codice in ordine alfabetico; senza cause il campo "
            "vale N/D.",
        ],
        "colonne": [
            ("aeroporto", "Codice IATA dell'aeroporto di partenza"),
            ("mese", "Mese (1–12)"),
            ("voli_ritardo_basso", "Voli con ritardo in partenza < 15 minuti"),
            ("ritardo_medio_dep_basso", "Ritardo medio in partenza dei voli in fascia bassa (minuti)"),
            ("ritardo_medio_arr_basso", "Ritardo medio in arrivo dei voli in fascia bassa (minuti)"),
            ("voli_ritardo_medio", "Voli con ritardo in partenza tra 15 e 60 minuti"),
            ("ritardo_medio_dep_medio", "Ritardo medio in partenza dei voli in fascia media"),
            ("ritardo_medio_arr_medio", "Ritardo medio in arrivo dei voli in fascia media"),
            ("voli_ritardo_alto", "Voli con ritardo in partenza > 60 minuti"),
            ("ritardo_medio_dep_alto", "Ritardo medio in partenza dei voli in fascia alta"),
            ("ritardo_medio_arr_alto", "Ritardo medio in arrivo dei voli in fascia alta"),
            ("top_3_cause_ritardo_canc", "Le tre cause più frequenti, dalla più frequente, separate da |"),
        ],
    },
}

CAUSES = [
    ("DELAY_CARRIER", "Ritardo causato dalla compagnia (equipaggio, manutenzione, pulizia)"),
    ("DELAY_WEATHER", "Ritardo per condizioni meteo"),
    ("DELAY_NAS", "Ritardo del sistema di traffico aereo nazionale (congestione, controllo del volo)"),
    ("DELAY_SECURITY", "Ritardo per motivi di sicurezza"),
    ("DELAY_LATE_AIRCRAFT", "Ritardo perché l'aereo è arrivato in ritardo dal volo precedente"),
    ("CANC_CARRIER", "Cancellazione decisa dalla compagnia"),
    ("CANC_WEATHER", "Cancellazione per meteo"),
    ("CANC_NAS", "Cancellazione dovuta al sistema di traffico aereo nazionale"),
    ("CANC_SECURITY", "Cancellazione per motivi di sicurezza"),
]

# Fasi dell'implementazione: (fase, cosa fa, operazioni usate)
STEPS = {
    ("job_1", "spark-core"): [
        ("Lettura", "Legge il CSV da HDFS come righe di testo e usa l'intestazione per trovare le colonne per nome",
         "textFile, first, filter"),
        ("Map", "Trasforma ogni volo in una coppia (compagnia, aeroporto) → accumulatore "
                "(voli, voli con ritardo, min, max, somma ritardi, cancellati, insieme dei mesi)", "map"),
        ("Aggregazione", "Combina gli accumulatori con la stessa chiave, prima in ogni partizione e poi dopo lo "
                         "shuffle; min e max ignorano i voli senza ritardo", "reduceByKey"),
        ("Calcolo finale", "Media = somma / voli con ritardo, tasso = cancellati / voli, mesi ordinati uniti da |", "map"),
        ("Ordinamento", "Per compagnia crescente e numero di voli decrescente", "sortBy"),
        ("Scrittura", "Unisce intestazione e dati in un'unica partizione e salva il CSV su HDFS",
         "union, sortByKey, saveAsTextFile"),
    ],
    ("job_1", "spark-sql"): [
        ("Lettura", "Legge il CSV come DataFrame, deducendo i tipi delle colonne, e lo registra come vista SQL",
         "read.csv (inferSchema), createOrReplaceTempView"),
        ("Query", "Raggruppa per compagnia e aeroporto e calcola le statistiche in una sola SELECT",
         "GROUP BY, COUNT, MIN, MAX, AVG"),
        ("Mesi", "Raccoglie i mesi distinti, li ordina e li unisce con |",
         "COLLECT_SET, SORT_ARRAY, ARRAY_JOIN"),
        ("Ordinamento", "Per compagnia crescente e numero di voli decrescente", "ORDER BY"),
        ("Anteprima e scrittura", "Mostra le prime 10 righe e salva un unico CSV con intestazione",
         "show, coalesce(1), write.csv"),
    ],
    ("job_1", "hive"): [
        ("Tabella", "Definisce una tabella esterna sulla cartella HDFS del dataset (i dati non vengono copiati)",
         "CREATE EXTERNAL TABLE … LOCATION"),
        ("Filtro", "Esclude le righe di intestazione, lette come voli con mese mancante", "WHERE month IS NOT NULL"),
        ("Query", "Stesse statistiche della versione Spark SQL, raggruppate per compagnia e aeroporto",
         "GROUP BY, COUNT, MIN, MAX, AVG"),
        ("Mesi", "Ordina i mesi come stringhe a due cifre e toglie lo zero iniziale (compatibile con Hive 3)",
         "collect_set, lpad, sort_array, concat_ws, regexp_replace"),
        ("Scrittura", "Scrive il risultato direttamente nella cartella di output su HDFS",
         "INSERT OVERWRITE DIRECTORY"),
        ("Esecuzione", "Hive traduce la query in 2 job MapReduce: aggregazione, poi ordinamento globale", "MapReduce"),
    ],
    ("job_2", "spark-core"): [
        ("Lettura", "Legge il CSV come righe di testo e le tiene in cache perché servono a due pipeline",
         "textFile, filter, cache"),
        ("Pipeline fasce", "Ogni volo diventa (aeroporto, mese) → 12 valori: voli, somma ritardi in partenza, "
                           "voli con ritardo in arrivo e somma ritardi in arrivo per ognuna delle 3 fasce",
         "map, reduceByKey"),
        ("Medie", "Calcola per ogni fascia il numero di voli e i ritardi medi", "map"),
        ("Pipeline cause", "Emette una coppia per il codice di cancellazione e una per la causa del ritardo, "
                           "conta le occorrenze e raggruppa le cause di ogni aeroporto e mese",
         "flatMap, reduceByKey, groupByKey"),
        ("Top 3", "Ordina le cause per frequenza (e alfabeticamente a parità) e tiene le prime 3", "map"),
        ("Unione", "Unisce fasce e cause; dove non ci sono cause scrive N/D", "leftOuterJoin"),
        ("Ordinamento e scrittura", "Per aeroporto e mese, poi salva il CSV con intestazione",
         "sortBy, saveAsTextFile"),
    ],
    ("job_2", "spark-sql"): [
        ("Lettura", "Legge il CSV come DataFrame e lo registra come vista SQL",
         "read.csv (inferSchema), createOrReplaceTempView"),
        ("Fasce", "Conta i voli e calcola le medie per fascia con espressioni condizionali",
         "CTE stats_fasce: CASE WHEN, COUNT, AVG"),
        ("Cause", "Mette in un'unica colonna i codici di cancellazione e le cause di ritardo",
         "CTE all_causes: UNION ALL"),
        ("Conteggio", "Conta ogni causa per aeroporto e mese", "CTE counted_causes: GROUP BY, COUNT"),
        ("Classifica", "Numera le cause dalla più frequente, con spareggio alfabetico",
         "CTE ranked_causes: ROW_NUMBER() OVER"),
        ("Top 3", "Ricompone le prime 3 in ordine con un pivot per posizione",
         "CTE top_3_causes: MAX(CASE WHEN ranking = k), CONCAT_WS"),
        ("Unione e ordinamento", "Unisce fasce e cause (N/D se mancano) e ordina per aeroporto e mese",
         "LEFT JOIN, COALESCE, ORDER BY"),
    ],
    ("job_2", "hive"): [
        ("Tabella", "Tabella esterna sulla cartella HDFS del dataset", "CREATE EXTERNAL TABLE … LOCATION"),
        ("Fasce", "Come in Spark SQL, escludendo le righe di intestazione",
         "CASE WHEN, COUNT, AVG, WHERE month IS NOT NULL"),
        ("Cause e classifica", "Unione dei codici, conteggio, classifica e pivot delle prime 3",
         "UNION ALL, ROW_NUMBER() OVER, MAX(CASE WHEN …)"),
        ("Unione e scrittura", "Join con le fasce e scrittura nella cartella di output",
         "LEFT OUTER JOIN, INSERT OVERWRITE DIRECTORY"),
        ("Esecuzione", "Hive esegue il piano come 6 job MapReduce in sequenza, salvando su HDFS ogni risultato "
                       "intermedio", "MapReduce"),
    ],
}
