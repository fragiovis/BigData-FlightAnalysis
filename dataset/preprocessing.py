#!/usr/bin/env python3
"""
preprocessing.py — Pulizia, validazione e trasformazione del dataset dei voli.

Fasi:
  1. Normalizzazione: tipi numerici, codici in maiuscolo e senza spazi.
  2. Validazione: a ogni riga viene assegnato il primo controllo non superato; le righe che
     falliscono un controllo vengono escluse e contate per motivo (un solo passaggio sui dati).
  3. Rimozione dei duplicati sulla chiave del volo (data, compagnia, numero, tratta, orario).
  4. Trasformazioni: ritardi dei voli cancellati, causa prevalente del ritardo, codici espliciti.
  5. Selezione delle 9 colonne usate dai job e scrittura di flights_cleaned.csv.

Il riepilogo di ogni fase (righe escluse o modificate, valori mancanti, statistiche) viene
salvato in results/qualita_dati.json ed è visibile nella dashboard.
"""

import json
import os
import shutil
from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# Recuperiamo la radice del progetto risalendo di un livello rispetto a dataset/
ROOT_DIR = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
RAW_PATH = os.path.join(ROOT_DIR, "data", "raw", "flight_data_2024.csv")
OUTPUT_DIR = os.path.join(ROOT_DIR, "data", "processed")
REPORT_PATH = os.path.join(ROOT_DIR, "results", "qualita_dati.json")

FLIGHT_KEY = ["fl_date", "op_unique_carrier", "op_carrier_fl_num", "origin", "dest", "crs_dep_time"]
CAUSE_COLS = ["carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"]
FINAL_COLUMNS = ["month", "op_unique_carrier", "origin", "dest", "dep_delay", "arr_delay",
                 "cancelled", "cancellation_code", "delay_code"]
LONG_DELAY_MIN = 24 * 60      # soglia per segnalare i ritardi eccezionali (non vengono esclusi)
SMALL_AIRPORT_FLIGHTS = 50    # soglia per segnalare gli aeroporti con pochissimi voli

spark = SparkSession.builder.appName("flight-preprocessing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print(f"[PYSPARK] Caricamento di: {RAW_PATH}")
raw = spark.read.option("header", True).option("inferSchema", True).csv(f"file://{RAW_PATH}")
raw_columns = len(raw.columns)

# --- 1. NORMALIZZAZIONE -----------------------------------------------------------------------
df = raw.select(
    "fl_date", "op_carrier_fl_num", "crs_dep_time",
    F.col("month").cast(IntegerType()).alias("month"),
    *[F.upper(F.trim(F.col(c))).alias(c) for c in ["op_unique_carrier", "origin", "dest", "cancellation_code"]],
    *[F.col(c).cast(DoubleType()).alias(c) for c in ["dep_delay", "arr_delay"]],
    *[F.col(c).cast(IntegerType()).alias(c) for c in ["cancelled", "diverted"]],
    # Minuti per causa: assenti quando il volo non ha ritardi attribuiti, quindi valgono 0
    *[F.coalesce(F.col(c).cast(DoubleType()), F.lit(0.0)).alias(c) for c in CAUSE_COLS],
)
cause_sum = sum(F.col(c) for c in CAUSE_COLS)
airport = r"^[A-Z]{3}$"

# --- 2. VALIDAZIONE ---------------------------------------------------------------------------
# (id, descrizione, condizione di esclusione) — conta solo il primo controllo non superato
CHECKS = [
    ("dirottato",
     "Volo dirottato: atterra in un aeroporto diverso da quello previsto, quindi ritardo in arrivo e "
     "cancellazione non sono confrontabili con gli altri voli",
     F.col("diverted") == 1),
    ("campi_chiave_mancanti",
     "Compagnia, aeroporto di partenza o di arrivo, mese o flag di cancellazione mancanti",
     F.col("op_unique_carrier").isNull() | F.col("origin").isNull() | F.col("dest").isNull()
     | F.col("month").isNull() | F.col("cancelled").isNull()),
    ("mese_non_valido", "Mese fuori dall'intervallo 1–12", ~F.col("month").between(1, 12)),
    ("codice_aeroporto_non_valido", "Codice aeroporto diverso da tre lettere maiuscole (IATA)",
     ~F.col("origin").rlike(airport) | ~F.col("dest").rlike(airport)),
    ("cancellazione_incoerente",
     "Flag di cancellazione diverso da 0/1, oppure incoerente con la presenza del codice di cancellazione",
     ~F.col("cancelled").isin(0, 1)
     | ((F.col("cancelled") == 1) & F.col("cancellation_code").isNull())
     | ((F.col("cancelled") == 0) & F.col("cancellation_code").isNotNull())),
    ("ritardi_mancanti", "Volo non cancellato senza ritardo in partenza o in arrivo",
     (F.col("cancelled") == 0) & (F.col("dep_delay").isNull() | F.col("arr_delay").isNull())),
    ("cause_incoerenti",
     "Minuti attribuiti alle cause di ritardo la cui somma differisce dal ritardo in arrivo di oltre 1 minuto",
     (cause_sum > 0) & (F.abs(cause_sum - F.col("arr_delay")) > 1)),
]

reason = F.lit(None).cast("string")
for check_id, _, condition in reversed(CHECKS):
    reason = F.when(F.coalesce(condition, F.lit(False)), F.lit(check_id)).otherwise(reason)

df = df.withColumn("motivo_esclusione", reason).persist(StorageLevel.MEMORY_AND_DISK)

print("[PYSPARK] Validazione delle righe...")
by_reason = {r["motivo_esclusione"]: r["count"] for r in df.groupBy("motivo_esclusione").count().collect()}
initial_rows = sum(by_reason.values())
valid = df.filter(F.col("motivo_esclusione").isNull()).drop("motivo_esclusione")
valid_rows = by_reason.get(None, 0)

# --- 3. DUPLICATI -----------------------------------------------------------------------------
print("[PYSPARK] Rimozione dei duplicati...")
dedup = valid.dropDuplicates(FLIGHT_KEY).persist(StorageLevel.MEMORY_AND_DISK)
dedup_rows = dedup.count()
df.unpersist()

# --- 4. TRASFORMAZIONI ------------------------------------------------------------------------
cancelled = F.col("cancelled") == 1
transform_counts = dedup.agg(
    F.sum(F.when(cancelled & (F.col("dep_delay").isNotNull() | F.col("arr_delay").isNotNull()), 1).otherwise(0))
     .alias("ritardo_cancellati"),
    F.sum(F.when(cause_sum > 0, 1).otherwise(0)).alias("con_causa"),
).first()

# Un volo cancellato non è un volo in ritardo: alcuni hanno un ritardo in partenza perché hanno
# lasciato il gate prima della cancellazione. Lo azzeriamo, così non entrano nelle fasce del Job 2
transformed = (dedup
    .withColumn("dep_delay", F.when(cancelled, None).otherwise(F.col("dep_delay")))
    .withColumn("arr_delay", F.when(cancelled, None).otherwise(F.col("arr_delay"))))

# Causa prevalente del ritardo. I codici hanno il prefisso DELAY_ per non collidere con quelli di
# cancellazione (es. "C" = Carrier nei ritardi ma National Air System nelle cancellazioni)
max_delay = F.greatest(*[F.col(c) for c in CAUSE_COLS])
transformed = transformed.withColumn("delay_code",
    F.when(max_delay == 0.0, None)
    .when(F.col("carrier_delay") == max_delay, "DELAY_CARRIER")
    .when(F.col("weather_delay") == max_delay, "DELAY_WEATHER")
    .when(F.col("nas_delay") == max_delay, "DELAY_NAS")
    .when(F.col("security_delay") == max_delay, "DELAY_SECURITY")
    .otherwise("DELAY_LATE_AIRCRAFT")
)

# Codici di cancellazione BTS (A-D) tradotti in etichette esplicite con prefisso CANC_
transformed = transformed.withColumn("cancellation_code",
    F.when(F.col("cancellation_code") == "A", "CANC_CARRIER")
    .when(F.col("cancellation_code") == "B", "CANC_WEATHER")
    .when(F.col("cancellation_code") == "C", "CANC_NAS")
    .when(F.col("cancellation_code") == "D", "CANC_SECURITY")
    .otherwise(None)
)

final = transformed.select(FINAL_COLUMNS)

# --- STATISTICHE DEL DATASET FINALE -----------------------------------------------------------
print("[PYSPARK] Calcolo delle statistiche...")
stats = final.agg(
    *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in FINAL_COLUMNS],
    F.sum(cancelled.cast("int")).alias("cancellati"),
    F.sum((F.col("dep_delay") > LONG_DELAY_MIN).cast("int")).alias("ritardo_partenza_oltre_24h"),
    F.sum((F.col("arr_delay") > LONG_DELAY_MIN).cast("int")).alias("ritardo_arrivo_oltre_24h"),
    F.min("dep_delay").alias("dep_delay_min"), F.max("dep_delay").alias("dep_delay_max"),
    F.min("arr_delay").alias("arr_delay_min"), F.max("arr_delay").alias("arr_delay_max"),
    F.countDistinct("op_unique_carrier").alias("compagnie"),
    F.countDistinct("origin").alias("aeroporti_partenza"),
).first().asDict()
small = (final.groupBy("origin").count().filter(F.col("count") < SMALL_AIRPORT_FLIGHTS)
         .agg(F.count("*").alias("aeroporti"), F.sum("count").alias("voli")).first())

# --- 5. SCRITTURA -----------------------------------------------------------------------------
output_temp_dir = os.path.join(OUTPUT_DIR, "temp_out")
print("[PYSPARK] Scrittura del file pulito in corso...")
final.coalesce(1).write.option("header", True).mode("overwrite").csv(f"file://{output_temp_dir}")
for file in os.listdir(output_temp_dir):
    if file.endswith(".csv"):
        os.replace(os.path.join(output_temp_dir, file), os.path.join(OUTPUT_DIR, "flights_cleaned.csv"))
shutil.rmtree(output_temp_dir)
dedup.unpersist()

# --- RIEPILOGO --------------------------------------------------------------------------------
steps = [{"id": cid, "tipo": "esclusione", "descrizione": desc, "righe": by_reason.get(cid, 0)}
         for cid, desc, _ in CHECKS]
steps.append({"id": "duplicati", "tipo": "esclusione",
              "descrizione": "Stesso volo ripetuto (data, compagnia, numero di volo, tratta e orario programmato)",
              "righe": valid_rows - dedup_rows})
steps.append({"id": "ritardo_cancellati", "tipo": "trasformazione",
              "descrizione": "Voli cancellati con un ritardo registrato (hanno lasciato il gate prima della "
                             "cancellazione): ritardi azzerati, così non entrano nelle fasce di ritardo",
              "righe": transform_counts["ritardo_cancellati"]})
steps.append({"id": "causa_prevalente", "tipo": "trasformazione",
              "descrizione": "Voli con minuti attribuiti alle cause di ritardo: delay_code = causa con più minuti",
              "righe": transform_counts["con_causa"]})

report = {
    "generato": datetime.now().isoformat(timespec="seconds"),
    "sorgente": os.path.relpath(RAW_PATH, ROOT_DIR),
    "colonne_iniziali": raw_columns,
    "colonne_finali": FINAL_COLUMNS,
    "righe_iniziali": initial_rows,
    "righe_finali": dedup_rows,
    "passi": steps,
    "valori_mancanti": {c: stats.pop(f"null_{c}") for c in FINAL_COLUMNS},
    "statistiche": {**stats, "aeroporti_sotto_soglia": small["aeroporti"], "voli_aeroporti_sotto_soglia": small["voli"] or 0,
                    "soglia_aeroporti_voli": SMALL_AIRPORT_FLIGHTS},
    "scelte": [
        "I ritardi superiori a 24 ore vengono mantenuti: sono voli riprogrammati reali e la traccia chiede il "
        "ritardo massimo.",
        f"Gli aeroporti con meno di {SMALL_AIRPORT_FLIGHTS} voli nell'anno vengono mantenuti: non alterano le "
        "statistiche per compagnia o per mese.",
        "Per ogni volo si conta una sola causa di ritardo, quella con più minuti attribuiti.",
        "I voli cancellati non hanno ritardo: contano nel tasso di cancellazione e nelle cause (tramite il codice "
        "di cancellazione), non nelle fasce di ritardo né nelle medie.",
    ],
}
os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
with open(REPORT_PATH, "w") as f:
    json.dump(report, f, indent=1, ensure_ascii=False)

print(f"\n[RIEPILOGO] Righe iniziali: {initial_rows:,}")
for s in steps:
    segno = "-" if s["tipo"] == "esclusione" else "~"
    print(f"   {segno} {s['id']:<30} {s['righe']:>10,}")
print(f"[RIEPILOGO] Righe finali:   {dedup_rows:,}")
print("[OK] Preprocessing completato: data/processed/flights_cleaned.csv (riepilogo in results/qualita_dati.json)")
spark.stop()
