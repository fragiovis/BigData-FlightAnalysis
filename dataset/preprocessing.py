#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, greatest
from pyspark.sql.types import DoubleType
import shutil

# Recuperiamo la radice del progetto Flight/ risalendo di un livello rispetto a dataset/
ROOT_DIR = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))

spark = SparkSession.builder.appName("flight-preprocessing").getOrCreate()

# Definiamo direttamente il percorso del file principale senza fare ricerche dinamiche
raw_path = os.path.join(ROOT_DIR, "data", "raw", "flight_data_2024.csv")
print(f"[PYSPARK] Caricamento diretto di: {raw_path}")

# Lettura del dataset dei voli
df = spark.read.option("header", True).option("inferSchema", True).csv(f"file://{raw_path}")

# --- FASE DI PULIZIA ---
print("[PYSPARK] Filtraggio righe e calcolo delay_code...")
df_filtered = df.filter(col("diverted") == 0)

delay_cols = ["carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"]
for c in delay_cols:
    df_filtered = df_filtered.withColumn(c, when(col(c).isNull(), 0.0).otherwise(col(c).cast(DoubleType())))

max_delay = greatest(*[col(c) for c in delay_cols])

df_with_code = df_filtered.withColumn("delay_code",
    when(max_delay == 0.0, None)
    .when(col("carrier_delay") == max_delay, "C")
    .when(col("weather_delay") == max_delay, "W")
    .when(col("nas_delay") == max_delay, "N")
    .when(col("security_delay") == max_delay, "S")
    .otherwise("L")
)

final_columns = ["month", "op_unique_carrier", "origin", "dest", "dep_delay", "arr_delay", "cancelled", "cancellation_code", "delay_code"]
df_final = df_with_code.select(final_columns)

# --- SCRITTURA NELLA CARTELLA PROCESSED ---
output_temp_dir = os.path.join(ROOT_DIR, "data", "processed", "temp_out")

print("[PYSPARK] Scrittura del file pulito in corso...")
df_final.coalesce(1).write.option("header", True).mode("overwrite").csv(f"file://{output_temp_dir}")

# Spostiamo il file part-*.csv generato da Spark rinominandolo in flights_cleaned.csv
for file in os.listdir(output_temp_dir):
    if file.endswith(".csv"):
        shutil_source = os.path.join(output_temp_dir, file)
        shutil_dest = os.path.join(ROOT_DIR, "data", "processed", "flights_cleaned.csv")
        os.replace(shutil_source, shutil_dest)

# Eliminiamo la cartella temporanea di Spark
shutil.rmtree(output_temp_dir)

print("[OK] Preprocessing completato! Il file si trova in data/processed/flights_cleaned.csv")
spark.stop()