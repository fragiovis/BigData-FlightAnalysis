-- 1. Pulizia e Creazione della Tabella Esterna mappata sulla cartella di staging HDFS
DROP TABLE IF EXISTS flights_input;

CREATE EXTERNAL TABLE flights_input (
    month INT,
    op_unique_carrier STRING,
    origin STRING,
    dest STRING,
    dep_delay FLOAT,
    arr_delay FLOAT,
    cancelled INT,
    cancellation_code STRING,
    delay_code STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${staging_path}'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 2. Elaborazione immediata e scrittura dei risultati direttamente nella directory HDFS di output
INSERT OVERWRITE DIRECTORY '${output_path}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    op_unique_carrier,
    origin,
    COUNT(*) as numero_voli,
    MIN(arr_delay) as ritardo_min_arrivo,
    MAX(arr_delay) as ritardo_max_arrivo,
    ROUND(AVG(arr_delay), 2) as ritardo_medio_arrivo,
    ROUND(SUM(cancelled) / COUNT(*), 4) as tasso_cancellazione,
    concat_ws(',', collect_set(cast(month as string))) as mesi_operativi
FROM flights_input
GROUP BY op_unique_carrier, origin
ORDER BY op_unique_carrier ASC, numero_voli DESC;

-- 3. Scollegamento finale del catalogo (Essendo una tabella EXTERNAL, i dati originari non vengono toccati)
DROP TABLE flights_input;