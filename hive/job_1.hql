-- 1. Pulizia e Creazione della Tabella Esterna mappata sulla cartella di staging HDFS
DROP TABLE IF EXISTS flights_input;

CREATE EXTERNAL TABLE flights_input (
    month INT,
    op_unique_carrier STRING,
    origin STRING,
    dest STRING,
    dep_delay DOUBLE,
    arr_delay DOUBLE,
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
-- NULL DEFINED AS '' scrive i valori mancanti come campi vuoti (come Spark) invece di \N
INSERT OVERWRITE DIRECTORY '${output_path}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
NULL DEFINED AS ''
SELECT
    op_unique_carrier,
    origin,
    COUNT(*) as numero_voli,
    MIN(arr_delay) as ritardo_min_arrivo,
    MAX(arr_delay) as ritardo_max_arrivo,
    ROUND(AVG(arr_delay), 2) as ritardo_medio_arrivo,
    ROUND(SUM(cancelled) / COUNT(*), 4) as tasso_cancellazione,
    -- Mesi ordinati numericamente: ordinamento su stringhe a 2 cifre ("01".."12") e rimozione
    -- dello zero iniziale. Evita array_join, disponibile solo da Hive 4 (EMR usa Hive 3.1)
    regexp_replace(concat_ws('|', sort_array(collect_set(lpad(cast(month AS STRING), 2, '0')))), '(^|\\|)0', '$1') as mesi_operativi
FROM flights_input
-- Hive 4 non applica skip.header.line.count in questa configurazione: l'header del CSV
-- verrebbe letto come un volo (con month = NULL), quindi lo escludiamo esplicitamente
WHERE month IS NOT NULL
GROUP BY op_unique_carrier, origin
ORDER BY op_unique_carrier ASC, numero_voli DESC;

-- 3. Scollegamento finale del catalogo (Essendo una tabella EXTERNAL, i dati originari non vengono toccati)
DROP TABLE flights_input;