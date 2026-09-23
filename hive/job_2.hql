-- 1. Pulizia e Creazione della Tabella Esterna mappata sullo staging HDFS
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

-- 2. Elaborazione: Il blocco WITH viene posizionato in cima a tutto
WITH 
-- Fase A: Calcolo dei contatori e dei ritardi medi per ciascuna delle 3 fasce
stats_fasce AS (
    SELECT 
        origin,
        month,
        COUNT(CASE WHEN dep_delay < 15 THEN 1 END) as voli_ritardo_basso,
        ROUND(AVG(CASE WHEN dep_delay < 15 THEN dep_delay END), 2) as ritardo_medio_dep_basso,
        ROUND(AVG(CASE WHEN dep_delay < 15 THEN arr_delay END), 2) as ritardo_medio_arr_basso,
        
        COUNT(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN 1 END) as voli_ritardo_medio,
        ROUND(AVG(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN dep_delay END), 2) as ritardo_medio_dep_medio,
        ROUND(AVG(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN arr_delay END), 2) as ritardo_medio_arr_medio,
        
        COUNT(CASE WHEN dep_delay > 60 THEN 1 END) as voli_ritardo_alto,
        ROUND(AVG(CASE WHEN dep_delay > 60 THEN dep_delay END), 2) as ritardo_medio_dep_alto,
        ROUND(AVG(CASE WHEN dep_delay > 60 THEN arr_delay END), 2) as ritardo_medio_arr_alto
    FROM flights_input
    GROUP BY origin, month
),
-- Fase B: flatMap delle cause (Uniamo i codici cancellazione e ritardo ignorando stringhe vuote o 'None')
all_causes AS (
    SELECT origin, month, trim(cancellation_code) as code FROM flights_input 
    WHERE cancellation_code IS NOT NULL AND trim(cancellation_code) != '' AND trim(cancellation_code) != 'None'
    UNION ALL
    SELECT origin, month, trim(delay_code) as code FROM flights_input 
    WHERE delay_code IS NOT NULL AND trim(delay_code) != '' AND trim(delay_code) != 'None'
),
-- Fase C: Conteggio frequenze di ciascun codice per gruppo
counted_causes AS (
    SELECT origin, month, code, COUNT(*) as freq
    FROM all_causes
    GROUP BY origin, month, code
),
-- Fase D: Classifica delle cause tramite Row Number
ranked_causes AS (
    SELECT origin, month, code,
           ROW_NUMBER() OVER (PARTITION BY origin, month ORDER BY freq DESC) as rn
    FROM counted_causes
),
-- Fase E: Selezione dei primi 3 codici più frequenti
top_3_list AS (
    SELECT origin, month, code, rn
    FROM ranked_causes
    WHERE rn <= 3
),
-- Fase F: Aggregazione dei top 3 codici in un'unica stringa separata da virgola
top_3_string AS (
    SELECT origin, month, CONCAT_WS(',', COLLECT_LIST(code)) as top_3_cause_ritardo_canc
    FROM (SELECT origin, month, code FROM top_3_list ORDER BY origin, month, rn) sorted_causes
    GROUP BY origin, month
)
-- Ora che le CTE sono pronte, inseriamo l'istruzione di scrittura e selezione finale
INSERT OVERWRITE DIRECTORY '${output_path}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    f.origin as aeroporto,
    f.month as mese,
    f.voli_ritardo_basso,
    f.ritardo_medio_dep_basso,
    f.ritardo_medio_arr_basso,
    f.voli_ritardo_medio,
    f.ritardo_medio_dep_medio,
    f.ritardo_medio_arr_medio,
    f.voli_ritardo_alto,
    f.ritardo_medio_dep_alto,
    f.ritardo_medio_arr_alto,
    COALESCE(t.top_3_cause_ritardo_canc, 'N/D') as top_3_cause_ritardo_canc
FROM stats_fasce f
LEFT OUTER JOIN top_3_string t ON f.origin = t.origin AND f.month = t.month
ORDER BY aeroporto ASC, mese ASC;

-- 3. Scollegamento finale del catalogo temporaneo
DROP TABLE flights_input;