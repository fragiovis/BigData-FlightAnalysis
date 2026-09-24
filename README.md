# ✈️ Flight Analysis — Spark Core, Spark SQL e Hive a confronto

**Corso di Big Data — Secondo progetto** · Università degli Studi Roma Tre · Prof. Riccardo Torlone
**Autore:** Francesco Giovanardi (matricola 577588)

📄 **Relazione finale:** [`docs/relazione/main.pdf`](docs/relazione/main.pdf) · 📋 **Traccia:** [`docs/Secondo progetto.pdf`](docs/Secondo%20progetto.pdf)

Il progetto realizza due analisi sul [Flight Delay Dataset 2024](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024) (circa 7 milioni di voli interni degli Stati Uniti) con tre tecnologie, e ne confronta correttezza, espressività ed efficienza al crescere dei dati e in tre ambienti di esecuzione:

| | |
|---|---|
| **Analisi** | **Job 1** — statistiche delle compagnie aeree (traccia 3.1) · **Job 2** — report dei ritardi per aeroporto e mese (traccia 3.2) |
| **Tecnologie** | **Spark Core** (RDD) · **Spark SQL** (DataFrame e Catalyst) · **Hive** (MapReduce; Tez su EMR) |
| **Ambienti** | locale (`local[*]`) · pseudo-cluster Hadoop/YARN · cluster AWS EMR (1 primario + 2 core `m5.xlarge`) |
| **Dataset** | 7 dimensioni, da 2 MB (1%) a 1 GB (replica 5×) |

---

## Indice
1. [Struttura del repository](#1-struttura-del-repository)
2. [Le fasi del progetto](#2-le-fasi-del-progetto)
3. [Risultati principali](#3-risultati-principali)
4. [Installazione](#4-installazione)
5. [Riprodurre i risultati](#5-riprodurre-i-risultati)
6. [Esecuzione su AWS EMR](#6-esecuzione-su-aws-emr)

---

## 1. Struttura del repository

```text
BigData-FlightAnalysis/
├── dataset/                   # Fase 1-3: dati
│   ├── download.py            #   download del dataset da Kaggle in data/raw/
│   ├── preprocessing.py       #   validazione, pulizia e trasformazioni → flights_cleaned.csv
│   ├── generate_portions.py   #   porzioni annidate 1%, 20%, 50%, 70%
│   └── generate_data.sh       #   orchestrazione: preprocessing, porzioni, repliche, caricamento su HDFS
├── spark-core/                # Fase 4: job in Spark Core (job_1.py, job_2.py)
├── spark-sql/                 # Fase 4: job in Spark SQL  (job_1.py, job_2.py)
├── hive/                      # Fase 4: job in HiveQL     (job_1.hql, job_2.hql)
│                              #   ogni cartella ha run.sh (locale/YARN) e run_aws.sh (EMR)
├── bench/                     # Fase 5: esecuzione dei job e raccolta di tempi e metriche
│   ├── runner.py              #   lancia un job, misura, salva log/metriche/output
│   ├── metrics.py             #   metriche da event log di Spark e log di Hive
│   ├── store.py, cluster.py, config.py
├── benchmark.py               # Fase 5: benchmark da riga di comando
├── experiments.sh             # Fase 5: benchmark completo (job 1 e 2) per un ambiente
├── verify_outputs.py          # Fase 6: confronto riga per riga degli output delle tre tecnologie
├── dashboard/                 # Fase 7: dashboard Streamlit
├── results/                   # risultati
│   ├── runs/<run_id>/         #   una cartella per esecuzione (tempi, metriche, log, output)
│   ├── qualita_dati.json      #   esito della validazione dei dati
│   └── aws_emr/               #   tempi della sessione su AWS EMR
├── docs/
│   ├── Secondo progetto.pdf   #   traccia
│   └── relazione/             # Fase 8: relazione LaTeX, script che ne genera tabelle e grafici, PDF
├── logs/                      # grafici PNG prodotti da benchmark.py
├── setup.sh                   # primo avvio del cluster locale (formatta HDFS!)
└── requirements.txt
```

---

## 2. Le fasi del progetto

### Fase 1 — Dataset
`dataset/download.py` scarica con `kagglehub` il file `flight_data_2024.csv` (1,2 GB, 7.079.081 righe, 35 colonne) in `data/raw/`.

### Fase 2 — Preparazione dei dati (`dataset/preprocessing.py`, PySpark)
1. **Normalizzazione**: tipi espliciti, codici in maiuscolo e senza spazi, minuti per causa di ritardo mancanti posti a 0.
2. **Validazione**: 7 controlli (volo dirottato, campi chiave mancanti, mese non valido, codice aeroporto non IATA, cancellazione incoerente, volo non cancellato senza ritardi, minuti per causa incoerenti con il ritardo in arrivo). Ogni riga è esclusa dal *primo* controllo che non supera; i conteggi per motivo sono calcolati in un solo passaggio. Segue la rimozione dei duplicati sulla chiave del volo.
   Esito: il dataset è coerente; vengono esclusi solo i **17.499 voli dirottati** → **7.061.582 righe**.
3. **Trasformazioni**:
   - ritardi rimossi per i 3.345 voli cancellati che ne avevano uno (un volo cancellato non è un volo in ritardo);
   - `delay_code` = **causa prevalente** del ritardo (quella con più minuti attribuiti);
   - codici espliciti e senza collisioni: `CANC_*` per le cancellazioni, `DELAY_*` per i ritardi (nel dataset la lettera `C` indicava due cause diverse).
4. **Selezione di 9 colonne**: `month`, `op_unique_carrier`, `origin`, `dest`, `dep_delay`, `arr_delay`, `cancelled`, `cancellation_code`, `delay_code` → `flights_cleaned.csv`, 205 MB.

Il riepilogo di ogni passo è in `results/qualita_dati.json` e nella pagina «Dataset e qualità» della dashboard.

### Fase 3 — Dataset di dimensione crescente
| Dataset | Dimensione | Come è costruito | Righe |
|---|---|---|---|
| `flights_1`, `_20`, `_50`, `_70` | 1%–70% | **porzioni annidate e riproducibili**: ogni volo riceve una volta un numero casuale (seed fisso), la porzione del p% contiene i voli con valore < p | 71 mila – 4,9 milioni |
| `flights_cleaned` | 100% | dataset pulito completo | 7,06 milioni |
| `flights_x2`, `flights_x5` | 2×, 5× | **repliche controllate**: 2 e 5 copie del dataset completo, create con copie interne a HDFS | 14,1 – 35,3 milioni |

Su HDFS ogni dataset è una cartella `/user/<utente>/data/<dataset>/`: Spark e Hive leggono la cartella intera, e la tabella esterna di Hive vi punta direttamente senza copie.

### Fase 4 — Implementazione dei job
Le tre tecnologie producono **lo stesso CSV** (stesse colonne, ordinamento e arrotondamenti; liste separate da `|`; valori mancanti come campo vuoto).

- **Job 1** — per ogni coppia (compagnia, aeroporto di partenza): voli, ritardo minimo/massimo/medio in arrivo, tasso di cancellazione, mesi di attività.
- **Job 2** — per ogni coppia (aeroporto, mese): voli nelle fasce di ritardo in partenza (< 15, 15–60, > 60 minuti), ritardo medio in partenza e in arrivo per fascia, le tre cause di ritardo o cancellazione più frequenti.

| Tecnologia | Implementazione |
|---|---|
| **Spark Core** | accumulatori per chiave combinati con `reduceByKey` (aggregazione prima dello shuffle); per il Job 2 due pipeline (fasce e cause) unite con `leftOuterJoin` |
| **Spark SQL** | una query per job: `GROUP BY`, `CASE WHEN` per le fasce, `ROW_NUMBER() OVER` e pivot per posizione per la classifica delle cause |
| **Hive** | stessa query su una tabella esterna, scritta con `INSERT OVERWRITE DIRECTORY`; eseguita come 2 (Job 1) o 6 (Job 2) job MapReduce |

Lo pseudocodice e le scelte di dettaglio sono nella relazione; la pagina «Risultati dei job» della dashboard mostra per ogni tecnologia le fasi dell'implementazione e il codice.

### Fase 5 — Esecuzione e misura
Ogni job viene lanciato dal `run.sh` della sua tecnologia tramite `bench/runner.py` (usato da `benchmark.py` e dalla dashboard), che registra per ogni esecuzione, in `results/runs/<run_id>/`:

| File | Contenuto |
|---|---|
| `record.json` | esito, **tempo totale**, **tempo di calcolo** (Spark: durata dell'applicazione dall'event log; Hive: durata delle istruzioni), **overhead**, input, righe prodotte, metriche del motore |
| `stages.json` | stage di Spark (task, byte letti, byte di shuffle) o job MapReduce di Hive (mapper, reducer, letture/scritture HDFS) |
| `log.txt` | log completo dell'esecuzione |
| `preview.csv`, `output.csv` | prime 10 righe e output completo |

L'output di ogni job è scritto anche su HDFS in `/user/<utente>/<tecnologia>/<job>/<ambiente>/<dataset>/`.

### Fase 6 — Verifica della correttezza
`verify_outputs.py` confronta riga per riga gli output delle tre tecnologie (valori numerici con tolleranza pari all'arrotondamento, stringhe in modo esatto). Nel benchmark definitivo le **28 verifiche** (7 dataset × 2 job × 2 ambienti) sono tutte superate: le tre tecnologie producono risultati identici.

### Fase 7 — Dashboard (`dashboard/`, Streamlit)
- **Esegui job**: lancio di batch (ambiente, tecnologie, job, dataset, ripetizioni) con log in tempo reale e verifica automatica;
- **Cluster e dati**: stato di HDFS e YARN, avvio e arresto, dataset su HDFS;
- **Dataset e qualità**: esito della validazione;
- **Tempi di esecuzione**: scalabilità, calcolo e overhead, confronto tra ambienti, shuffle, tabelle esportabili;
- **Dettaglio esecuzione**: timeline degli stage, metriche, output completo e log di ogni run;
- **Risultati dei job**: descrizione dei job, esplorazione degli output, verifica e codice di ogni implementazione.
La barra laterale mostra sempre l'ora e i dettagli dell'ultima esecuzione.

### Fase 8 — Relazione (`docs/relazione/`)
`genera_dati.py` produce da `results/` tutte le tabelle, i grafici e i numeri citati nel testo; `main.tex` si compila con [Tectonic](https://tectonic-typesetting.github.io/). Nessun numero della relazione è scritto a mano.

---

## 3. Risultati principali

Tempo totale in secondi sul dataset completo (100%):

| Job | Tecnologia | Locale | YARN | AWS EMR |
|---|---|---:|---:|---:|
| Job 1 | Spark Core | 14,6 | 29,7 | 48,3 |
| | Spark SQL | 14,5 | 37,3 | 52,4 |
| | Hive | 19,5 | 53,9 | 43,1 |
| Job 2 | Spark Core | 20,8 | 33,5 | 50,8 |
| | Spark SQL | 17,1 | 39,1 | 59,1 |
| | Hive | 30,1 | 138,6 | 54,0 |

- Con questi volumi prevalgono i **costi fissi** di avvio e coordinamento: il locale è il più veloce, YARN ed EMR pagano l'allocazione delle risorse.
- **Spark Core** è il più veloce sul Job 1 perché legge l'input una sola volta e riusa i risultati dello shuffle.
- **Spark SQL** è il più semplice da scrivere, ma per la valutazione pigra e l'inferenza dello schema rilegge l'input più volte (3 nel Job 1, 5 nel Job 2).
- **Hive** ha il costo fisso più alto (un job MapReduce per fase) ma cresce meno al crescere dei dati: su YARN, al 5×, supera Spark SQL nel Job 1.
- Lo **shuffle** è di pochi MB anche con 1 GB di input, grazie all'aggregazione prima dello scambio: il costo dominante è leggere il CSV, per cui la preparazione dei dati conta più delle differenze negli shuffle.

L'analisi completa (espressività, semplicità, efficienza, scalabilità, shuffle) è nella [relazione](docs/relazione/main.pdf).

---

## 4. Installazione

### Software
| Componente | Versione usata |
|---|---|
| Java | 21 (Temurin) |
| Apache Hadoop (HDFS + YARN) | 3.4.1 |
| Apache Spark | 3.5.5 (pre-built per Hadoop 3) |
| Apache Hive | 4.0.0 (motore MapReduce, metastore Derby integrato) |
| Python | 3.11 (PySpark 3.5 non supporta ufficialmente versioni successive) |

Gli script cercano i framework nella home: `~/hadoop-3.4.1`, `~/spark-3.5.5-bin-hadoop3`, `~/apache-hive-4.0.0-bin`.

```bash
cd ~
curl -LO https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1-lean.tar.gz
curl -LO https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
curl -LO https://archive.apache.org/dist/hive/hive-4.0.0/apache-hive-4.0.0-bin.tar.gz
for f in *.tar.gz *.tgz; do tar -xzf "$f" && rm "$f"; done
```

Variabili d'ambiente (in `~/.zshrc` o `~/.bashrc`):
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)        # su Linux: /usr/lib/jvm/java-21-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop-3.4.1
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$HOME/spark-3.5.5-bin-hadoop3
export HIVE_HOME=$HOME/apache-hive-4.0.0-bin
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$HIVE_HOME/bin
```

### Configurazione di Hadoop (`$HADOOP_HOME/etc/hadoop/`)
Lo pseudo-cluster richiede che `ssh localhost` funzioni senza password (su macOS: *Impostazioni → Condivisione → Login remoto*).

**`hadoop-env.sh`** (in fondo) — Java 21 richiede di aprire alcuni moduli interni:
```bash
export JAVA_HOME=...   # stesso valore di sopra: i demoni avviati via ssh non leggono il profilo
export HADOOP_OPTS="$HADOOP_OPTS --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.xml/jdk.xml.internal=ALL-UNNAMED"
```

**`core-site.xml`** e **`hdfs-site.xml`**:
```xml
<property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
<property><name>hadoop.tmp.dir</name><value>/percorso/assoluto/hadoop-data</value></property>  <!-- fuori da /tmp -->
<!-- hdfs-site.xml -->
<property><name>dfs.replication</name><value>1</value></property>
```

**`mapred-site.xml`** (usato da Hive):
```xml
<property><name>mapreduce.framework.name</name><value>yarn</value></property>
<property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value></property>
<property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value></property>
<property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value></property>
<property><name>mapreduce.map.memory.mb</name><value>1024</value></property>
<property><name>mapreduce.reduce.memory.mb</name><value>1024</value></property>
<property><name>yarn.app.mapreduce.am.resource.mb</name><value>1024</value></property>
<!-- mapreduce.map.java.opts, mapreduce.reduce.java.opts, yarn.app.mapreduce.am.command-opts:
     gli stessi flag --add-opens di hadoop-env.sh -->
```

**`yarn-site.xml`** (valori per una macchina con 8 GB di RAM):
```xml
<property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
<property><name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name><value>org.apache.hadoop.mapred.ShuffleHandler</value></property>
<property><name>yarn.nodemanager.env-whitelist</name><value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value></property>
<property><name>yarn.nodemanager.resource.memory-mb</name><value>4096</value></property>
<property><name>yarn.nodemanager.resource.cpu-vcores</name><value>4</value></property>
<property><name>yarn.scheduler.minimum-allocation-mb</name><value>512</value></property>
<property><name>yarn.scheduler.maximum-allocation-mb</name><value>4096</value></property>
<property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
<!-- un nodo con disco oltre la soglia diventa UNHEALTHY e non esegue più container -->
<property><name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name><value>99.5</value></property>
<property><name>yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb</name><value>3072</value></property>
<!-- limita la cache dei file localizzati da YARN -->
<property><name>yarn.nodemanager.localizer.cache.target-size-mb</name><value>2048</value></property>
```

### Primo avvio
```bash
bash setup.sh      # SOLO la prima volta: formatta HDFS (cancella i dati) e avvia HDFS e YARN
jps                # NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
```
Negli avvii successivi: `start-dfs.sh && start-yarn.sh` (arresto: `stop-yarn.sh && stop-dfs.sh`).
Interfacce web: HDFS http://localhost:9870 · YARN http://localhost:8088

### Ambiente Python e credenziali Kaggle
```bash
python3.11 -m venv env && source env/bin/activate
pip install -r requirements.txt
mkdir -p ~/.kaggle && echo "<token>" > ~/.kaggle/access_token && chmod 600 ~/.kaggle/access_token
```
Il token si crea da kaggle.com → *Settings* → *API*.

---

## 5. Riprodurre i risultati

Con HDFS e YARN attivi e l'ambiente virtuale attivato, dalla radice del progetto:

```bash
# Fasi 1-3: download, preparazione, porzioni, repliche e caricamento su HDFS
cd dataset && python3 download.py && bash generate_data.sh "local[*]" && cd ..
#   (carica anche le librerie di Spark in /spark/jars, usate dai job su YARN)

# Fase 5: benchmark (tutti i dataset, le tre tecnologie, job 1 e 2)
bash experiments.sh "local[*]"
bash experiments.sh yarn
#   un singolo job:  cd spark-sql && bash run.sh job_2 flights_20 yarn
#   con ripetizioni: python3 benchmark.py job_1 yarn --repeat 3

# Fase 6: verifica (predefiniti: dataset flights_cleaned, ambiente local)
python3 verify_outputs.py job_1
python3 verify_outputs.py job_2 flights_x5 --env yarn

# Fase 7: dashboard → http://localhost:8501
streamlit run dashboard/app.py

# Fase 8: tabelle e grafici della relazione, poi compilazione
python3 docs/relazione/genera_dati.py
cd docs/relazione && tectonic main.tex
```

> ⚠️ Durante un benchmark lungo il computer non deve andare in stop (su un portatile a batteria chiudere lo schermo sospende i processi e falsa i tempi misurati). Su macOS gli script possono essere lanciati con `caffeinate -i`.

---

## 6. Esecuzione su AWS EMR

Configurazione usata: **1 nodo primario + 2 nodi core `m5.xlarge`** (4 vCPU, 16 GB), con Hadoop, Spark e Hive (su EMR Hive usa Tez).

1. **Dati su S3**: caricare i CSV di `data/processed/` in un bucket, per esempio `s3://<bucket>/data/`.
2. **Cluster**: EMR con Hadoop, Hive e Spark; abilitare l'accesso SSH al nodo primario solo dal proprio IP e impostare la terminazione automatica dopo un periodo di inattività.
3. **Dati su HDFS**, dal nodo primario (una cartella per dataset, poi le repliche):
   ```bash
   for f in flights_1 flights_20 flights_50 flights_70 flights_cleaned; do
       hdfs dfs -mkdir -p /user/hadoop/data/$f
       hdfs dfs -cp s3://<bucket>/data/$f.csv /user/hadoop/data/$f/
   done
   for n in 2 5; do
       hdfs dfs -mkdir -p /user/hadoop/data/flights_x$n
       for i in $(seq 1 $n); do
           hdfs dfs -cp /user/hadoop/data/flights_cleaned/flights_cleaned.csv /user/hadoop/data/flights_x$n/part-$i.csv
       done
   done
   ```
4. **Progetto ed esecuzione**:
   ```bash
   sudo dnf install -y git python3.11
   git clone https://github.com/fragiovis/BigData-FlightAnalysis.git && cd BigData-FlightAnalysis
   python3.11 -m venv env && env/bin/pip install pandas matplotlib streamlit plotly
   bash experiments.sh aws            # usa gli script run_aws.sh
   ```
   La dashboard si può avviare anche sul nodo primario e raggiungere con un tunnel SSH (`ssh -L 8501:localhost:8501 hadoop@<nodo-primario>`). Le cartelle `results/runs/*` si copiano poi in locale con `scp` per confrontarle con gli altri ambienti.

I tempi della sessione su EMR sono in `results/aws_emr/tempi_aws_emr.csv` e compaiono nella dashboard e nella relazione.
