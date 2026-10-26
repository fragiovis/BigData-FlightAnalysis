# ✈️ Flight Analysis — Spark Core vs Spark SQL vs Hive

**Big Data course — second project** · Università degli Studi Roma Tre · Prof. Riccardo Torlone
**Author:** Francesco Giovanardi

📄 **Final report (Italian):** [`docs/relazione/main.pdf`](docs/relazione/main.pdf) · 📋 **Assignment:** [`docs/Secondo progetto.pdf`](docs/Secondo%20progetto.pdf)

The project runs two analyses on the [2024 Flight Delay Dataset](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024) (about 7 million US domestic flights) with three technologies. It compares their correctness, expressiveness and efficiency as the data grows, in three execution environments:

| | |
|---|---|
| **Analyses** | **Job 1** — airline statistics per departure airport · **Job 2** — delay report per airport and month |
| **Technologies** | **Spark Core** (RDD) · **Spark SQL** (DataFrame + Catalyst) · **Hive** (MapReduce; Tez on EMR) |
| **Environments** | local (`local[*]`) · Hadoop/YARN pseudo-cluster · AWS EMR cluster (1 primary + 2 core `m5.xlarge`) |
| **Data** | 7 dataset sizes, from 2 MB (1%) to 1 GB (5× replica) |

**Highlights:** a fully reproducible benchmark harness that collects Spark event-log and Hive metrics, row-by-row cross-verification of the three implementations (28/28 checks passed), a Streamlit dashboard to launch and inspect runs, and a LaTeX report whose tables and figures are all generated from the raw results.

**Stack:** Python · PySpark · Spark SQL · HiveQL · Hadoop HDFS/YARN · AWS EMR & S3 · Streamlit · Plotly · LaTeX (Tectonic)

---

## Contents
1. [Repository layout](#1-repository-layout)
2. [Project phases](#2-project-phases)
3. [Key results](#3-key-results)
4. [Installation](#4-installation)
5. [Reproducing the results](#5-reproducing-the-results)
6. [Running on AWS EMR](#6-running-on-aws-emr)

---

## 1. Repository layout

```text
BigData-FlightAnalysis/
├── dataset/                   # Phases 1-3: data
│   ├── download.py            #   downloads the dataset from Kaggle into data/raw/
│   ├── preprocessing.py       #   validation, cleaning and transformations → flights_cleaned.csv
│   ├── generate_portions.py   #   nested 1%, 20%, 50%, 70% samples
│   └── generate_data.sh       #   orchestration: preprocessing, samples, replicas, upload to HDFS
├── spark-core/                # Phase 4: Spark Core jobs (job_1.py, job_2.py)
├── spark-sql/                 # Phase 4: Spark SQL jobs  (job_1.py, job_2.py)
├── hive/                      # Phase 4: HiveQL jobs     (job_1.hql, job_2.hql)
│                              #   each folder has run.sh (local/YARN) and run_aws.sh (EMR)
├── bench/                     # Phase 5: job execution and collection of timings and metrics
│   ├── runner.py              #   launches a job, measures it, stores logs/metrics/output
│   ├── metrics.py             #   metrics from Spark event logs and Hive logs
│   ├── store.py, cluster.py, config.py
├── benchmark.py               # Phase 5: command-line benchmark
├── experiments.sh             # Phase 5: full benchmark (jobs 1 and 2) for one environment
├── verify_outputs.py          # Phase 6: row-by-row comparison of the three technologies' outputs
├── dashboard/                 # Phase 7: Streamlit dashboard
├── results/
│   ├── runs/<run_id>/         #   one folder per run (timings, metrics, logs, output)
│   ├── qualita_dati.json      #   data-validation report
│   └── aws_emr/               #   timings of the AWS EMR session
├── docs/
│   ├── Secondo progetto.pdf   #   assignment
│   └── relazione/             # Phase 8: LaTeX report, script generating its tables and charts, PDF
├── logs/                      # PNG charts produced by benchmark.py
├── setup.sh                   # first start of the local cluster (formats HDFS!)
└── requirements.txt
```

---

## 2. Project phases

### Phase 1 — Dataset
`dataset/download.py` uses `kagglehub` to download `flight_data_2024.csv` (1.2 GB, 7,079,081 rows, 35 columns) into `data/raw/`.

### Phase 2 — Data preparation (`dataset/preprocessing.py`, PySpark)
1. **Normalisation**: explicit types, upper-cased and trimmed codes, missing per-cause delay minutes set to 0.
2. **Validation**: 7 checks (diverted flight, missing key fields, invalid month, non-IATA airport code, inconsistent cancellation, non-cancelled flight without delay data, per-cause minutes inconsistent with the arrival delay). Each row is excluded by the *first* check it fails, and the per-reason counts are computed in a single pass. Duplicates on the flight key are then removed.
   Outcome: the dataset is consistent; only the **17,499 diverted flights** are excluded → **7,061,582 rows**.
3. **Transformations**:
   - delays removed from the 3,345 cancelled flights that had one (a cancelled flight is not a delayed flight);
   - `delay_code` = **dominant cause** of the delay (the one with the most attributed minutes);
   - explicit, collision-free codes: `CANC_*` for cancellations, `DELAY_*` for delays (in the raw data the letter `C` meant two different causes).
4. **Selection of 9 columns**: `month`, `op_unique_carrier`, `origin`, `dest`, `dep_delay`, `arr_delay`, `cancelled`, `cancellation_code`, `delay_code` → `flights_cleaned.csv`, 205 MB.

A summary of every step is stored in `results/qualita_dati.json` and shown on the dashboard's data-quality page.

### Phase 3 — Datasets of increasing size
| Dataset | Size | How it is built | Rows |
|---|---|---|---|
| `flights_1`, `_20`, `_50`, `_70` | 1%–70% | **nested, reproducible samples**: each flight draws one random number (fixed seed); the p% sample holds the flights with value < p | 71K – 4.9M |
| `flights_cleaned` | 100% | full cleaned dataset | 7.06M |
| `flights_x2`, `flights_x5` | 2×, 5× | **controlled replicas**: 2 and 5 copies of the full dataset, created with HDFS-internal copies | 14.1M – 35.3M |

On HDFS each dataset is a folder `/user/<user>/data/<dataset>/`: Spark and Hive read the whole folder, and Hive's external table points to it directly, with no copies.

### Phase 4 — Job implementation
All three technologies produce **the same CSV** (same columns, ordering and rounding; lists separated by `|`; missing values as empty fields).

- **Job 1** — for each (airline, departure airport) pair: number of flights, min/max/average arrival delay, cancellation rate, months of activity.
- **Job 2** — for each (airport, month) pair: flights per departure-delay band (< 15, 15–60, > 60 minutes), average departure and arrival delay per band, and the three most frequent delay or cancellation causes.

| Technology | Implementation |
|---|---|
| **Spark Core** | per-key accumulators combined with `reduceByKey` (aggregation before the shuffle); for Job 2, two pipelines (bands and causes) joined with `leftOuterJoin` |
| **Spark SQL** | one query per job: `GROUP BY`, `CASE WHEN` for the bands, `ROW_NUMBER() OVER` and a positional pivot for the cause ranking |
| **Hive** | the same query on an external table, written with `INSERT OVERWRITE DIRECTORY`; runs as 2 (Job 1) or 6 (Job 2) MapReduce jobs |

Pseudocode and design details are in the report; the dashboard's job-results page shows the implementation steps and the code for each technology.

### Phase 5 — Execution and measurement
Each job is launched by its technology's `run.sh` through `bench/runner.py` (used by `benchmark.py` and by the dashboard), which records for every run, in `results/runs/<run_id>/`:

| File | Content |
|---|---|
| `record.json` | outcome, **total time**, **compute time** (Spark: application duration from the event log; Hive: statement duration), **overhead**, input, rows produced, engine metrics |
| `stages.json` | Spark stages (tasks, bytes read, shuffle bytes) or Hive MapReduce jobs (mappers, reducers, HDFS reads/writes) |
| `log.txt` | full execution log |
| `preview.csv`, `output.csv` | first 10 rows and full output |

Each job's output is also written to HDFS under `/user/<user>/<technology>/<job>/<environment>/<dataset>/`.

### Phase 6 — Correctness check
`verify_outputs.py` compares the outputs of the three technologies row by row (numbers within the rounding tolerance, strings exactly). In the final benchmark all **28 checks** (7 datasets × 2 jobs × 2 environments) pass: the three technologies produce identical results.

### Phase 7 — Dashboard (`dashboard/`, Streamlit)
- **Run jobs**: launch batches (environment, technologies, jobs, datasets, repetitions) with live logs and automatic verification;
- **Cluster and data**: HDFS and YARN status, start/stop, datasets on HDFS;
- **Dataset and quality**: validation outcome;
- **Execution times**: scalability, compute vs overhead, environment comparison, shuffle, exportable tables;
- **Run detail**: stage timeline, metrics, full output and log of each run;
- **Job results**: job descriptions, output exploration, verification and code of each implementation.

The sidebar always shows the time and details of the latest run.

### Phase 8 — Report (`docs/relazione/`)
`genera_dati.py` generates every table, chart and number quoted in the text from `results/`; `main.tex` compiles with [Tectonic](https://tectonic-typesetting.github.io/). No number in the report is written by hand.

---

## 3. Key results

Total time in seconds on the full dataset (100%):

| Job | Technology | Local | YARN | AWS EMR |
|---|---|---:|---:|---:|
| Job 1 | Spark Core | 14.6 | 29.7 | 48.3 |
| | Spark SQL | 14.5 | 37.3 | 52.4 |
| | Hive | 19.5 | 53.9 | 43.1 |
| Job 2 | Spark Core | 20.8 | 33.5 | 50.8 |
| | Spark SQL | 17.1 | 39.1 | 59.1 |
| | Hive | 30.1 | 138.6 | 54.0 |

- At these volumes **fixed start-up and coordination costs** dominate: local mode is the fastest, while YARN and EMR pay for resource allocation.
- **Spark Core** is the fastest on Job 1 because it reads the input once and reuses the shuffle results.
- **Spark SQL** is the simplest to write, but because of lazy evaluation and schema inference it re-reads the input several times (3 in Job 1, 5 in Job 2).
- **Hive** has the highest fixed cost (one MapReduce job per stage) but grows the least as data increases: on YARN, at 5×, it overtakes Spark SQL on Job 1.
- The **shuffle** stays at a few MB even with 1 GB of input, thanks to aggregation before the exchange: the dominant cost is reading the CSV, so data preparation matters more than shuffle differences.

The full analysis (expressiveness, simplicity, efficiency, scalability, shuffle) is in the [report](docs/relazione/main.pdf).

---

## 4. Installation

### Software
| Component | Version used |
|---|---|
| Java | 21 (Temurin) |
| Apache Hadoop (HDFS + YARN) | 3.4.1 |
| Apache Spark | 3.5.5 (pre-built for Hadoop 3) |
| Apache Hive | 4.0.0 (MapReduce engine, embedded Derby metastore) |
| Python | 3.11 (PySpark 3.5 does not officially support later versions) |

The scripts look for the frameworks in the home directory: `~/hadoop-3.4.1`, `~/spark-3.5.5-bin-hadoop3`, `~/apache-hive-4.0.0-bin`.

```bash
cd ~
curl -LO https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1-lean.tar.gz
curl -LO https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
curl -LO https://archive.apache.org/dist/hive/hive-4.0.0/apache-hive-4.0.0-bin.tar.gz
for f in *.tar.gz *.tgz; do tar -xzf "$f" && rm "$f"; done
```

Environment variables (in `~/.zshrc` or `~/.bashrc`):
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)        # on Linux: /usr/lib/jvm/java-21-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop-3.4.1
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$HOME/spark-3.5.5-bin-hadoop3
export HIVE_HOME=$HOME/apache-hive-4.0.0-bin
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$HIVE_HOME/bin
```

### Hadoop configuration (`$HADOOP_HOME/etc/hadoop/`)
The pseudo-cluster requires passwordless `ssh localhost` (on macOS: *System Settings → Sharing → Remote Login*).

**`hadoop-env.sh`** (at the end) — Java 21 requires opening some internal modules:
```bash
export JAVA_HOME=...   # same value as above: daemons started via ssh do not read your profile
export HADOOP_OPTS="$HADOOP_OPTS --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.xml/jdk.xml.internal=ALL-UNNAMED"
```

**`core-site.xml`** and **`hdfs-site.xml`**:
```xml
<property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
<property><name>hadoop.tmp.dir</name><value>/absolute/path/to/hadoop-data</value></property>  <!-- outside /tmp -->
<!-- hdfs-site.xml -->
<property><name>dfs.replication</name><value>1</value></property>
```

**`mapred-site.xml`** (used by Hive):
```xml
<property><name>mapreduce.framework.name</name><value>yarn</value></property>
<property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value></property>
<property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value></property>
<property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value></property>
<property><name>mapreduce.map.memory.mb</name><value>1024</value></property>
<property><name>mapreduce.reduce.memory.mb</name><value>1024</value></property>
<property><name>yarn.app.mapreduce.am.resource.mb</name><value>1024</value></property>
<!-- mapreduce.map.java.opts, mapreduce.reduce.java.opts, yarn.app.mapreduce.am.command-opts:
     the same --add-opens flags as in hadoop-env.sh -->
```

**`yarn-site.xml`** (values for a machine with 8 GB of RAM):
```xml
<property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
<property><name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name><value>org.apache.hadoop.mapred.ShuffleHandler</value></property>
<property><name>yarn.nodemanager.env-whitelist</name><value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value></property>
<property><name>yarn.nodemanager.resource.memory-mb</name><value>4096</value></property>
<property><name>yarn.nodemanager.resource.cpu-vcores</name><value>4</value></property>
<property><name>yarn.scheduler.minimum-allocation-mb</name><value>512</value></property>
<property><name>yarn.scheduler.maximum-allocation-mb</name><value>4096</value></property>
<property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
<!-- a node whose disk is above the threshold becomes UNHEALTHY and stops running containers -->
<property><name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name><value>99.5</value></property>
<property><name>yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb</name><value>3072</value></property>
<!-- caps the cache of files localised by YARN -->
<property><name>yarn.nodemanager.localizer.cache.target-size-mb</name><value>2048</value></property>
```

### First start
```bash
bash setup.sh      # FIRST TIME ONLY: formats HDFS (erases data) and starts HDFS and YARN
jps                # NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
```
Afterwards: `start-dfs.sh && start-yarn.sh` (stop: `stop-yarn.sh && stop-dfs.sh`).
Web UIs: HDFS http://localhost:9870 · YARN http://localhost:8088

### Python environment and Kaggle credentials
```bash
python3.11 -m venv env && source env/bin/activate
pip install -r requirements.txt
mkdir -p ~/.kaggle && echo "<token>" > ~/.kaggle/access_token && chmod 600 ~/.kaggle/access_token
```
Create the token at kaggle.com → *Settings* → *API*.

---

## 5. Reproducing the results

With HDFS and YARN running and the virtual environment active, from the project root:

```bash
# Phases 1-3: download, preparation, samples, replicas and upload to HDFS
cd dataset && python3 download.py && bash generate_data.sh "local[*]" && cd ..
#   (also uploads the Spark jars to /spark/jars, used by the jobs on YARN)

# Phase 5: benchmark (all datasets, the three technologies, jobs 1 and 2)
bash experiments.sh "local[*]"
bash experiments.sh yarn
#   a single job:     cd spark-sql && bash run.sh job_2 flights_20 yarn
#   with repetitions: python3 benchmark.py job_1 yarn --repeat 3

# Phase 6: verification (defaults: dataset flights_cleaned, environment local)
python3 verify_outputs.py job_1
python3 verify_outputs.py job_2 flights_x5 --env yarn

# Phase 7: dashboard → http://localhost:8501
streamlit run dashboard/app.py

# Phase 8: report tables and charts, then compilation
python3 docs/relazione/genera_dati.py
cd docs/relazione && tectonic main.tex
```

> ⚠️ During a long benchmark the computer must not go to sleep (on a laptop on battery, closing the lid suspends the processes and skews the measured times). On macOS you can launch the scripts with `caffeinate -i`.

---

## 6. Running on AWS EMR

Configuration used: **1 primary + 2 core `m5.xlarge` nodes** (4 vCPU, 16 GB), with Hadoop, Spark and Hive (on EMR, Hive runs on Tez).

1. **Data on S3**: upload the CSVs in `data/processed/` to a bucket, e.g. `s3://<bucket>/data/`.
2. **Cluster**: EMR with Hadoop, Hive and Spark; allow SSH to the primary node only from your own IP and enable auto-termination after an idle period.
3. **Data on HDFS**, from the primary node (one folder per dataset, then the replicas):
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
4. **Project and execution**:
   ```bash
   sudo dnf install -y git python3.11
   git clone https://github.com/fragiovis/BigData-FlightAnalysis.git && cd BigData-FlightAnalysis
   python3.11 -m venv env && env/bin/pip install pandas matplotlib streamlit plotly
   bash experiments.sh aws            # uses the run_aws.sh scripts
   ```
   The dashboard can also run on the primary node and be reached through an SSH tunnel (`ssh -L 8501:localhost:8501 hadoop@<primary-node>`). The `results/runs/*` folders are then copied locally with `scp` to compare them with the other environments.

The timings of the EMR session are in `results/aws_emr/tempi_aws_emr.csv` and appear in the dashboard and in the report.
