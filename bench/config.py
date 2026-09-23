"""
config.py — Costanti del progetto e ambiente di esecuzione dei job.
"""

import getpass
import os
import re
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT_DIR / "results"
RUNS_DIR = RESULTS_DIR / "runs"

TOOLS = ["spark-core", "spark-sql", "hive"]
JOBS = ["job_1", "job_2"]

TOOL_LABELS = {"spark-core": "Spark Core", "spark-sql": "Spark SQL", "hive": "Hive"}
TOOL_COLORS = {"spark-core": "#2a9d8f", "spark-sql": "#3a6ea5", "hive": "#e76f51"}

JOB_TITLES = {
    "job_1": "Job 1 — Statistiche delle compagnie aeree (3.1)",
    "job_2": "Job 2 — Report ritardi per aeroporto e mese (3.2)",
}

# Nomi delle colonne di output (Hive non scrive l'header, quindi servono qui)
JOB_COLUMNS = {
    "job_1": [
        "compagnia", "aeroporto_partenza", "numero_voli", "ritardo_min_arrivo",
        "ritardo_max_arrivo", "ritardo_medio_arrivo", "tasso_cancellazione", "mesi_operativi",
    ],
    "job_2": [
        "aeroporto", "mese",
        "voli_ritardo_basso", "ritardo_medio_dep_basso", "ritardo_medio_arr_basso",
        "voli_ritardo_medio", "ritardo_medio_dep_medio", "ritardo_medio_arr_medio",
        "voli_ritardo_alto", "ritardo_medio_dep_alto", "ritardo_medio_arr_alto",
        "top_3_cause_ritardo_canc",
    ],
}

# Ambienti: master passato a run.sh, script di lancio, radice HDFS
ENVIRONMENTS = {
    "local": {"label": "Locale (local[*])", "master": "local[*]", "script": "run.sh"},
    "yarn": {"label": "Pseudo-cluster YARN", "master": "yarn", "script": "run.sh"},
    "aws": {"label": "AWS EMR", "master": "yarn", "script": "run_aws.sh"},
}

# Tempi della sessione su AWS EMR (1 primary + 2 core m5.xlarge), mostrati nella dashboard
AWS_EMR_CSV = RESULTS_DIR / "aws_emr" / "tempi_aws_emr.csv"

DEFAULT_DATASETS = ["flights_1", "flights_20", "flights_50", "flights_70", "flights_cleaned",
                    "flights_x2", "flights_x5"]


def hdfs_base(env):
    return "/user/hadoop" if env == "aws" else f"/user/{getpass.getuser()}"


def dataset_percent(dataset):
    """Dimensione del dataset in % dell'originale: flights_20 -> 20, flights_cleaned -> 100, flights_x5 -> 500."""
    if dataset == "flights_cleaned":
        return 100
    m = re.fullmatch(r"flights_x(\d+)", dataset)
    if m:
        return int(m.group(1)) * 100
    m = re.fullmatch(r"flights_(\d+)", dataset)
    return int(m.group(1)) if m else None


def dataset_label(dataset):
    pct = dataset_percent(dataset)
    if pct is None:
        return dataset
    return f"{pct // 100}×" if pct > 100 else f"{pct}%"


def job_env(env):
    """Variabili d'ambiente per run.sh: in locale completa quelle di Hadoop/Spark/Hive se mancano."""
    e = os.environ.copy()
    if env == "aws":
        # Driver ed executor devono usare la stessa versione di Python: sui nodi EMR è quella di sistema,
        # anche se la dashboard gira in un ambiente virtuale con un Python più recente
        e["PYSPARK_PYTHON"] = "/usr/bin/python3"
        e["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
        return e

    home = Path.home()
    defaults = {
        "HADOOP_HOME": home / "hadoop-3.4.1",
        "SPARK_HOME": home / "spark-3.5.5-bin-hadoop3",
        "HIVE_HOME": home / "apache-hive-4.0.0-bin",
    }
    for name, path in defaults.items():
        if not Path(e.get(name, "")).is_dir() and path.is_dir():
            e[name] = str(path)
    e["HADOOP_CONF_DIR"] = str(Path(e["HADOOP_HOME"]) / "etc" / "hadoop")
    e["PATH"] = os.pathsep.join(
        [str(Path(e[n]) / "bin") for n in defaults if n in e] + [e.get("PATH", "")]
    )
    if "JAVA_HOME" not in e and Path("/usr/libexec/java_home").exists():
        e["JAVA_HOME"] = os.popen("/usr/libexec/java_home -v 21 2>/dev/null").read().strip()
    # Driver ed executor PySpark usano lo stesso interprete dell'ambiente virtuale
    e["PYSPARK_PYTHON"] = sys.executable
    return e
