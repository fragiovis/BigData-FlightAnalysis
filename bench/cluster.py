"""
cluster.py — Stato del cluster Hadoop (demoni, nodi YARN, dataset su HDFS) e avvio/arresto.
"""

import subprocess
from pathlib import Path

from . import config

DAEMONS = ["NameNode", "DataNode", "SecondaryNameNode", "ResourceManager", "NodeManager"]


def _run(cmd, env="local", timeout=60):
    try:
        r = subprocess.run(cmd, env=config.job_env(env), stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, text=True, timeout=timeout)
        return r.returncode, r.stdout
    except (OSError, subprocess.TimeoutExpired) as e:
        return 1, str(e)


def daemons():
    """Demoni Hadoop attivi secondo jps."""
    _, out = _run(["jps"])
    running = {line.split()[1] for line in out.splitlines() if len(line.split()) == 2}
    return {d: d in running for d in DAEMONS}


def yarn_nodes(env="local"):
    rc, out = _run(["yarn", "node", "-list", "-all"], env)
    nodes = []
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 4 and ":" in parts[0] and parts[1] in {"RUNNING", "UNHEALTHY", "LOST", "DECOMMISSIONED", "SHUTDOWN", "NEW"}:
            nodes.append({"nodo": parts[0], "stato": parts[1], "container_attivi": parts[-1]})
    return nodes


def hdfs_datasets(env="local"):
    """Dataset su HDFS: una cartella per dataset in <base>/data/ (le repliche contengono più file)."""
    rc, out = _run(["hdfs", "dfs", "-du", f"{config.hdfs_base(env)}/data/"], env)
    rows = []
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[0].isdigit() and "/data/" in parts[-1]:
            name = Path(parts[-1]).name
            rows.append({
                "dataset": name,
                "dimensione": config.dataset_label(name),
                "MB": round(int(parts[0]) / 2**20, 1),
                "percorso": parts[-1],
            })
    return sorted(rows, key=lambda r: config.dataset_percent(r["dataset"]) or 0)


def hadoop_home():
    return Path(config.job_env("local")["HADOOP_HOME"])


def start():
    sbin = hadoop_home() / "sbin"
    out = _run([str(sbin / "start-dfs.sh")], timeout=180)[1]
    out += _run([str(sbin / "start-yarn.sh")], timeout=180)[1]
    return out


def stop():
    sbin = hadoop_home() / "sbin"
    out = _run([str(sbin / "stop-yarn.sh")], timeout=180)[1]
    out += _run([str(sbin / "stop-dfs.sh")], timeout=180)[1]
    return out
