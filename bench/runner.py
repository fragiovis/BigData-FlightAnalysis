"""
runner.py — Esecuzione di un singolo job e salvataggio di tempi, metriche, log e anteprima.

Ogni esecuzione produce la cartella results/runs/<run_id>/ con:
  record.json   riepilogo (tempi, metriche, esito)
  log.txt       stdout + stderr dello script di lancio
  stages.json   stage Spark o stage MapReduce di Hive
  preview.csv   prime 10 righe dell'output
"""

import fcntl
import json
import os
import signal
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime

from . import config, metrics


class RunnerBusy(RuntimeError):
    pass


@contextmanager
def exclusive_lock():
    """Impedisce due esecuzioni contemporanee (es. dalla pagina e da benchmark.py)."""
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(config.RESULTS_DIR / ".runner.lock", "w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RunnerBusy("Un'altra esecuzione è già in corso") from None
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def hdfs(env, *args):
    return subprocess.run(
        ["hdfs", "dfs", *args], env=config.job_env(env),
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )


def input_size(env, dataset):
    out = hdfs(env, "-du", "-s", f"{config.hdfs_base(env)}/data/{dataset}.csv").stdout.split()
    return int(out[0]) if out else None


def git_commit():
    r = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=config.ROOT_DIR,
                       stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    return r.stdout.strip() or None


def run_job(tool, job, dataset, env, repetition=1, batch_id=None, on_line=None, should_stop=None):
    """Esegue tool/job su dataset nell'ambiente env e restituisce il record salvato.

    on_line: callback opzionale chiamata con ogni riga di log (per lo streaming nella pagina).
    should_stop: callback opzionale; se restituisce True il job viene interrotto.
    """
    started = datetime.now()
    run_id = f"{started:%Y%m%d-%H%M%S}-{env}-{tool}-{job}-{dataset}-r{repetition}"
    run_dir = config.RUNS_DIR / run_id
    run_dir.mkdir(parents=True)

    proc_env = config.job_env(env)
    events_dir = None
    if tool != "hive":
        events_dir = run_dir / "spark-events"
        events_dir.mkdir()
        proc_env["SPARK_SUBMIT_EXTRA"] = (
            f"--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file://{events_dir}"
        )

    settings = config.ENVIRONMENTS[env]
    cmd = ["bash", settings["script"], job, dataset, settings["master"]]

    t0 = time.time()
    with open(run_dir / "log.txt", "w") as log:
        # Nuova sessione di processo: in caso di interruzione si termina anche la JVM figlia
        proc = subprocess.Popen(cmd, cwd=config.ROOT_DIR / tool, env=proc_env, start_new_session=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout:
            log.write(line)
            if on_line:
                on_line(line.rstrip("\n"))
            if should_stop and should_stop():
                os.killpg(proc.pid, signal.SIGTERM)
                log.write("\n[RUNNER] Esecuzione interrotta dall'utente\n")
                break
        rc = proc.wait()
    wall_seconds = time.time() - t0

    log_text = (run_dir / "log.txt").read_text(errors="replace")
    if tool == "hive":
        summary, stages = metrics.parse_hive_log(log_text)
    else:
        summary, stages = metrics.parse_spark_eventlog(events_dir)

    # Anteprima e numero di righe dell'output (Hive non scrive l'header)
    output_lines = hdfs(env, "-cat", f"{config.hdfs_base(env)}/{tool}/{job}/*").stdout.splitlines() if rc == 0 else []
    has_header = bool(output_lines) and output_lines[0].startswith(config.JOB_COLUMNS[job][0])
    data_lines = output_lines[1:] if has_header else output_lines
    (run_dir / "preview.csv").write_text("\n".join([",".join(config.JOB_COLUMNS[job])] + data_lines[:10]) + "\n")
    (run_dir / "stages.json").write_text(json.dumps(stages, indent=1))

    engine = summary.get("engine_seconds")
    record = {
        "run_id": run_id,
        "batch_id": batch_id or run_id,
        "timestamp": started.isoformat(timespec="seconds"),
        "env": env,
        "tool": tool,
        "job": job,
        "dataset": dataset,
        "dataset_percent": config.dataset_percent(dataset),
        "repetition": repetition,
        "status": "ok" if rc == 0 else "failed",
        "return_code": rc,
        "wall_seconds": round(wall_seconds, 3),
        "engine_seconds": round(engine, 3) if engine is not None else None,
        "overhead_seconds": round(wall_seconds - engine, 3) if engine is not None else None,
        "input_bytes": input_size(env, dataset),
        "output_rows": len(data_lines),
        "git_commit": git_commit(),
        **{k: v for k, v in summary.items() if k != "engine_seconds"},
    }
    (run_dir / "record.json").write_text(json.dumps(record, indent=1))
    return record
