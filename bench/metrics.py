"""
metrics.py — Estrazione delle metriche di esecuzione.

Spark: legge l'event log (JSON, un evento per riga) scritto con spark.eventLog.enabled.
Hive:  analizza l'output di beeline (job MapReduce lanciati, stage, letture/scritture HDFS).
"""

import json
import re
from datetime import datetime
from pathlib import Path

SPARK_ACCUMULABLES = {
    "internal.metrics.input.bytesRead": "input_bytes",
    "internal.metrics.input.recordsRead": "input_records",
    "internal.metrics.output.bytesWritten": "output_bytes",
    "internal.metrics.shuffle.write.bytesWritten": "shuffle_write_bytes",
    "internal.metrics.shuffle.write.recordsWritten": "shuffle_write_records",
    "internal.metrics.shuffle.read.localBytesRead": "shuffle_read_bytes",
    "internal.metrics.shuffle.read.remoteBytesRead": "shuffle_read_bytes",
    "internal.metrics.executorRunTime": "executor_run_ms",
    "internal.metrics.jvmGCTime": "gc_ms",
}


def _number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_spark_eventlog(events_dir):
    """Restituisce (riepilogo, lista_stage) dall'event log presente in events_dir."""
    files = sorted(Path(events_dir).glob("*")) if events_dir else []
    if not files:
        return {}, []

    app_start = app_end = None
    jobs = {}
    stages = []

    with open(files[0]) as f:
        for line in f:
            ev = json.loads(line)
            kind = ev.get("Event")
            if kind == "SparkListenerApplicationStart":
                app_start = ev["Timestamp"]
            elif kind == "SparkListenerApplicationEnd":
                app_end = ev["Timestamp"]
            elif kind == "SparkListenerJobStart":
                jobs[ev["Job ID"]] = {"start": ev["Submission Time"], "stages": ev.get("Stage IDs", [])}
            elif kind == "SparkListenerJobEnd":
                jobs.setdefault(ev["Job ID"], {})["end"] = ev["Completion Time"]
            elif kind == "SparkListenerStageCompleted":
                info = ev["Stage Info"]
                stage = {
                    "stage_id": info["Stage ID"],
                    "name": info.get("Stage Name", ""),
                    "tasks": info.get("Number of Tasks", 0),
                    "start_ms": info.get("Submission Time"),
                    "end_ms": info.get("Completion Time"),
                    **{v: 0.0 for v in set(SPARK_ACCUMULABLES.values())},
                }
                for acc in info.get("Accumulables", []):
                    key = SPARK_ACCUMULABLES.get(acc.get("Name"))
                    if key:
                        stage[key] += _number(acc.get("Value"))
                stages.append(stage)

    # Tempi degli stage relativi all'avvio dell'applicazione
    origin = app_start or min((s["start_ms"] for s in stages if s["start_ms"]), default=0)
    for s in stages:
        s["start_s"] = ((s["start_ms"] or origin) - origin) / 1000
        s["end_s"] = ((s["end_ms"] or s["start_ms"] or origin) - origin) / 1000
        s["duration_s"] = s["end_s"] - s["start_s"]
        s["job_id"] = next((j for j, d in jobs.items() if s["stage_id"] in d.get("stages", [])), None)
        del s["start_ms"], s["end_ms"]

    summary = {
        "engine_seconds": (app_end - app_start) / 1000 if app_start and app_end else None,
        "n_jobs": len(jobs),
        "n_stages": len(stages),
        "n_tasks": sum(s["tasks"] for s in stages),
        "input_bytes_read": sum(s["input_bytes"] for s in stages),
        "shuffle_read_bytes": sum(s["shuffle_read_bytes"] for s in stages),
        "shuffle_write_bytes": sum(s["shuffle_write_bytes"] for s in stages),
        "executor_run_seconds": sum(s["executor_run_ms"] for s in stages) / 1000,
        "gc_seconds": sum(s["gc_ms"] for s in stages) / 1000,
    }
    return summary, stages


HIVE_STAGE_INFO = re.compile(r"Hadoop job information for (Stage-\d+): number of mappers: (\d+); number of reducers: (\d+)")
HIVE_PROGRESS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) (Stage-\d+) map = (\d+)%,\s+reduce = (\d+)%")
HIVE_STAGE_IO = re.compile(r"^Stage-(Stage-\d+):(.*?)HDFS Read: (\d+) HDFS Write: (\d+).*?(SUCCESS|FAIL)")
HIVE_STATEMENT_TIME = re.compile(r"(?:rows? (?:affected|selected)) \((\d+)[,.](\d+) seconds\)")


def parse_hive_log(text):
    """Restituisce (riepilogo, lista_stage) dall'output di beeline."""
    stages = {}
    statement_seconds = []

    for line in text.splitlines():
        if m := HIVE_STAGE_INFO.search(line):
            stages.setdefault(m.group(1), {}).update(mappers=int(m.group(2)), reducers=int(m.group(3)))
        elif m := HIVE_PROGRESS.match(line):
            ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S,%f").timestamp()
            s = stages.setdefault(m.group(2), {})
            s.setdefault("first_ts", ts)
            s["last_ts"] = ts
        elif m := HIVE_STAGE_IO.match(line):
            s = stages.setdefault(m.group(1), {})
            s["hdfs_read_bytes"] = int(m.group(3))
            s["hdfs_write_bytes"] = int(m.group(4))
            s["status"] = m.group(5)
        elif m := HIVE_STATEMENT_TIME.search(line):
            statement_seconds.append(float(f"{m.group(1)}.{m.group(2)}"))

    origin = min((s["first_ts"] for s in stages.values() if "first_ts" in s), default=None)
    rows = []
    for name, s in stages.items():
        start = s.get("first_ts", origin)
        end = s.get("last_ts", start)
        rows.append({
            "stage_id": name,
            "name": f"MapReduce {name}",
            "mappers": s.get("mappers"),
            "reducers": s.get("reducers"),
            "start_s": (start - origin) if origin is not None and start is not None else 0.0,
            "end_s": (end - origin) if origin is not None and end is not None else 0.0,
            "hdfs_read_bytes": s.get("hdfs_read_bytes", 0),
            "hdfs_write_bytes": s.get("hdfs_write_bytes", 0),
            "status": s.get("status", ""),
        })
    rows.sort(key=lambda r: (r["start_s"], r["stage_id"]))
    for r in rows:
        r["duration_s"] = r["end_s"] - r["start_s"]

    summary = {
        "engine_seconds": sum(statement_seconds) if statement_seconds else None,
        "query_seconds": max(statement_seconds) if statement_seconds else None,
        "n_jobs": len(rows),
        "n_stages": len(rows),
        "hdfs_read_bytes": sum(r["hdfs_read_bytes"] for r in rows),
        "hdfs_write_bytes": sum(r["hdfs_write_bytes"] for r in rows),
    }
    return summary, rows
