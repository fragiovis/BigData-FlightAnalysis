"""
store.py — Lettura dei risultati salvati in results/runs/<run_id>/.
"""

import json

import pandas as pd

from . import config


def load_legacy():
    """Esecuzioni su AWS EMR della versione precedente, ricostruite dai grafici (sola consultazione)."""
    if not config.LEGACY_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(config.LEGACY_CSV)
    df["run_id"] = "legacy-" + df["tool"] + "-" + df["job"] + "-" + df["dataset"]
    df["batch_id"] = "legacy-aws-emr"
    df["timestamp"] = pd.NaT
    df["status"] = "ok"
    df["repetition"] = 1
    df["legacy"] = True
    return df


def load_runs(include_legacy=False):
    """Tutte le esecuzioni come DataFrame, dalla più recente.

    include_legacy aggiunge i tempi AWS della versione precedente (senza log né metriche).
    """
    records = []
    for path in config.RUNS_DIR.glob("*/record.json"):
        try:
            records.append(json.loads(path.read_text()))
        except (OSError, json.JSONDecodeError):
            continue

    frames = [pd.DataFrame(records).assign(legacy=False)] if records else []
    if include_legacy:
        frames.append(load_legacy())
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["tool_label"] = df["tool"].map(config.TOOL_LABELS)
    df["dataset_label"] = df["dataset"].map(config.dataset_label)
    df["input_mb"] = df["input_bytes"] / 2**20
    return df.sort_values("timestamp", ascending=False).reset_index(drop=True)


def runs_signature():
    """Cambia quando si aggiunge un'esecuzione: serve a invalidare la cache di Streamlit."""
    paths = list(config.RUNS_DIR.glob("*/record.json"))
    if config.LEGACY_CSV.exists():
        paths.append(config.LEGACY_CSV)
    return len(paths), max((p.stat().st_mtime for p in paths), default=0)


def run_dir(run_id):
    return config.RUNS_DIR / run_id


def load_stages(run_id):
    path = run_dir(run_id) / "stages.json"
    return pd.DataFrame(json.loads(path.read_text())) if path.exists() else pd.DataFrame()


def load_preview(run_id):
    path = run_dir(run_id) / "preview.csv"
    return pd.read_csv(path, dtype=str, keep_default_na=False) if path.exists() else pd.DataFrame()


def load_log(run_id):
    path = run_dir(run_id) / "log.txt"
    return path.read_text(errors="replace") if path.exists() else ""


def aggregate(df):
    """Media, deviazione standard e numero di ripetizioni per (ambiente, tecnologia, job, dataset)."""
    ok = df[df["status"] == "ok"]
    if ok.empty:
        return pd.DataFrame()
    keys = ["env", "tool", "tool_label", "job", "dataset", "dataset_label", "dataset_percent"]
    agg = ok.groupby(keys, dropna=False).agg(
        wall_mean=("wall_seconds", "mean"),
        wall_std=("wall_seconds", "std"),
        wall_min=("wall_seconds", "min"),
        wall_max=("wall_seconds", "max"),
        engine_mean=("engine_seconds", "mean"),
        overhead_mean=("overhead_seconds", "mean"),
        input_mb=("input_mb", "mean"),
        runs=("run_id", "count"),
    ).reset_index()
    agg["wall_std"] = agg["wall_std"].fillna(0.0)
    agg["throughput_mb_s"] = agg["input_mb"] / agg["wall_mean"]
    return agg.sort_values(["job", "tool", "dataset_percent"])
