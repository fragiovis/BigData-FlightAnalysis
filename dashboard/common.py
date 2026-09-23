"""
common.py — Funzioni condivise dalle pagine della dashboard.
"""

import pandas as pd
import streamlit as st

from background import BatchManager
from bench import config, store


@st.cache_resource
def batch_manager():
    return BatchManager()


@st.cache_data(show_spinner=False)
def _load_runs(signature, include_legacy):
    return store.load_runs(include_legacy)


def runs(include_legacy=False):
    return _load_runs(store.runs_signature(), include_legacy)


LEGACY_NOTE = ("I tempi **AWS EMR** provengono dalla sessione eseguita sul cluster (1 primary + 2 core m5.xlarge), "
               "con un'esecuzione per combinazione. Da questa installazione non è possibile lanciare job su AWS.")


def human_bytes(n):
    if n is None or pd.isna(n):
        return "—"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(n) < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} PB"


def seconds(x):
    return "—" if x is None or pd.isna(x) else f"{x:,.2f} s"


def tool_color_map():
    return {config.TOOL_LABELS[t]: c for t, c in config.TOOL_COLORS.items()}


def filter_runs(df, key, envs=True, jobs=True, tools=True, datasets=True, batches=True):
    """Filtri standard nella sidebar; key rende univoci i widget di ogni pagina."""
    if df.empty:
        return df
    sb = st.sidebar
    sb.subheader("Filtri")
    if envs:
        options = sorted(df["env"].unique())
        chosen = sb.multiselect("Ambiente", options, default=options, key=f"{key}_env",
                                format_func=lambda e: config.ENVIRONMENTS[e]["label"])
        df = df[df["env"].isin(chosen)]
    if jobs:
        options = sorted(df["job"].unique())
        chosen = sb.multiselect("Job", options, default=options, key=f"{key}_job")
        df = df[df["job"].isin(chosen)]
    if tools:
        options = [t for t in config.TOOLS if t in set(df["tool"])]
        chosen = sb.multiselect("Tecnologia", options, default=options, key=f"{key}_tool",
                                format_func=config.TOOL_LABELS.get)
        df = df[df["tool"].isin(chosen)]
    if datasets:
        options = sorted(df["dataset"].unique(), key=lambda d: config.dataset_percent(d) or 0)
        chosen = sb.multiselect("Dataset", options, default=options, key=f"{key}_ds",
                                format_func=lambda d: f"{d} ({config.dataset_label(d)})")
        df = df[df["dataset"].isin(chosen)]
    if batches:
        options = ["Tutti"] + list(df["batch_id"].drop_duplicates())
        chosen = sb.selectbox("Batch", options, key=f"{key}_batch")
        if chosen != "Tutti":
            df = df[df["batch_id"] == chosen]
    return df


def run_label(row):
    status = "✅" if row["status"] == "ok" else "❌"
    return (f"{status} {row['timestamp']:%d/%m %H:%M:%S} · {config.ENVIRONMENTS[row['env']]['label']} · "
            f"{row['tool_label']} · {row['job']} · {row['dataset']} · r{row['repetition']} · {row['wall_seconds']:.1f}s")
