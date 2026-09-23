"""Cluster e dati — stato dei demoni Hadoop, nodi YARN e dataset su HDFS."""

from pathlib import Path

import pandas as pd
import streamlit as st

from bench import cluster, config
from common import batch_manager

st.title("🖥️ Cluster e dati")

on_emr = Path("/usr/lib/spark").exists()
env = "aws" if on_emr else "local"

if not on_emr:
    st.subheader("Demoni Hadoop")
    status = cluster.daemons()
    cols = st.columns(len(status))
    for col, (name, up) in zip(cols, status.items()):
        col.metric(name, "attivo" if up else "spento", delta=None)

    c1, c2, _ = st.columns([1, 1, 3])
    busy = batch_manager().state.running
    if c1.button("▶ Avvia HDFS e YARN", disabled=all(status.values())):
        with st.spinner("Avvio in corso…"):
            st.code(cluster.start(), language=None)
        st.rerun()
    if c2.button("⏹ Ferma HDFS e YARN", disabled=busy or not any(status.values())):
        with st.spinner("Arresto in corso…"):
            st.code(cluster.stop(), language=None)
        st.rerun()
    st.caption("Interfacce web: HDFS http://localhost:9870 · YARN http://localhost:8088")

st.subheader("Nodi YARN")
nodes = cluster.yarn_nodes(env)
if nodes:
    st.dataframe(pd.DataFrame(nodes), hide_index=True, width="stretch")
    if any(n["stato"] == "UNHEALTHY" for n in nodes):
        st.warning("Un nodo è UNHEALTHY: di solito il disco è quasi pieno. YARN non assegnerà container "
                   "finché non si libera spazio.")
else:
    st.info("YARN non risponde: il cluster è spento?")

st.subheader(f"Dataset su HDFS ({config.hdfs_base(env)}/data)")
datasets = cluster.hdfs_datasets(env)
if datasets:
    st.dataframe(pd.DataFrame(datasets), hide_index=True, width="stretch")
else:
    st.info("Nessun dataset su HDFS. Generali dalla pagina «Esegui job».")
