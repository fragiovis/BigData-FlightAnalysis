"""Risultati dei job — esplorazione degli output (compagnie, aeroporti, ritardi, cause)."""

import io
import subprocess
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from bench import config

st.title("📊 Risultati dei job")

on_emr = Path("/usr/lib/spark").exists()
env = "aws" if on_emr else "local"
base = config.hdfs_base(env)

@st.cache_data(ttl=30, show_spinner=False)
def available_datasets(path):
    """Dataset per cui esiste un output (una cartella per dataset sotto <tecnologia>/<job>)."""
    r = subprocess.run(["hdfs", "dfs", "-ls", path], env=config.job_env(env),
                       stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    names = [Path(l.split()[-1]).name for l in r.stdout.splitlines() if l.startswith("d")]
    return sorted([n for n in names if config.dataset_percent(n)], key=config.dataset_percent)


c1, c2, c3 = st.columns(3)
job = c1.selectbox("Job", config.JOBS, format_func=config.JOB_TITLES.get)
tool = c2.selectbox("Output prodotto da", config.TOOLS, index=1, format_func=config.TOOL_LABELS.get)
datasets = available_datasets(f"{base}/{tool}/{job}")
if not datasets:
    st.warning("Nessun output su HDFS per questa combinazione: esegui prima il job.")
    st.stop()
dataset = c3.selectbox("Dataset", datasets, index=datasets.index("flights_cleaned") if "flights_cleaned" in datasets else 0,
                       format_func=lambda d: f"{d} ({config.dataset_label(d)})")
st.caption(f"Legge `{base}/{tool}/{job}/{dataset}` su HDFS: l'output dell'ultima esecuzione di quella tecnologia "
           "su quel dataset.")


@st.cache_data(ttl=30, show_spinner="Lettura da HDFS…")
def load_output(path, job):
    r = subprocess.run(["hdfs", "dfs", "-cat", f"{path}/*"], env=config.job_env(env),
                       stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    lines = [l for l in r.stdout.splitlines() if l and not l.startswith(config.JOB_COLUMNS[job][0])]
    if not lines:
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO("\n".join(lines)), names=config.JOB_COLUMNS[job], keep_default_na=True)
    return df


data = load_output(f"{base}/{tool}/{job}/{dataset}", job)
if data.empty:
    st.warning("Nessun output su HDFS per questa combinazione: esegui prima il job.")
    st.stop()

if job == "job_1":
    carriers = (data.groupby("compagnia")
                .apply(lambda g: pd.Series({
                    "voli": g["numero_voli"].sum(),
                    "aeroporti": len(g),
                    "ritardo_medio_arrivo": (g["ritardo_medio_arrivo"] * g["numero_voli"]).sum() / g["numero_voli"].sum(),
                    "tasso_cancellazione": (g["tasso_cancellazione"] * g["numero_voli"]).sum() / g["numero_voli"].sum(),
                }), include_groups=False)
                .reset_index().sort_values("voli", ascending=False))

    st.subheader("Panoramica delle compagnie")
    st.caption("Medie pesate sul numero di voli di ciascun aeroporto.")
    c1, c2 = st.columns(2)
    c1.plotly_chart(px.bar(carriers, x="compagnia", y="voli", title="Voli per compagnia"), width="stretch")
    c2.plotly_chart(px.scatter(carriers, x="ritardo_medio_arrivo", y="tasso_cancellazione", size="voli",
                               text="compagnia", title="Ritardo medio in arrivo e tasso di cancellazione",
                               labels={"ritardo_medio_arrivo": "Ritardo medio arrivo (min)",
                                       "tasso_cancellazione": "Tasso di cancellazione"}), width="stretch")

    st.subheader("Dettaglio di una compagnia")
    carrier = st.selectbox("Compagnia", carriers["compagnia"])
    sub = data[data["compagnia"] == carrier].sort_values("numero_voli", ascending=False)
    k = st.columns(4)
    row = carriers[carriers["compagnia"] == carrier].iloc[0]
    k[0].metric("Voli", f"{int(row['voli']):,}")
    k[1].metric("Aeroporti serviti", int(row["aeroporti"]))
    k[2].metric("Ritardo medio arrivo", f"{row['ritardo_medio_arrivo']:.2f} min")
    k[3].metric("Tasso di cancellazione", f"{row['tasso_cancellazione']:.2%}")
    top = sub.head(25)
    fig = px.bar(top, x="aeroporto_partenza", y="numero_voli", color="ritardo_medio_arrivo",
                 color_continuous_scale="RdYlGn_r", title="Primi 25 aeroporti per numero di voli",
                 labels={"numero_voli": "Voli", "aeroporto_partenza": "", "ritardo_medio_arrivo": "Ritardo medio"})
    st.plotly_chart(fig, width="stretch")
    st.dataframe(sub, hide_index=True, width="stretch")

else:
    st.subheader("Dettaglio di un aeroporto")
    airports = data.groupby("aeroporto")[["voli_ritardo_basso", "voli_ritardo_medio", "voli_ritardo_alto"]].sum().sum(axis=1)
    airports = airports.sort_values(ascending=False)
    airport = st.selectbox("Aeroporto di partenza", airports.index,
                           format_func=lambda a: f"{a} ({int(airports[a]):,} voli)")
    sub = data[data["aeroporto"] == airport].sort_values("mese")

    bands = sub.melt(id_vars="mese", value_vars=["voli_ritardo_basso", "voli_ritardo_medio", "voli_ritardo_alto"],
                     var_name="fascia", value_name="voli")
    bands["fascia"] = bands["fascia"].map({"voli_ritardo_basso": "Basso (< 15 min)",
                                           "voli_ritardo_medio": "Medio (15–60 min)",
                                           "voli_ritardo_alto": "Alto (> 60 min)"})
    c1, c2 = st.columns(2)
    c1.plotly_chart(px.bar(bands, x="mese", y="voli", color="fascia", title="Voli per fascia di ritardo",
                           color_discrete_sequence=["#2a9d8f", "#e9c46a", "#e76f51"]), width="stretch")

    avg = sub.melt(id_vars="mese", value_vars=["ritardo_medio_dep_medio", "ritardo_medio_dep_alto",
                                               "ritardo_medio_arr_medio", "ritardo_medio_arr_alto"],
                   var_name="serie", value_name="minuti")
    c2.plotly_chart(px.line(avg, x="mese", y="minuti", color="serie", markers=True,
                            title="Ritardo medio nelle fasce medio e alto"), width="stretch")

    st.write("**Tre cause più frequenti per mese**")
    st.dataframe(sub[["mese", "top_3_cause_ritardo_canc"]], hide_index=True, width="stretch")

    st.subheader("Cause principali in tutti gli aeroporti")
    first = data["top_3_cause_ritardo_canc"].dropna().str.split("|").str[0]
    counts = first[first != "N/D"].value_counts().reset_index()
    counts.columns = ["causa", "aeroporti_mese"]
    st.plotly_chart(px.bar(counts, x="causa", y="aeroporti_mese",
                           title="Quante volte ogni causa è la più frequente (per aeroporto e mese)"),
                    width="stretch")

with st.expander("Output completo"):
    st.dataframe(data, hide_index=True, width="stretch")

st.divider()
st.subheader("Verifica tra tecnologie")
st.write(f"Confronta riga per riga gli output di Spark Core, Spark SQL e Hive sul dataset `{dataset}`.")
if st.button("Esegui verifica"):
    r = subprocess.run([sys.executable, str(config.ROOT_DIR / "verify_outputs.py"), job, dataset, "--base", base],
                       env=config.job_env(env), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    (st.success if r.returncode == 0 else st.error)("Output identici" if r.returncode == 0 else "Ci sono differenze")
    st.code(r.stdout, language=None)
