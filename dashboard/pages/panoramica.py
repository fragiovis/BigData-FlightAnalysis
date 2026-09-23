"""Panoramica — descrizione del progetto e sintesi delle esecuzioni."""

import plotly.express as px
import streamlit as st

from bench import config, store
from common import runs, tool_color_map

st.title("✈️ Flight Analysis — Benchmark Big Data")
st.write(
    "Confronto tra **Spark Core** (RDD), **Spark SQL** (DataFrame e Catalyst) e **Hive** (MapReduce) sul "
    "[Flight Delay Dataset 2024](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024): "
    "circa 7 milioni di voli interni negli Stati Uniti. Gli stessi job vengono eseguiti in locale, "
    "sullo pseudo-cluster YARN e su AWS EMR, con dataset di dimensione crescente."
)

c1, c2 = st.columns(2)
with c1:
    st.subheader(config.JOB_TITLES["job_1"])
    st.write("Per ogni compagnia e aeroporto di partenza: numero di voli, ritardo minimo, massimo e medio "
             "in arrivo, tasso di cancellazione e mesi in cui la compagnia opera.")
with c2:
    st.subheader(config.JOB_TITLES["job_2"])
    st.write("Per ogni aeroporto e mese: voli nelle fasce di ritardo in partenza (< 15, 15–60, > 60 minuti), "
             "ritardi medi in partenza e arrivo per fascia e le tre cause di ritardo o cancellazione più frequenti.")

df = runs()
st.divider()
if df.empty:
    st.info("Non ci sono ancora esecuzioni registrate. Parti dalla pagina «Esegui job».")
    st.stop()

ok = df[df["status"] == "ok"]
k = st.columns(4)
k[0].metric("Esecuzioni registrate", len(df))
k[1].metric("Riuscite", f"{len(ok) / len(df):.0%}")
k[2].metric("Ambienti", ", ".join(config.ENVIRONMENTS[e]["label"] for e in sorted(df["env"].unique())))
k[3].metric("Ultima esecuzione", f"{df['timestamp'].max():%d/%m %H:%M}")

agg = store.aggregate(df)
if not agg.empty:
    largest = agg.loc[agg.groupby(["env", "job"])["dataset_percent"].transform("max") == agg["dataset_percent"]].copy()
    largest["ambiente"] = largest["env"].map(lambda e: config.ENVIRONMENTS[e]["label"])
    largest["gruppo"] = largest["ambiente"] + " · " + largest["dataset_label"]
    st.subheader("Tempo medio sul dataset più grande di ogni ambiente")
    fig = px.bar(largest, x="gruppo", y="wall_mean", color="tool_label", barmode="group", facet_col="job",
                 error_y="wall_std", color_discrete_map=tool_color_map(),
                 labels={"gruppo": "", "wall_mean": "Tempo totale (s)", "tool_label": "Tecnologia", "job": "Job"})
    fig.update_layout(height=380, legend_title_text="")
    st.plotly_chart(fig, width="stretch")
    st.caption("Tutti i grafici, con filtri e tabelle, sono nella pagina «Tempi di esecuzione».")
