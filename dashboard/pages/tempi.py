"""Tempi di esecuzione — confronto tra tecnologie, dimensioni del dataset e ambienti."""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from bench import config, store
from common import filter_runs, human_bytes, runs, tool_color_map

st.title("⏱️ Tempi di esecuzione")

df = filter_runs(runs(), key="tempi")
if df.empty:
    st.info("Nessuna esecuzione registrata con questi filtri. Lancia dei job dalla pagina «Esegui job».")
    st.stop()

ok = df[df["status"] == "ok"]
agg = store.aggregate(df)
colors = tool_color_map()

# --- Indicatori -------------------------------------------------------------------------------
k = st.columns(5)
k[0].metric("Esecuzioni", len(df))
k[1].metric("Fallite", int((df["status"] != "ok").sum()))
k[2].metric("Tempo totale", f"{ok['wall_seconds'].sum() / 60:,.1f} min")
k[3].metric("Dati elaborati", human_bytes(ok["input_bytes"].sum()))
k[4].metric("Combinazioni", len(agg))

metric = st.radio(
    "Metrica", ["wall_mean", "engine_mean", "throughput_mb_s"], horizontal=True,
    format_func={
        "wall_mean": "Tempo totale (s)",
        "engine_mean": "Tempo di calcolo (s)",
        "throughput_mb_s": "Throughput (MB/s)",
    }.get,
    help="Tempo totale = dall'avvio dello script alla fine. Calcolo = durata dell'applicazione Spark "
         "o delle query Hive, senza avvio della JVM e operazioni HDFS di contorno.",
)
metric_label = {"wall_mean": "Tempo totale (s)", "engine_mean": "Tempo di calcolo (s)",
                "throughput_mb_s": "Throughput (MB/s)"}[metric]

# --- Scalabilità al crescere dell'input ------------------------------------------------------
st.subheader("Scalabilità al crescere dell'input")
for env in sorted(agg["env"].unique()):
    sub = agg[agg["env"] == env]
    fig = px.line(
        sub, x="input_mb", y=metric, color="tool_label", facet_col="job", markers=True,
        error_y="wall_std" if metric == "wall_mean" else None,
        color_discrete_map=colors, hover_data=["dataset_label", "runs"],
        labels={"input_mb": "Dimensione input (MB)", metric: metric_label, "tool_label": "Tecnologia", "job": "Job"},
        title=config.ENVIRONMENTS[env]["label"],
    )
    fig.update_layout(height=380, legend_title_text="")
    st.plotly_chart(fig, width="stretch")

# --- Dove va il tempo ---------------------------------------------------------------------------
st.subheader("Calcolo e overhead")
st.caption("L'overhead comprende avvio della JVM, sottomissione a YARN, pulizia delle cartelle HDFS e, per Hive, "
           "la copia del dataset nella cartella di staging e la compilazione delle query.")
c1, c2 = st.columns(2)
job_sel = c1.selectbox("Job", sorted(agg["job"].unique()), key="ovh_job")
env_sel = c2.selectbox("Ambiente", sorted(agg["env"].unique()), key="ovh_env",
                       format_func=lambda e: config.ENVIRONMENTS[e]["label"])
sub = agg[(agg["job"] == job_sel) & (agg["env"] == env_sel)].copy()
sub["x"] = sub["tool_label"] + " · " + sub["dataset_label"]
fig = go.Figure([
    go.Bar(name="Calcolo", x=sub["x"], y=sub["engine_mean"], marker_color="#3a6ea5"),
    go.Bar(name="Overhead", x=sub["x"], y=sub["overhead_mean"], marker_color="#bbbbbb"),
])
fig.update_layout(barmode="stack", height=380, yaxis_title="Secondi", legend_title_text="")
st.plotly_chart(fig, width="stretch")

# --- Confronto tra ambienti --------------------------------------------------------------------
if agg["env"].nunique() > 1:
    st.subheader("Confronto tra ambienti")
    c1, c2 = st.columns(2)
    job_env = c1.selectbox("Job", sorted(agg["job"].unique()), key="env_job")
    common_ds = sorted(agg["dataset"].unique(), key=lambda d: config.dataset_percent(d) or 0)
    ds_env = c2.selectbox("Dataset", common_ds, index=len(common_ds) - 1, key="env_ds")
    sub = agg[(agg["job"] == job_env) & (agg["dataset"] == ds_env)].copy()
    sub["ambiente"] = sub["env"].map(lambda e: config.ENVIRONMENTS[e]["label"])
    fig = px.bar(sub, x="ambiente", y=metric, color="tool_label", barmode="group",
                 error_y="wall_std" if metric == "wall_mean" else None, color_discrete_map=colors,
                 labels={"ambiente": "", metric: metric_label, "tool_label": "Tecnologia"})
    fig.update_layout(height=380, legend_title_text="")
    st.plotly_chart(fig, width="stretch")

# --- Shuffle e I/O ---------------------------------------------------------------------------------
st.subheader("Shuffle e I/O")
st.caption("Spark: byte scritti nello shuffle (dall'event log). Hive: byte letti da HDFS da tutti i job MapReduce "
           "della query, che rileggono i dati a ogni stage.")
io = ok.copy()
io["byte_mb"] = io.apply(
    lambda r: (r.get("shuffle_write_bytes") if r["tool"] != "hive" else r.get("hdfs_read_bytes")) or 0, axis=1
) / 2**20
io["metrica"] = io["tool"].map(lambda t: "HDFS letti (Hive)" if t == "hive" else "Shuffle scritto (Spark)")
io = io.groupby(["env", "tool_label", "job", "input_mb", "metrica"], as_index=False)["byte_mb"].mean()
fig = px.line(io, x="input_mb", y="byte_mb", color="tool_label", facet_col="job", line_dash="metrica",
              markers=True, color_discrete_map=colors,
              labels={"input_mb": "Dimensione input (MB)", "byte_mb": "MB", "tool_label": "Tecnologia"})
fig.update_layout(height=380, legend_title_text="")
st.plotly_chart(fig, width="stretch")

# --- Tabelle ---------------------------------------------------------------------------------------
st.subheader("Tabella riassuntiva")
table = agg.assign(ambiente=agg["env"].map(lambda e: config.ENVIRONMENTS[e]["label"]))[
    ["ambiente", "tool_label", "job", "dataset_label", "input_mb", "runs", "wall_mean", "wall_std",
     "wall_min", "wall_max", "engine_mean", "overhead_mean", "throughput_mb_s"]
]
st.dataframe(
    table, hide_index=True, width="stretch",
    column_config={
        "ambiente": "Ambiente", "tool_label": "Tecnologia", "job": "Job", "dataset_label": "Dataset",
        "input_mb": st.column_config.NumberColumn("Input (MB)", format="%.1f"),
        "runs": st.column_config.NumberColumn("Ripetizioni"),
        "wall_mean": st.column_config.NumberColumn("Media (s)", format="%.2f"),
        "wall_std": st.column_config.NumberColumn("Dev. std (s)", format="%.2f"),
        "wall_min": st.column_config.NumberColumn("Min (s)", format="%.2f"),
        "wall_max": st.column_config.NumberColumn("Max (s)", format="%.2f"),
        "engine_mean": st.column_config.NumberColumn("Calcolo (s)", format="%.2f"),
        "overhead_mean": st.column_config.NumberColumn("Overhead (s)", format="%.2f"),
        "throughput_mb_s": st.column_config.NumberColumn("MB/s", format="%.2f"),
    },
)
st.download_button("Scarica tabella riassuntiva (CSV)", table.to_csv(index=False), "tempi_riassunto.csv", "text/csv")

with st.expander(f"Tutte le esecuzioni ({len(df)})"):
    st.dataframe(df.drop(columns=["tool"]), hide_index=True, width="stretch")
    st.download_button("Scarica tutte le esecuzioni (CSV)", df.to_csv(index=False), "esecuzioni.csv", "text/csv")
