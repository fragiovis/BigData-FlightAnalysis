"""Dettaglio esecuzione — tempi, stage, metriche, anteprima dell'output e log di un singolo run."""

import plotly.express as px
import streamlit as st

from bench import store
from common import filter_runs, human_bytes, run_label, runs, seconds

st.title("🔍 Dettaglio esecuzione")

df = filter_runs(runs(), key="dettaglio", batches=False)
if df.empty:
    st.info("Nessuna esecuzione registrata con questi filtri.")
    st.stop()

labels = {row["run_id"]: run_label(row) for _, row in df.iterrows()}
ids = list(labels)
requested = st.query_params.get("run")
run_id = st.selectbox("Esecuzione", ids, index=ids.index(requested) if requested in ids else 0,
                      format_func=labels.get)
st.query_params["run"] = run_id
r = df[df["run_id"] == run_id].iloc[0]
is_hive = r["tool"] == "hive"

st.caption(f"`{run_id}` · commit `{r.get('git_commit') or '—'}` · batch `{r['batch_id']}`")
if r["status"] != "ok":
    st.error(f"Esecuzione fallita (codice di uscita {r['return_code']}). Controlla il log in fondo alla pagina.")

# --- Indicatori -------------------------------------------------------------------------------
k = st.columns(4)
k[0].metric("Tempo totale", seconds(r["wall_seconds"]))
k[1].metric("Calcolo", seconds(r["engine_seconds"]),
            help="Hive: somma delle istruzioni eseguite da beeline. Spark: durata dell'applicazione.")
k[2].metric("Overhead", seconds(r["overhead_seconds"]))
k[3].metric("Righe output", f"{int(r['output_rows']):,}")

k = st.columns(4)
k[0].metric("Input", human_bytes(r["input_bytes"]))
if is_hive:
    k[1].metric("Job MapReduce", int(r.get("n_jobs") or 0))
    k[2].metric("HDFS letti", human_bytes(r.get("hdfs_read_bytes")))
    k[3].metric("HDFS scritti", human_bytes(r.get("hdfs_write_bytes")))
else:
    k[1].metric("Job / stage / task", f"{int(r.get('n_jobs') or 0)} / {int(r.get('n_stages') or 0)} / {int(r.get('n_tasks') or 0)}")
    k[2].metric("Shuffle scritto", human_bytes(r.get("shuffle_write_bytes")))
    k[3].metric("Shuffle letto", human_bytes(r.get("shuffle_read_bytes")))

# --- Timeline degli stage -----------------------------------------------------------------------
stages = store.load_stages(run_id)
st.subheader("Timeline degli stage" if not is_hive else "Timeline dei job MapReduce")
if stages.empty:
    st.info("Nessuna metrica di stage disponibile per questa esecuzione.")
else:
    stages = stages.copy()
    stages["etichetta"] = stages["stage_id"].astype(str) + " · " + stages["name"].str.slice(0, 50)
    stages["durata"] = stages["duration_s"].clip(lower=0.05)
    color = "reducers" if is_hive else "job_id"
    if color in stages:
        stages[color] = stages[color].astype(str)
    fig = px.bar(stages, base="start_s", x="durata", y="etichetta", orientation="h",
                 color=color if color in stages else None,
                 hover_data=[c for c in ["tasks", "mappers", "reducers", "input_bytes",
                                         "shuffle_write_bytes", "hdfs_read_bytes"] if c in stages],
                 labels={"durata": "Secondi dall'avvio", "etichetta": "", "job_id": "Job Spark",
                         "reducers": "Reducer"})
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(height=max(250, 32 * len(stages) + 80))
    st.plotly_chart(fig, width="stretch")

    if is_hive:
        cols = ["stage_id", "mappers", "reducers", "start_s", "duration_s", "hdfs_read_bytes", "hdfs_write_bytes", "status"]
    else:
        cols = ["stage_id", "job_id", "name", "tasks", "start_s", "duration_s", "input_bytes", "input_records",
                "shuffle_read_bytes", "shuffle_write_bytes", "shuffle_write_records", "output_bytes",
                "executor_run_ms", "gc_ms"]
    table = stages[[c for c in cols if c in stages]].copy()
    for c in table.columns:
        if c.endswith("_bytes"):
            table[c] = table[c].map(human_bytes)
    st.dataframe(table, hide_index=True, width="stretch",
                 column_config={"start_s": st.column_config.NumberColumn("Inizio (s)", format="%.2f"),
                                "duration_s": st.column_config.NumberColumn("Durata (s)", format="%.2f")})

# --- Anteprima output ----------------------------------------------------------------------------
st.subheader("Prime 10 righe dell'output")
preview = store.load_preview(run_id)
if preview.empty or len(preview) == 0:
    st.info("Nessun output disponibile.")
else:
    st.dataframe(preview, hide_index=True, width="stretch")

full = store.load_output(run_id)
if not full.empty:
    hdfs_path = r.get("output_path")
    with st.expander(f"Output completo ({len(full):,} righe)"):
        if isinstance(hdfs_path, str):
            st.caption(f"Copia dell'output scritto su HDFS in `{hdfs_path}`")
        st.dataframe(full, hide_index=True, width="stretch")
    st.download_button("Scarica output completo (CSV)", full.to_csv(index=False), f"{run_id}.csv", "text/csv")

# --- Confronto con le altre ripetizioni ------------------------------------------------------------
same = runs()
same = same[(same["env"] == r["env"]) & (same["tool"] == r["tool"]) & (same["job"] == r["job"])
            & (same["dataset"] == r["dataset"]) & (same["status"] == "ok")]
if len(same) > 1:
    st.subheader(f"Tutte le esecuzioni della stessa combinazione ({len(same)})")
    fig = px.scatter(same.sort_values("timestamp"), x="timestamp", y="wall_seconds",
                     hover_data=["run_id", "engine_seconds"], labels={"wall_seconds": "Tempo totale (s)", "timestamp": ""})
    fig.add_hline(y=same["wall_seconds"].mean(), line_dash="dash", annotation_text="media")
    fig.update_layout(height=280)
    st.plotly_chart(fig, width="stretch")

# --- Log --------------------------------------------------------------------------------------------
st.subheader("Log")
log = store.load_log(run_id)
only_relevant = st.toggle("Nascondi le righe INFO/WARN", value=True)
lines = log.splitlines()
if only_relevant:
    lines = [l for l in lines if " INFO " not in l and " WARN " not in l and not l.startswith("SLF4J")]
query = st.text_input("Cerca nel log")
if query:
    lines = [l for l in lines if query.lower() in l.lower()]
st.code("\n".join(lines[-500:]) or "(vuoto)", language=None)
st.download_button("Scarica log completo", log, f"{run_id}.log")
