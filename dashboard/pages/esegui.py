"""Esegui job — lancio di batch di job con log in tempo reale."""

import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st

from background import build_plan
from bench import cluster, config, runner
from common import batch_manager, seconds

st.title("▶️ Esegui job")

manager = batch_manager()
state = manager.state

# --- Configurazione del batch -------------------------------------------------------------
on_emr = Path("/usr/lib/spark").exists()
env_options = ["aws"] if on_emr else ["local", "yarn"]

with st.form("batch"):
    c1, c2 = st.columns(2)
    env = c1.radio("Ambiente", env_options, horizontal=True,
                   format_func=lambda e: config.ENVIRONMENTS[e]["label"])
    repetitions = c2.number_input("Ripetizioni per combinazione", 1, 10, 1,
                                  help="Più ripetizioni permettono di calcolare media e deviazione standard")

    tools = st.multiselect("Tecnologie", config.TOOLS, default=config.TOOLS, format_func=config.TOOL_LABELS.get)
    jobs = st.multiselect("Job", config.JOBS, default=config.JOBS, format_func=config.JOB_TITLES.get)

    available = [d["dataset"] for d in cluster.hdfs_datasets(env)] or config.DEFAULT_DATASETS
    datasets = st.multiselect("Dataset", available, default=available[:1],
                              format_func=lambda d: f"{d} ({config.dataset_label(d)})")

    verify = st.checkbox("Verifica che le tre tecnologie producano gli stessi risultati", value=True,
                         help="Eseguita dopo ogni gruppo (job, dataset) che include tutte e tre le tecnologie")

    plan = build_plan(tools, jobs, datasets, int(repetitions))
    submitted = st.form_submit_button(f"Avvia ({len(plan)} esecuzioni)", type="primary",
                                      disabled=state.running)

if submitted:
    if not plan:
        st.warning("Seleziona almeno una tecnologia, un job e un dataset.")
    else:
        try:
            manager.start(plan, env, verify)
            st.rerun()
        except runner.RunnerBusy as e:
            st.error(str(e))


# --- Avanzamento (aggiornato ogni secondo senza ricaricare la pagina) ----------------------
@st.fragment(run_every="1s")
def progress():
    s = manager.state
    if not s.plan:
        st.info("Nessun batch avviato in questa sessione del server.")
        return

    done = len(s.records)
    header = st.columns([4, 1])
    if s.running:
        header[0].subheader(f"In esecuzione: {done}/{len(s.plan)}")
        if header[1].button("⏹ Interrompi", disabled=s.stop_requested):
            manager.stop()
        st.progress(done / len(s.plan), text=s.current_label)
        with st.expander("Log in tempo reale", expanded=True):
            st.code("\n".join(list(s.log)[-60:]) or "…", language=None)
    else:
        esito = "interrotto" if s.stop_requested else "completato"
        durata = (s.finished_at - s.started_at).total_seconds() if s.finished_at else 0
        header[0].subheader(f"Batch {esito}: {done}/{len(s.plan)} esecuzioni in {durata:.0f} s")
        if not s.running and header[1].button("🔄 Aggiorna pagina"):
            st.rerun()

    if s.error:
        st.error(s.error)

    if s.records:
        df = pd.DataFrame(s.records)
        df["tecnologia"] = df["tool"].map(config.TOOL_LABELS)
        st.dataframe(
            df[["status", "tecnologia", "job", "dataset", "repetition", "wall_seconds",
                "engine_seconds", "overhead_seconds", "output_rows", "run_id"]],
            hide_index=True, width="stretch",
            column_config={
                "status": st.column_config.TextColumn("Esito"),
                "repetition": st.column_config.NumberColumn("Rip."),
                "wall_seconds": st.column_config.NumberColumn("Tempo totale (s)", format="%.2f"),
                "engine_seconds": st.column_config.NumberColumn("Calcolo (s)", format="%.2f"),
                "overhead_seconds": st.column_config.NumberColumn("Overhead (s)", format="%.2f"),
                "output_rows": st.column_config.NumberColumn("Righe output"),
                "run_id": st.column_config.TextColumn("Run ID"),
            },
        )
        last = s.records[-1]
        st.caption(f"Ultima esecuzione: {last['run_id']} — {seconds(last['wall_seconds'])}. "
                   "Il dettaglio completo è nella pagina «Dettaglio esecuzione».")

    for v in s.verifications:
        icon = "✅" if v["ok"] else "❌"
        with st.expander(f"{icon} Verifica {v['job']} su {v['dataset']}"):
            st.code(v["output"], language=None)


progress()

# --- Preparazione del dataset --------------------------------------------------------------
st.divider()
st.subheader("Preparazione dei dati")
st.write("Rigenera `flights_cleaned.csv` e le porzioni dal CSV originale e le carica su HDFS "
         "(`dataset/generate_data.sh`). Richiede il file grezzo in `data/raw/`.")
if st.button("Rigenera dataset", disabled=state.running or env == "aws"):
    with st.status("Preparazione dei dati in corso…", expanded=True) as status:
        box = st.empty()
        lines = []
        proc = subprocess.Popen(["bash", "generate_data.sh", "local[*]"], cwd=config.ROOT_DIR / "dataset",
                                env=config.job_env("local"), stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout:
            if line.startswith("[") or "OK" in line or "ERRORE" in line:
                lines.append(line.rstrip())
                box.code("\n".join(lines[-30:]), language=None)
        ok = proc.wait() == 0
        status.update(label="Dataset pronti" if ok else "Preparazione fallita",
                      state="complete" if ok else "error")
