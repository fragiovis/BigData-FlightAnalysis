"""
app.py — Dashboard Streamlit del benchmark Flight Analysis.

Avvio (dalla radice del progetto):  streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

# La dashboard importa sia i moduli locali (dashboard/) sia la libreria bench/ nella radice
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from bench import config  # noqa: E402
from common import batch_manager, external_run, runs  # noqa: E402

st.set_page_config(page_title="Flight Analysis — Benchmark", page_icon="✈️", layout="wide")

pages = st.navigation({
    "Progetto": [
        st.Page("pages/panoramica.py", title="Panoramica", icon="🏠", default=True),
        st.Page("pages/dataset.py", title="Dataset e qualità", icon="🧹"),
    ],
    "Esecuzione": [
        st.Page("pages/esegui.py", title="Esegui job", icon="▶️"),
        st.Page("pages/cluster.py", title="Cluster e dati", icon="🖥️"),
    ],
    "Analisi": [
        st.Page("pages/tempi.py", title="Tempi di esecuzione", icon="⏱️"),
        st.Page("pages/dettaglio.py", title="Dettaglio esecuzione", icon="🔍"),
        st.Page("pages/risultati.py", title="Risultati dei job", icon="📊"),
    ],
})

# Indicatore del batch in corso, visibile da ogni pagina e aggiornato senza ricaricare la pagina
@st.fragment(run_every="2s")
def batch_indicator():
    state = batch_manager().state
    if state.running:
        st.info(f"⏳ Batch in corso: {state.current + 1}/{len(state.plan)}\n\n{state.current_label}")
        st.progress(len(state.records) / len(state.plan))
    elif (ext := external_run()) is not None:
        st.info(f"⏳ Esecuzione da riga di comando\n\n{ext['label']}\n\n"
                f"Completate nel batch `{ext['batch_id']}`: {ext['completate']}")
    elif state.plan and state.finished_at:
        esito = "interrotto" if state.stop_requested else "completato"
        st.success(f"✅ Ultimo batch {esito}: {len(state.records)}/{len(state.plan)} "
                   f"esecuzioni alle {state.finished_at:%H:%M}")

    # Ultima esecuzione completata e riepilogo del suo batch (sempre visibili)
    df = runs()
    if df.empty:
        st.caption("Nessuna esecuzione registrata.")
        return
    last = df.iloc[0]
    finished = last["timestamp"] + pd.to_timedelta(last["wall_seconds"], unit="s")
    esito = "✅" if last["status"] == "ok" else "❌"
    batch = df[df["batch_id"] == last["batch_id"]]
    batch_ok = int((batch["status"] == "ok").sum())
    st.markdown(
        f"**🕒 Ultima esecuzione: {finished:%H:%M:%S}** del {finished:%d/%m}  \n"
        f"{esito} {last['tool_label']} · {last['job']} · {last['dataset']} · "
        f"{config.ENVIRONMENTS[last['env']]['label']} ({last['wall_seconds']:.1f} s)  \n"
        f"Batch `{last['batch_id']}`: {batch_ok}/{len(batch)} riuscite, "
        f"dalle {batch['timestamp'].min():%H:%M} alle {finished:%H:%M}"
    )


with st.sidebar:
    batch_indicator()

pages.run()
