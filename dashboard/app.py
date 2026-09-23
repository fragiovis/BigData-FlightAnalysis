"""
app.py — Dashboard Streamlit del benchmark Flight Analysis.

Avvio (dalla radice del progetto):  streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

# La dashboard importa sia i moduli locali (dashboard/) sia la libreria bench/ nella radice
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st  # noqa: E402

from common import batch_manager  # noqa: E402

st.set_page_config(page_title="Flight Analysis — Benchmark", page_icon="✈️", layout="wide")

pages = st.navigation({
    "Progetto": [
        st.Page("pages/panoramica.py", title="Panoramica", icon="🏠", default=True),
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

# Indicatore del batch in corso, visibile da ogni pagina
state = batch_manager().state
if state.running:
    st.sidebar.info(f"⏳ Batch in corso: {state.current + 1}/{len(state.plan)}\n\n{state.current_label}")

pages.run()
