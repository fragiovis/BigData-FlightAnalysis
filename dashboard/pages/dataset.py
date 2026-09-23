"""Dataset e qualità — esito della validazione e delle trasformazioni del preprocessing."""

import json

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from bench import cluster, config

st.title("🧹 Dataset e qualità dei dati")

path = config.RESULTS_DIR / "qualita_dati.json"
if not path.exists():
    st.info("Nessun riepilogo disponibile: rigenera i dataset dalla pagina «Esegui job».")
    st.stop()

q = json.loads(path.read_text())
steps = pd.DataFrame(q["passi"])
excluded = steps[steps["tipo"] == "esclusione"]
transformed = steps[steps["tipo"] == "trasformazione"]
removed = int(excluded["righe"].sum())

st.caption(f"Sorgente `{q['sorgente']}` · preprocessing eseguito il {pd.Timestamp(q['generato']):%d/%m/%Y alle %H:%M}")

k = st.columns(4)
k[0].metric("Righe iniziali", f"{q['righe_iniziali']:,}")
k[1].metric("Righe escluse", f"{removed:,}", f"-{removed / q['righe_iniziali']:.2%}", delta_color="off")
k[2].metric("Righe finali", f"{q['righe_finali']:,}")
k[3].metric("Colonne", f"{q['colonne_iniziali']} → {len(q['colonne_finali'])}")

# --- Validazione --------------------------------------------------------------------------------
st.subheader("Controlli di validazione")
st.write("Ogni riga viene esclusa dal primo controllo che non supera; i controlli sono applicati in quest'ordine.")

remaining = [q["righe_iniziali"]]
for n in excluded["righe"]:
    remaining.append(remaining[-1] - n)
fig = go.Figure(go.Funnel(
    y=["Righe iniziali"] + [f"dopo «{i}»" for i in excluded["id"]],
    x=remaining, textinfo="value", marker_color="#3a6ea5",
))
fig.update_layout(height=60 * len(remaining) + 60, margin=dict(l=10, r=10, t=10, b=10))
st.plotly_chart(fig, width="stretch")

table = excluded.assign(esito=excluded["righe"].map(lambda n: "✅ nessuna riga" if n == 0 else f"❌ {n:,} escluse"))
st.dataframe(table[["id", "descrizione", "esito"]], hide_index=True, width="stretch",
             column_config={"id": "Controllo", "descrizione": "Descrizione", "esito": "Esito"})

# --- Trasformazioni -----------------------------------------------------------------------------
st.subheader("Trasformazioni")
st.dataframe(transformed[["id", "descrizione", "righe"]], hide_index=True, width="stretch",
             column_config={"id": "Trasformazione", "descrizione": "Descrizione",
                            "righe": st.column_config.NumberColumn("Righe interessate", format="%d")})

# --- Dataset finale -----------------------------------------------------------------------------
st.subheader("Dataset finale")
s = q["statistiche"]
c = st.columns(4)
c[0].metric("Compagnie", s["compagnie"])
c[1].metric("Aeroporti di partenza", s["aeroporti_partenza"])
c[2].metric("Voli cancellati", f"{s['cancellati']:,}")
c[3].metric("Ritardi oltre 24 ore", f"{s['ritardo_partenza_oltre_24h']:,}", help="Ritardo in partenza > 1440 minuti")

c1, c2 = st.columns(2)
with c1:
    st.write("**Valori mancanti**")
    nulls = pd.DataFrame({"colonna": list(q["valori_mancanti"]), "mancanti": list(q["valori_mancanti"].values())})
    nulls["%"] = nulls["mancanti"] / q["righe_finali"] * 100
    st.dataframe(nulls, hide_index=True, width="stretch",
                 column_config={"%": st.column_config.NumberColumn(format="%.2f %%")})
    st.caption("I ritardi mancanti corrispondono ai voli cancellati; codici di cancellazione e di ritardo "
               "mancano quando il volo non è cancellato o non ha minuti di ritardo attribuiti.")
with c2:
    st.write("**Intervalli dei ritardi (minuti)**")
    st.dataframe(pd.DataFrame([
        {"ritardo": "in partenza", "minimo": s["dep_delay_min"], "massimo": s["dep_delay_max"]},
        {"ritardo": "in arrivo", "minimo": s["arr_delay_min"], "massimo": s["arr_delay_max"]},
    ]), hide_index=True, width="stretch")
    st.write(f"**Aeroporti con meno di {s['soglia_aeroporti_voli']} voli:** {s['aeroporti_sotto_soglia']} "
             f"({s['voli_aeroporti_sotto_soglia']} voli in tutto)")

st.subheader("Scelte di progetto")
for choice in q["scelte"]:
    st.markdown(f"- {choice}")

# --- File su HDFS ---------------------------------------------------------------------------------
st.subheader("Porzioni del dataset su HDFS")
datasets = cluster.hdfs_datasets("local")
if datasets:
    st.dataframe(pd.DataFrame(datasets), hide_index=True, width="stretch")
else:
    st.info("HDFS non raggiungibile o nessun dataset caricato.")
