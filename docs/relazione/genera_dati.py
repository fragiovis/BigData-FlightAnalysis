#!/usr/bin/env python3
"""
genera_dati.py — Produce tabelle, grafici e numeri della relazione a partire dai risultati misurati.

Legge results/ (esecuzioni registrate dal runner, riepilogo del preprocessing, tempi su AWS EMR)
e scrive in docs/relazione/generato/:
  dati.tex        macro con i numeri citati nel testo (\\T{ambiente}{tecnologia}{job}{dataset}, ...)
  tab_*.tex       tabelle LaTeX
  fig_*.pdf       grafici

Per ogni combinazione (ambiente, tecnologia, job, dataset) si usa il batch più recente eseguito
sui dati attuali, cioè dopo l'ultima esecuzione del preprocessing; se il batch contiene più
ripetizioni si usa la media.

Uso (dalla radice del progetto):  python3 docs/relazione/genera_dati.py
"""

import json
import re
import sys
from pathlib import Path

import matplotlib
matplotlib.use("pdf")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from bench import config, store  # noqa: E402

OUT = Path(__file__).resolve().parent / "generato"
OUT.mkdir(exist_ok=True)

TOOLS = config.TOOLS
JOBS = config.JOBS
LABEL = {"spark-core": "Spark Core", "spark-sql": "Spark SQL", "hive": "Hive"}
ENV_LABEL = {"local": "Locale (local[*])", "yarn": "Pseudo-cluster YARN", "aws": "AWS EMR"}
JOB_LABEL = {"job_1": "Job 1", "job_2": "Job 2"}
COLORS = config.TOOL_COLORS
ENV_STYLE = {"local": "-", "yarn": "--", "aws": ":"}
ENV_MARKER = {"local": "o", "yarn": "s", "aws": "^"}

plt.rcParams.update({"font.size": 9, "axes.titlesize": 10, "axes.grid": True, "grid.alpha": 0.3,
                     "figure.dpi": 150, "savefig.bbox": "tight"})

macros = []


def macro(name, value):
    macros.append(f"\\expandafter\\def\\csname {name}\\endcsname{{{value}}}")


def fmt(x, dec=1):
    if x is None or pd.isna(x):
        return "--"
    return f"{x:,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_int(x):
    return f"{int(x):,}".replace(",", ".")


def tex_escape(s):
    return str(s).replace("_", r"\_").replace("|", r"\textbar{}").replace("&", r"\&").replace("%", r"\%")


def write(name, text):
    (OUT / name).write_text(text)


def ds_label(d):
    return config.dataset_label(d).replace("%", r"\%")


# --- Dati ------------------------------------------------------------------------------------
quality = json.loads((config.RESULTS_DIR / "qualita_dati.json").read_text())
cutoff = pd.Timestamp(quality["generato"])

runs = store.load_runs(include_legacy=True)
measured = runs[(~runs["legacy"]) & (runs["status"] == "ok") & (runs["timestamp"] >= cutoff)
                & (runs["env"].isin(["local", "yarn"])) & (runs["dataset"].isin(config.DEFAULT_DATASETS))
                & (~runs["batch_id"].str.startswith("esperimento"))].copy()
keys = ["env", "tool", "job", "dataset"]
latest = (measured.sort_values("timestamp").groupby(keys)["batch_id"].last().reset_index())
selected = measured.merge(latest, on=keys + ["batch_id"])
numeric = ["wall_seconds", "engine_seconds", "overhead_seconds", "input_bytes", "output_rows",
           "n_jobs", "n_stages", "n_tasks", "shuffle_read_bytes", "shuffle_write_bytes",
           "input_bytes_read", "hdfs_read_bytes", "hdfs_write_bytes", "executor_run_seconds", "gc_seconds"]
numeric = [c for c in numeric if c in selected]
agg = selected.groupby(keys)[numeric].mean().reset_index()
reps = selected.groupby(keys)["run_id"].count().rename("ripetizioni").reset_index()
agg = agg.merge(reps, on=keys)

aws = runs[runs["legacy"]][["env", "tool", "job", "dataset", "wall_seconds", "input_bytes"]].copy()
alldata = pd.concat([agg, aws], ignore_index=True)
alldata["pct"] = alldata["dataset"].map(config.dataset_percent)
alldata["input_mb"] = alldata["input_bytes"] / 2**20

datasets = sorted(agg["dataset"].unique(), key=config.dataset_percent)
aws_datasets = sorted(aws["dataset"].unique(), key=config.dataset_percent)


def value(env, tool, job, dataset, col="wall_seconds"):
    row = alldata[(alldata.env == env) & (alldata.tool == tool) & (alldata.job == job) & (alldata.dataset == dataset)]
    return None if row.empty else row.iloc[0][col]


for _, r in alldata.iterrows():
    base = f"{r.env}:{r.tool}:{r.job}:{r.dataset}"
    macro(f"t:{base}", fmt(r.wall_seconds))
    if "engine_seconds" in r and not pd.isna(r.get("engine_seconds")):
        macro(f"e:{base}", fmt(r.engine_seconds))
        macro(f"o:{base}", fmt(r.overhead_seconds))

# --- Qualità dei dati ------------------------------------------------------------------------
q = quality
macro("q:righe_iniziali", fmt_int(q["righe_iniziali"]))
macro("q:righe_finali", fmt_int(q["righe_finali"]))
macro("q:colonne_iniziali", q["colonne_iniziali"])
steps = {s["id"]: s["righe"] for s in q["passi"]}
for k, v in steps.items():
    macro(f"q:{k}", fmt_int(v))
removed = q["righe_iniziali"] - q["righe_finali"]
macro("q:escluse", fmt_int(removed))
macro("q:escluse_pct", fmt(removed / q["righe_iniziali"] * 100, 2))
for k, v in q["statistiche"].items():
    macro(f"q:{k}", fmt_int(v) if isinstance(v, int) else fmt(v, 0))
for k, v in q["valori_mancanti"].items():
    macro(f"q:null_{k}", fmt_int(v))

rows = []
for s in q["passi"]:
    tipo = "esclusione" if s["tipo"] == "esclusione" else "trasformazione"
    rows.append(f"\\texttt{{{tex_escape(s['id'])}}} & {tipo} & {tex_escape(s['descrizione'])} & {fmt_int(s['righe'])} \\\\")
write("tab_qualita.tex", "\n".join([
    r"\begin{tabularx}{\textwidth}{@{}l l X r@{}}", r"\toprule",
    r"Passo & Tipo & Descrizione & Righe \\", r"\midrule", *rows, r"\bottomrule", r"\end{tabularx}"]))

rows = [f"\\texttt{{{tex_escape(c)}}} & {fmt_int(v)} & {fmt(v / q['righe_finali'] * 100, 2)}\\,\\% \\\\"
        for c, v in q["valori_mancanti"].items()]
write("tab_mancanti.tex", "\n".join([
    r"\begin{tabular}{@{}l r r@{}}", r"\toprule", r"Colonna & Valori mancanti & Quota \\", r"\midrule",
    *rows, r"\bottomrule", r"\end{tabular}"]))

# --- Dataset ---------------------------------------------------------------------------------
processed = ROOT / "data" / "processed"
line_counts = {}
for d in ["flights_1", "flights_20", "flights_50", "flights_70", "flights_cleaned"]:
    with open(processed / f"{d}.csv") as f:
        line_counts[d] = sum(1 for _ in f) - 1
sizes = agg.groupby("dataset")["input_bytes"].first()
rows = []
for d in datasets:
    m = re.fullmatch(r"flights_x(\d+)", d)
    n = line_counts["flights_cleaned"] * int(m.group(1)) if m else line_counts[d]
    tipo = f"replica ({m.group(1)} copie)" if m else ("completo" if d == "flights_cleaned" else "porzione annidata")
    rows.append(f"\\texttt{{{tex_escape(d)}}} & {ds_label(d)} & {tipo} & {fmt_int(n)} & {fmt(sizes[d] / 2**20, 1)} \\\\")
    macro(f"ds:righe:{d}", fmt_int(n))
    macro(f"ds:mb:{d}", fmt(sizes[d] / 2**20, 1))
write("tab_dataset.tex", "\n".join([
    r"\begin{tabular}{@{}l c l r r@{}}", r"\toprule",
    r"Dataset & Dimensione & Tipo & Righe & MB su HDFS \\", r"\midrule", *rows, r"\bottomrule", r"\end{tabular}"]))

# --- Tabelle dei tempi -------------------------------------------------------------------------
def times_table(env, ds_list, name):
    head = " & ".join(ds_label(d) for d in ds_list)
    rows = []
    for job in JOBS:
        for i, tool in enumerate(TOOLS):
            first = f"\\multirow{{3}}{{*}}{{{JOB_LABEL[job]}}}" if i == 0 else ""
            vals = " & ".join(fmt(value(env, tool, job, d)) for d in ds_list)
            rows.append(f"{first} & {LABEL[tool]} & {vals} \\\\")
        if job != JOBS[-1]:
            rows.append(r"\midrule")
    write(name, "\n".join([
        f"\\begin{{tabular}}{{@{{}}l l {'r' * len(ds_list)}@{{}}}}", r"\toprule",
        f"Job & Tecnologia & {head} \\\\", r"\midrule", *rows, r"\bottomrule", r"\end{tabular}"]))


times_table("local", datasets, "tab_tempi_local.tex")
times_table("yarn", datasets, "tab_tempi_yarn.tex")
times_table("aws", aws_datasets, "tab_tempi_aws.tex")

# Confronto tra ambienti sul dataset completo
rows = []
for job in JOBS:
    for i, tool in enumerate(TOOLS):
        first = f"\\multirow{{3}}{{*}}{{{JOB_LABEL[job]}}}" if i == 0 else ""
        vals = [value(e, tool, job, "flights_cleaned") for e in ["local", "yarn", "aws"]]
        ratio_yarn = vals[1] / vals[0] if vals[0] and vals[1] else None
        ratio_aws = vals[2] / vals[0] if vals[0] and vals[2] else None
        rows.append(f"{first} & {LABEL[tool]} & {' & '.join(fmt(v) for v in vals)} & "
                    f"{fmt(ratio_yarn, 2)}$\\times$ & {fmt(ratio_aws, 2)}$\\times$ \\\\")
        macro(f"r:yarn:{tool}:{job}", fmt(ratio_yarn, 2))
        macro(f"r:aws:{tool}:{job}", fmt(ratio_aws, 2))
    if job != JOBS[-1]:
        rows.append(r"\midrule")
write("tab_ambienti.tex", "\n".join([
    r"\begin{tabular}{@{}l l r r r r r@{}}", r"\toprule",
    r"Job & Tecnologia & Locale & YARN & AWS EMR & YARN/Locale & AWS/Locale \\", r"\midrule",
    *rows, r"\bottomrule", r"\end{tabular}"]))

# Calcolo e overhead
def overhead_table(ds_list, name):
    rows = []
    for env in ["local", "yarn"]:
        for d in ds_list:
            for job in JOBS:
                for tool in TOOLS:
                    w, e, o = (value(env, tool, job, d, c) for c in ["wall_seconds", "engine_seconds", "overhead_seconds"])
                    share = o / w * 100 if w and o is not None else None
                    rows.append(f"{ENV_LABEL[env].split(' ')[0] if env == 'local' else 'YARN'} & {ds_label(d)} & "
                                f"{JOB_LABEL[job]} & {LABEL[tool]} & {fmt(w)} & {fmt(e)} & {fmt(o)} & {fmt(share, 0)}\\,\\% \\\\")
                    macro(f"ovh:{env}:{tool}:{job}:{d}", fmt(share, 0))
            rows.append(r"\midrule")
    rows = rows[:-1]
    write(name, "\n".join([
        r"\begin{tabular}{@{}l c l l r r r r@{}}", r"\toprule",
        r"Ambiente & Dataset & Job & Tecnologia & Totale (s) & Calcolo (s) & Overhead (s) & Overhead \\",
        r"\midrule", *rows, r"\bottomrule", r"\end{tabular}"]))


overhead_table(["flights_1", "flights_x5"], "tab_overhead.tex")

# Metriche del motore sul dataset completo e sulla replica 5x su YARN: in locale i contatori HDFS di Hive
# non sono affidabili (i task MapReduce condividono la JVM e riportano valori cumulativi)
rows = []
for d in ["flights_cleaned", "flights_x5"]:
    for job in JOBS:
        for tool in TOOLS:
            r = agg[(agg.env == "yarn") & (agg.tool == tool) & (agg.job == job) & (agg.dataset == d)]
            if r.empty:
                continue
            r = r.iloc[0]
            if tool == "hive":
                rows.append(f"{ds_label(d)} & {JOB_LABEL[job]} & {LABEL[tool]} & {int(r.n_jobs)} MR & -- & "
                            f"-- & {fmt(r.hdfs_read_bytes / 2**20, 0)} (HDFS) \\\\")
                macro(f"m:hive_mr:{job}", int(r.n_jobs))
                macro(f"m:hive_read:{job}:{d}", fmt(r.hdfs_read_bytes / 2**20, 0))
            else:
                rows.append(f"{ds_label(d)} & {JOB_LABEL[job]} & {LABEL[tool]} & {int(r.n_jobs)} job & {int(r.n_stages)} & "
                            f"{int(r.n_tasks)} & {fmt(r.shuffle_write_bytes / 2**20, 1)} (shuffle) \\\\")
                macro(f"m:stages:{tool}:{job}:{d}", int(r.n_stages))
                macro(f"m:tasks:{tool}:{job}:{d}", int(r.n_tasks))
                macro(f"m:shuffle:{tool}:{job}:{d}", fmt(r.shuffle_write_bytes / 2**20, 1))
        rows.append(r"\midrule")
write("tab_metriche.tex", "\n".join([
    r"\begin{tabular}{@{}c l l l r r r@{}}", r"\toprule",
    r"Dataset & Job & Tecnologia & Job lanciati & Stage & Task & MB scambiati \\", r"\midrule",
    *rows[:-1], r"\bottomrule", r"\end{tabular}"]))

# --- Crescita dei tempi: da 1% a 100% e da 100% a 5x ------------------------------------------
for env in ["local", "yarn", "aws"]:
    for tool in TOOLS:
        for job in JOBS:
            t1, t100, t5x = (value(env, tool, job, d) for d in ["flights_1", "flights_cleaned", "flights_x5"])
            if t1 and t100:
                macro(f"g1:{env}:{tool}:{job}", fmt(t100 / t1, 1))
            if t100 and t5x:
                macro(f"g5:{env}:{tool}:{job}", fmt(t5x / t100, 1))

# --- Letture complete dell'input (stage che leggono quasi tutto l'input), locale, replica 5x ---
for tool in ["spark-core", "spark-sql"]:
    for job in JOBS:
        r = selected[(selected.env == "local") & (selected.tool == tool) & (selected.job == job)
                     & (selected.dataset == "flights_x5")].sort_values("timestamp").iloc[-1]
        st = store.load_stages(r.run_id)
        scans = st[st.input_bytes > 0.5 * r.input_bytes].sort_values("start_s")
        macro(f"scan:n:{tool}:{job}", len(scans))
        for i, (_, s) in enumerate(scans.iterrows(), 1):
            macro(f"scan:{i}:{tool}:{job}", fmt(s.duration_s))

# --- Prime 10 righe --------------------------------------------------------------------------
SHORT = {"CARRIER": "CAR", "WEATHER": "WEA", "NAS": "NAS", "SECURITY": "SEC", "LATE_AIRCRAFT": "LATE"}
HEADERS = {
    "job_1": ["Comp.", "Aerop.", "Voli", "Rit. min", "Rit. max", "Rit. medio", "Tasso canc.", "Mesi"],
    "job_2": ["Aerop.", "Mese", "B: voli", "B: dep", "B: arr", "M: voli", "M: dep", "M: arr",
              "A: voli", "A: dep", "A: arr", "Top 3 cause"],
}
for job in JOBS:
    r = selected[(selected.env == "local") & (selected.tool == "spark-sql") & (selected.job == job)
                 & (selected.dataset == "flights_cleaned")].sort_values("timestamp").iloc[-1]
    preview = store.load_preview(r.run_id)
    lines = []
    for _, row in preview.iterrows():
        cells = [tex_escape(v) for v in row.values]
        if job == "job_2":
            codes = row.values[-1].split("|")
            cells[-1] = ", ".join(("C:" if c.startswith("CANC_") else "") + SHORT.get(c.split("_", 1)[-1], c)
                                  for c in codes)
        if job == "job_1":
            cells[-1] = cells[-1].replace(r"\textbar{}", r"$|$")
        lines.append(" & ".join(cells) + r" \\")
    cols = "l l " + "r" * (len(HEADERS[job]) - 3) + " l"
    write(f"tab_righe_{job}.tex", "\n".join([
        f"\\begin{{tabular}}{{@{{}}{cols}@{{}}}}", r"\toprule", " & ".join(HEADERS[job]) + r" \\", r"\midrule",
        *lines, r"\bottomrule", r"\end{tabular}"]))
    macro(f"righe_output:{job}", fmt_int(r.output_rows))

# --- Righe di codice -------------------------------------------------------------------------
def code_lines(path):
    """Righe significative: né vuote, né commenti, né docstring, né stampe di servizio."""
    text = path.read_text()
    if path.suffix == ".py":
        # Solo le docstring (blocchi che iniziano la riga); le query SQL assegnate a variabili restano
        text = re.sub(r'^\s*"""[\s\S]*?"""', "", text, flags=re.M)
    count = 0
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#") or s.startswith("--") or s.startswith("print("):
            continue
        count += 1
    return count


FILES = {"spark-core": "{job}.py", "spark-sql": "{job}.py", "hive": "{job}.hql"}
rows = []
for tool in TOOLS:
    counts = [code_lines(ROOT / tool / FILES[tool].format(job=j)) for j in JOBS]
    for j, c in zip(JOBS, counts):
        macro(f"loc:{tool}:{j}", c)
    rows.append(f"{LABEL[tool]} & {counts[0]} & {counts[1]} \\\\")
write("tab_loc.tex", "\n".join([
    r"\begin{tabular}{@{}l r r@{}}", r"\toprule", r"Tecnologia & Job 1 & Job 2 \\", r"\midrule",
    *rows, r"\bottomrule", r"\end{tabular}"]))

# --- Grafici ---------------------------------------------------------------------------------
POS = {config.dataset_percent(d): i for i, d in enumerate(config.DEFAULT_DATASETS)}


def pct_axis(ax, ds_list):
    """Dimensioni a distanza costante sull'asse x (etichette 1%, 20%, ..., 5x)."""
    ax.set_xticks([POS[config.dataset_percent(d)] for d in ds_list])
    ax.set_xticklabels([config.dataset_label(d) for d in ds_list], rotation=0)


def scalability(env, name):
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.8), sharey=False)
    for ax, job in zip(axes, JOBS):
        for tool in TOOLS:
            sub = alldata[(alldata.env == env) & (alldata.tool == tool) & (alldata.job == job)].sort_values("pct")
            ax.plot(sub.pct.map(POS), sub.wall_seconds, marker="o", color=COLORS[tool], label=LABEL[tool])
        pct_axis(ax, sorted(alldata[alldata.env == env].dataset.unique(), key=config.dataset_percent))
        ax.set_title(JOB_LABEL[job])
        ax.set_xlabel("Dimensione del dataset")
        ax.set_ylabel("Tempo totale (s)")
    axes[0].legend()
    fig.savefig(OUT / name)
    plt.close(fig)


scalability("local", "fig_scalabilita_local.pdf")
scalability("yarn", "fig_scalabilita_yarn.pdf")
scalability("aws", "fig_scalabilita_aws.pdf")

fig, axes = plt.subplots(2, 3, figsize=(7.4, 4.6), sharex=True)
for i, job in enumerate(JOBS):
    for j, tool in enumerate(TOOLS):
        ax = axes[i, j]
        for env in ["local", "yarn", "aws"]:
            sub = alldata[(alldata.env == env) & (alldata.tool == tool) & (alldata.job == job)].sort_values("pct")
            ax.plot(sub.pct.map(POS), sub.wall_seconds, linestyle=ENV_STYLE[env], marker=ENV_MARKER[env],
                    color=COLORS[tool], label=ENV_LABEL[env], markersize=4)
        pct_axis(ax, datasets)
        ax.set_title(f"{LABEL[tool]} — {JOB_LABEL[job]}")
        ax.set_xlabel("Dimensione del dataset" if i == 1 else "")
        if j == 0:
            ax.set_ylabel("Tempo totale (s)")
        ax.tick_params(axis="x", labelsize=7)
axes[0, 0].legend(fontsize=7)
fig.tight_layout()
fig.savefig(OUT / "fig_ambienti.pdf")
plt.close(fig)

# Calcolo e overhead in locale e su YARN (dataset completo)
fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.8), sharey=True)
for ax, env in zip(axes, ["local", "yarn"]):
    labels, eng, ovh = [], [], []
    for job in JOBS:
        for tool in TOOLS:
            labels.append(f"{LABEL[tool].replace('Spark ', '')}\n{JOB_LABEL[job]}")
            eng.append(value(env, tool, job, "flights_cleaned", "engine_seconds") or 0)
            ovh.append(value(env, tool, job, "flights_cleaned", "overhead_seconds") or 0)
    x = range(len(labels))
    ax.bar(x, eng, color="#3a6ea5", label="Calcolo")
    ax.bar(x, ovh, bottom=eng, color="#bbbbbb", label="Overhead")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, fontsize=7)
    ax.set_title(f"{ENV_LABEL[env]} — dataset completo")
axes[0].set_ylabel("Secondi")
axes[0].legend()
fig.savefig(OUT / "fig_overhead.pdf")
plt.close(fig)

# Throughput
fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.8), sharey=True)
for ax, job in zip(axes, JOBS):
    for tool in TOOLS:
        for env in ["local", "yarn"]:
            sub = alldata[(alldata.env == env) & (alldata.tool == tool) & (alldata.job == job)].sort_values("pct")
            ax.plot(sub.pct.map(POS), sub.input_mb / sub.wall_seconds, linestyle=ENV_STYLE[env], marker=ENV_MARKER[env],
                    color=COLORS[tool], markersize=4, label=f"{LABEL[tool]} ({'locale' if env == 'local' else 'YARN'})")
    pct_axis(ax, datasets)
    ax.set_title(JOB_LABEL[job])
    ax.set_xlabel("Dimensione del dataset")
axes[0].set_ylabel("Throughput (MB/s)")
axes[0].legend(fontsize=6.5)
fig.savefig(OUT / "fig_throughput.pdf")
plt.close(fig)

# Dati scambiati: shuffle di Spark e letture HDFS di Hive (locale)
fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.8))
for ax, job in zip(axes, JOBS):
    for tool in TOOLS:
        sub = agg[(agg.env == "local") & (agg.tool == tool) & (agg.job == job)].copy()
        sub["pct"] = sub.dataset.map(config.dataset_percent)
        sub = sub.sort_values("pct")
        col = "hdfs_read_bytes" if tool == "hive" else "shuffle_write_bytes"
        ax.plot(sub.pct.map(POS), sub[col] / 2**20, marker="o", color=COLORS[tool],
                label=f"{LABEL[tool]} ({'letture HDFS' if tool == 'hive' else 'shuffle'})")
    pct_axis(ax, datasets)
    ax.set_yscale("log")
    ax.set_title(JOB_LABEL[job])
    ax.set_xlabel("Dimensione del dataset")
axes[0].set_ylabel("MB (scala logaritmica)")
axes[0].legend(fontsize=7)
fig.savefig(OUT / "fig_shuffle.pdf")
plt.close(fig)

# Timeline degli stage sul dataset completo (Job 2, locale)
fig, axes = plt.subplots(3, 1, figsize=(7.2, 4.6), sharex=True)
for ax, tool in zip(axes, TOOLS):
    r = selected[(selected.env == "local") & (selected.tool == tool) & (selected.job == "job_2")
                 & (selected.dataset == "flights_cleaned")].sort_values("timestamp").iloc[-1]
    st = store.load_stages(r.run_id)
    if st.empty:
        continue
    st = st.sort_values("start_s").reset_index(drop=True)
    for i, s in st.iterrows():
        ax.barh(i, max(s.duration_s, 0.1), left=s.start_s, color=COLORS[tool])
    ax.set_yticks(range(len(st)))
    ax.set_yticklabels([str(s) for s in st.stage_id], fontsize=6)
    ax.invert_yaxis()
    ax.set_title(f"{LABEL[tool]} — {len(st)} {'job MapReduce' if tool == 'hive' else 'stage'}", fontsize=9)
axes[-1].set_xlabel("Secondi dall'avvio del calcolo")
fig.tight_layout()
fig.savefig(OUT / "fig_timeline.pdf")
plt.close(fig)

# --- Esperimento: dataset con e senza la colonna dest (locale, 3 ripetizioni alternate) ---------
exp = runs[(~runs["legacy"]) & (runs["batch_id"] == "esperimento-dest") & (runs["status"] == "ok")].copy()
if not exp.empty:
    exp["senza_dest"] = exp["dataset"].str.endswith(config.NO_DEST_SUFFIX)
    exp["base"] = exp["dataset"].str.removesuffix(config.NO_DEST_SUFFIX)
    rows, diffs = [], {"flights_cleaned": [], "flights_x5": []}
    for base in ["flights_cleaned", "flights_x5"]:
        sizes = exp[exp.base == base].groupby("senza_dest")["input_bytes"].first()
        macro(f"dest:size:{base}", fmt((sizes[True] - sizes[False]) / sizes[False] * 100, 1))
        for job in JOBS:
            for tool in TOOLS:
                sub = exp[(exp.base == base) & (exp.job == job) & (exp.tool == tool)]
                w9, w8 = (sub[sub.senza_dest == flag]["wall_seconds"] for flag in (False, True))
                e9, e8 = (sub[sub.senza_dest == flag]["engine_seconds"] for flag in (False, True))
                if w9.empty or w8.empty:
                    continue
                dw = (w8.mean() - w9.mean()) / w9.mean() * 100
                de = (e8.mean() - e9.mean()) / e9.mean() * 100
                diffs[base].append(dw)
                macro(f"dest:diff:{tool}:{job}:{base}", fmt(dw, 1))
                rows.append(f"{ds_label(base)} & {JOB_LABEL[job]} & {LABEL[tool]} & "
                            f"{fmt(w9.mean())} $\\pm$ {fmt(w9.std())} & {fmt(w8.mean())} $\\pm$ {fmt(w8.std())} & "
                            f"{fmt(dw, 1)}\\,\\% & {fmt(de, 1)}\\,\\% \\\\")
        rows.append(r"\midrule")
    for base, vals in diffs.items():
        if vals:
            macro(f"dest:media:{base}", fmt(sum(vals) / len(vals), 1))
            macro(f"dest:min:{base}", fmt(min(vals), 1))
            macro(f"dest:max:{base}", fmt(max(vals), 1))
    macro("dest:ripetizioni", int(exp.groupby(["dataset", "job", "tool"]).size().min()))
    write("tab_dest.tex", "\n".join([
        r"\begin{tabular}{@{}c l l r r r r@{}}", r"\toprule",
        r"Dataset & Job & Tecnologia & Con dest (s) & Senza dest (s) & $\Delta$ totale & $\Delta$ calcolo \\",
        r"\midrule", *rows[:-1], r"\bottomrule", r"\end{tabular}"]))

# --- Macro -----------------------------------------------------------------------------------
macro("n:esecuzioni_local", int(reps[reps.env == "local"].ripetizioni.sum()))
macro("n:esecuzioni_yarn", int(reps[reps.env == "yarn"].ripetizioni.sum()))
macro("n:ripetizioni", int(agg.ripetizioni.max()))
macro("n:esecuzioni_aws", len(aws))
write("dati.tex", "\n".join(macros) + "\n")
print(f"Generati {len(list(OUT.iterdir()))} file in {OUT.relative_to(ROOT)} "
      f"({len(selected)} esecuzioni misurate + {len(aws)} su AWS EMR)")
