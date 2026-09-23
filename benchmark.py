#!/usr/bin/env python3
"""
benchmark.py — Esecuzione automatica del benchmark da riga di comando.

Ogni esecuzione viene salvata in results/runs/<run_id>/ (tempi, metriche, log, anteprima),
lo stesso archivio letto dalla dashboard Streamlit. Al termine viene generato il grafico
logs/<ambiente>/benchmark_<job>.png con la media delle ripetizioni.

Uso:  python3 benchmark.py job_1 yarn [--aws] [--fractions "0.01 0.2 0.5 0.7"] [--repeat 3]
"""

import argparse
import os
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd

from bench import config, runner


def main():
    parser = argparse.ArgumentParser(description="Automated Benchmark Execution Pipeline")
    parser.add_argument("job", type=str, choices=config.JOBS, help="Job name (matching script prefix)")
    parser.add_argument("master", type=str, choices=["local[*]", "yarn"], help="Master type execution environment")
    parser.add_argument("--fractions", type=str, default="0.01 0.2 0.5 0.7", help="Fractions of dataset to use")
    parser.add_argument("--aws", action="store_true", help="Execute using cloud scripts (run_aws.sh)")
    parser.add_argument("--repeat", type=int, default=1, help="Ripetizioni per ogni combinazione")
    args = parser.parse_args()

    env = "aws" if args.aws else ("local" if "local" in args.master else "yarn")
    datasets = [f"flights_{int(float(x) * 100)}" for x in args.fractions.split()] + ["flights_cleaned"]
    batch_id = f"batch-{datetime.now():%Y%m%d-%H%M%S}-{env}"

    records = []
    with runner.exclusive_lock():
        for tool in config.TOOLS:
            for dataset in datasets:
                for rep in range(1, args.repeat + 1):
                    print(f"[BENCHMARK] Avvio {tool} -> {args.job} su {dataset} ({env}, ripetizione {rep})")
                    r = runner.run_job(tool, args.job, dataset, env, repetition=rep, batch_id=batch_id)
                    records.append(r)
                    if r["status"] == "ok":
                        print(f"[BENCHMARK] Completato {tool}#{args.job} per \"{dataset}\" in {r['wall_seconds']:.2f} secondi")
                    else:
                        print(f"[⚠️ ERRORE CRITICO] {tool} ha fallito su {dataset}. Log: results/runs/{r['run_id']}/log.txt")

    plot(pd.DataFrame(records), args.job, env, datasets)


def plot(df, job, env, datasets):
    ok = df[df["status"] == "ok"]
    means = ok.groupby(["tool", "dataset"])["wall_seconds"].agg(["mean", "std"]).reset_index()

    plt.figure(figsize=(11, 6))
    for tool in config.TOOLS:
        sub = means[means["tool"] == tool].set_index("dataset").reindex(datasets).dropna(subset=["mean"])
        if sub.empty:
            continue
        x = [datasets.index(d) for d in sub.index]
        plt.errorbar(x, sub["mean"], yerr=sub["std"].fillna(0), marker="o", linewidth=2, capsize=4,
                     label=config.TOOL_LABELS[tool], color=config.TOOL_COLORS[tool])

    plt.xticks(range(len(datasets)), [config.dataset_label(d) for d in datasets])
    plt.xlabel("Dimensione Dataset (% su scala reale)", fontsize=11, fontweight="bold")
    plt.ylabel("Tempo di esecuzione (Secondi)", fontsize=11, fontweight="bold")
    plt.title(f"Benchmark Execution Time ({env.upper()} Mode) - {job.upper()}", fontsize=13, fontweight="bold", pad=15)
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend(fontsize=10)
    plt.tight_layout()

    folder = os.path.join("logs", env)
    os.makedirs(folder, exist_ok=True)
    output = os.path.join(folder, f"benchmark_{job}.png")
    plt.savefig(output, dpi=300)
    print(f"\n[BENCHMARK EXECUTOR] Esperimento completato! Grafico salvato in: {output}\n")


if __name__ == "__main__":
    main()
