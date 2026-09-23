#!/usr/bin/env python3
"""
estrai_da_grafici.py — Ricostruisce i tempi delle esecuzioni su AWS EMR della versione precedente
del progetto, di cui sono rimasti solo i grafici PNG e i log testuali in logs/aws/.

- Tempo totale: stimato dai grafici logs/aws/benchmark_job_*.png. L'asse Y viene tarato sulle
  linee della griglia (che cadono sulle tacche note), poi si individua il centro di ogni marcatore
  colorato sulla verticale di ciascun dataset. Precisione: circa 0,02–0,03 s per pixel.
- Tempo di calcolo parziale di Spark: letto dai log ("Calcolo completato in X secondi"), misura
  solo la pipeline fino all'anteprima (senza avvio e scrittura su HDFS). Hive non lo stampava.
- Dimensione dell'input: quella dei file di allora (codici delle cause a una lettera), ricavata
  dai file attuali sostituendo i codici estesi.

Uso (dalla radice del progetto):  python3 results/legacy/estrai_da_grafici.py
"""

import csv
import re
from pathlib import Path

import numpy as np
from matplotlib.image import imread

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = Path(__file__).resolve().parent / "aws_emr_versione_precedente.csv"

TICKS = {"job_1": [35, 40, 45, 50], "job_2": [30, 35, 40, 45, 50, 55, 60]}
COLORS = {"spark-core": (0, 128, 0), "spark-sql": (0, 0, 255), "hive": (255, 0, 0)}
DATASETS = ["flights_1", "flights_20", "flights_50", "flights_70", "flights_cleaned"]
PERCENT = {"flights_1": 1, "flights_20": 20, "flights_50": 50, "flights_70": 70, "flights_cleaned": 100}
OLD_CODES = {"DELAY_CARRIER": "C", "DELAY_WEATHER": "W", "DELAY_NAS": "N", "DELAY_SECURITY": "S",
             "DELAY_LATE_AIRCRAFT": "L", "CANC_CARRIER": "A", "CANC_WEATHER": "B", "CANC_NAS": "C",
             "CANC_SECURITY": "D"}
MARKER_PX = 29  # diametro dei marcatori a 300 dpi


def gridlines(img, axis):
    gray = (np.abs(img[..., 0] - img[..., 1]) < 6) & (np.abs(img[..., 1] - img[..., 2]) < 6) \
        & (img[..., 0] > 190) & (img[..., 0] < 235)
    counts = gray.sum(axis=axis)
    idx = np.where(counts > counts.max() * 0.5)[0]
    groups = np.split(idx, np.where(np.diff(idx) > 3)[0] + 1)
    # Le linee della griglia sono spesse 3 px; i gruppi da 1 px sono i bordi antialias della cornice
    return [float(g.mean()) for g in groups if len(g) >= 2]


def color_mask(img, rgb):
    mask = np.all(np.abs(img - np.array(rgb)) < 40, axis=2)
    mask[:400, :650] = False  # legenda in alto a sinistra
    return mask


def read_chart(job):
    img = imread(ROOT / "logs" / "aws" / f"benchmark_{job}.png")[..., :3] * 255
    rows, cols = gridlines(img, axis=1), gridlines(img, axis=0)
    ticks = sorted(TICKS[job], reverse=True)
    assert len(rows) == len(ticks) and len(cols) == len(DATASETS), "griglia non riconosciuta"
    slope, intercept = np.polyfit(rows, ticks, 1)

    masks = {tool: color_mask(img, rgb) for tool, rgb in COLORS.items()}
    points = {}
    for tool, mask in masks.items():
        others = np.any([m for t, m in masks.items() if t != tool], axis=0)
        for ds, c in zip(DATASETS, cols):
            band = slice(int(c) - 3, int(c) + 4)
            ys = np.where(mask[:, band].any(axis=1))[0]
            groups = np.split(ys, np.where(np.diff(ys) > 2)[0] + 1)
            marker = max(groups, key=len)
            if len(marker) >= MARKER_PX:
                center = (marker.min() + marker.max()) / 2
            elif others[marker.min() - 1, band].any():
                # Coperto in alto da un altro marcatore: si usa il bordo inferiore
                center = marker.max() - (MARKER_PX - 1) / 2
            else:
                center = marker.min() + (MARKER_PX - 1) / 2
            points[(tool, ds)] = (round(slope * center + intercept, 2), len(marker) < MARKER_PX)
    return points, abs(slope)


def spark_partial_seconds(tool, job, ds):
    path = ROOT / "logs" / "aws" / tool / job / f"stdout-{ds}.txt"
    m = re.search(r"completato in ([\d.]+) secondi", path.read_text()) if path.exists() else None
    return float(m.group(1)) if m else None


def old_input_bytes(ds):
    path = ROOT / "data" / "processed" / f"{ds}.csv"
    if not path.exists():
        return None
    saved = 0
    with open(path) as f:
        next(f)
        for line in f:
            for value in line.rstrip("\n").split(",")[7:9]:
                if value in OLD_CODES:
                    saved += len(value) - len(OLD_CODES[value])
    return path.stat().st_size - saved


def main():
    sizes = {ds: old_input_bytes(ds) for ds in DATASETS}
    rows = []
    for job in TICKS:
        points, s_per_px = read_chart(job)
        for (tool, ds), (seconds, occluded) in points.items():
            rows.append({
                "env": "aws_legacy", "tool": tool, "job": job, "dataset": ds, "dataset_percent": PERCENT[ds],
                "wall_seconds": seconds, "precisione_s": round(s_per_px * (3 if occluded else 1), 3),
                "marcatore_parzialmente_coperto": occluded,
                "spark_calcolo_parziale_s": spark_partial_seconds(tool, job, ds) if tool != "hive" else None,
                "input_bytes": sizes[ds], "ripetizioni": 1,
                "cluster": "AWS EMR, 1 primary + 2 core m5.xlarge",
                "versione_codice": "precedente alle correzioni (commit 7ea55a0)",
                "fonte": f"stima dal grafico logs/aws/benchmark_{job}.png",
            })
    with open(OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} punti salvati in {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
