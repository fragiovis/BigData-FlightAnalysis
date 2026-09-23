#!/usr/bin/env python3
"""
verify_outputs.py — Confronta gli output dello stesso job prodotti da Spark Core, Spark SQL e Hive.

Legge i risultati da HDFS (/user/<utente>/<tecnologia>/<job>), li indicizza per chiave
(le prime due colonne) e confronta campo per campo: i valori numerici con una tolleranza
pari all'arrotondamento a 2 decimali, le stringhe in modo esatto.

Uso:  python3 verify_outputs.py job_1 [--base /user/hadoop]
"""

import argparse
import csv
import getpass
import io
import subprocess
import sys

TOOLS = ["spark-core", "spark-sql", "hive"]
KEY_SIZE = 2
TOLERANCE = 0.011
MAX_EXAMPLES = 5


def read_output(path):
    result = subprocess.run(
        ["hdfs", "dfs", "-cat", f"{path}/*"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    if result.returncode != 0:
        sys.exit(f"[VERIFY] Impossibile leggere {path}: {result.stderr.strip()}")
    rows = [r for r in csv.reader(io.StringIO(result.stdout)) if r]

    # Spark Core e Spark SQL scrivono l'header, Hive no
    if rows and not is_number(rows[0][KEY_SIZE]):
        rows = rows[1:]
    return {tuple(r[:KEY_SIZE]): r for r in rows}


def is_number(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def same_value(a, b):
    if is_number(a) and is_number(b):
        return abs(float(a) - float(b)) <= TOLERANCE
    return a == b


def compare(name_a, rows_a, name_b, rows_b):
    errors = []
    for key in sorted(rows_a.keys() - rows_b.keys()):
        errors.append(f"chiave {key} presente solo in {name_a}")
    for key in sorted(rows_b.keys() - rows_a.keys()):
        errors.append(f"chiave {key} presente solo in {name_b}")

    for key in sorted(rows_a.keys() & rows_b.keys()):
        a, b = rows_a[key], rows_b[key]
        if len(a) != len(b):
            errors.append(f"{key}: numero di colonne diverso ({len(a)} vs {len(b)})")
            continue
        for i, (va, vb) in enumerate(zip(a, b)):
            if not same_value(va, vb):
                errors.append(f"{key} colonna {i}: {name_a}={va!r} {name_b}={vb!r}")

    status = "OK" if not errors else f"{len(errors)} differenze"
    print(f"  {name_a} vs {name_b}: {len(rows_a)} vs {len(rows_b)} righe -> {status}")
    for e in errors[:MAX_EXAMPLES]:
        print(f"      {e}")
    return not errors


def main():
    parser = argparse.ArgumentParser(description="Confronto degli output tra tecnologie")
    parser.add_argument("job", choices=["job_1", "job_2"])
    parser.add_argument("--base", default=f"/user/{getpass.getuser()}", help="Radice HDFS degli output")
    args = parser.parse_args()

    outputs = {tool: read_output(f"{args.base}/{tool}/{args.job}") for tool in TOOLS}

    print(f"[VERIFY] {args.job}")
    ok = True
    for other in TOOLS[1:]:
        ok &= compare(TOOLS[0], outputs[TOOLS[0]], other, outputs[other])
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
