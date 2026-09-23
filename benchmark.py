#!/usr/bin/env python3
import argparse
import subprocess
import time
import os
import matplotlib.pyplot as plt

def main():
    parser = argparse.ArgumentParser(description="Automated Benchmark Execution Pipeline")
    parser.add_argument("job", type=str, choices=["job_1", "job_2"], help="Job name (matching script prefix)")
    parser.add_argument("master", type=str, choices=["local[*]", "yarn"], help="Master type execution environment")
    parser.add_argument("--fractions", type=str, default="0.01 0.2 0.5 0.7", help="Fractions of dataset to use")
    
    # NUOVO: Aggiungiamo il flag per intercettare se siamo su AWS
    parser.add_argument("--aws", action="store_true", help="Execute using cloud scripts (run_aws.sh)")
    args = parser.parse_args()

    # Se l'utente ha passato il flag --aws, usiamo run_aws.sh, altrimenti il run.sh classico
    script_to_run = "run_aws.sh" if args.aws else "run.sh"
    
    # Determiniamo la cartella finale dei log (se eseguiamo in modalità aws, salviamo in logs/aws/)
    master_dir = "aws" if args.aws else ("local" if "local" in args.master else "yarn")

    # Le tre cartelle reali presenti nel tuo progetto Flight
    tools = ["spark-core", "spark-sql", "hive"]

    # Traduzione delle frazioni decimali nei nomi dei file effettivi su HDFS
    fraction_values = list(map(float, args.fractions.split()))
    fractions = [f"flights_{int(x * 100)}" for x in fraction_values] + ["flights_cleaned"]

    # Dizionario per memorizzare i tempi di esecuzione per i grafici
    execution_data = {tool: [] for tool in tools}
    successful_fractions = {tool: [] for tool in tools}

    for tool in tools:
        for fraction in fractions:
            env_label = "AWS_EMR" if args.aws else args.master
            print(f"[BENCHMARK] Avvio {tool} -> {args.job} su dataset: {fraction} ({env_label})")
            
            start = time.time()
            
            # MODIFICATO: Usa script_to_run (dinamico!) invece del valore fisso
            process = subprocess.run(
                ["bash", script_to_run, args.job, fraction, args.master],
                cwd=tool,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            end = time.time()
            exec_time = end - start

            # Struttura dei log dinamica
            output_path = os.path.join("logs", master_dir, tool, args.job)
            os.makedirs(output_path, exist_ok=True)
            
            with open(os.path.join(output_path, f"stdout-{fraction}.txt"), "wb") as f:
                f.write(process.stdout)

            if process.returncode == 0:
                print(f"[BENCHMARK] Completato {tool}#{args.job} per \"{fraction}\" in {exec_time:.2f} secondi")
                execution_data[tool].append(exec_time)
                successful_fractions[tool].append(fraction)
            else:
                print(f"[⚠️ ERRORE CRITICO] {tool} ha fallito su {fraction}. Controlla stderr-{fraction}.txt")
                with open(os.path.join(output_path, f"stderr-{fraction}.txt"), "wb") as f:
                    f.write(process.stderr)

    # --- GENERAZIONE GRAFICI CON MATPLOTLIB ---
    plt.figure(figsize=(11, 6))

    colors = {
        "hive": "red",
        "spark-core": "green",
        "spark-sql": "blue"
    }

    for tool in tools:
        if execution_data[tool]:
            x_pos = list(range(len(successful_fractions[tool])))
            plt.plot(
                x_pos,
                execution_data[tool],
                marker='o',
                linestyle='-',
                linewidth=2,
                label=tool,
                color=colors.get(tool, "black")
            )

    full_x_labels = [f.replace("flights_", "") + "%" if "cleaned" not in f else "100% (Cleaned)" for f in fractions]
    plt.xticks(list(range(len(fractions))), full_x_labels)
    
    plt.xlabel('Dimensione Dataset (% su scala reale)', fontsize=11, fontweight='bold')
    plt.ylabel('Tempo di esecuzione (Secondi)', fontsize=11, fontweight='bold')
    plt.title(f'Benchmark Execution Time ({master_dir.upper()} Mode) - {args.job.upper()}', fontsize=13, fontweight='bold', pad=15)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend(fontsize=10)
    plt.tight_layout()

    graph_folder = os.path.join("logs", master_dir)
    os.makedirs(graph_folder, exist_ok=True)
    output_graph = os.path.join(graph_folder, f"benchmark_{args.job}.png")
    
    plt.savefig(output_graph, dpi=300)
    print(f"\n[BENCHMARK EXECUTOR] Esperimento completato! Grafico salvato in: {output_graph}\n")

if __name__ == "__main__":
    main()