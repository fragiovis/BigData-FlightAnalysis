"""
background.py — Esecuzione dei batch di job in un thread separato.

Il thread è indipendente dalla sessione Streamlit: si può cambiare pagina (o ricaricarla)
mentre i job girano, e ogni pagina legge lo stato aggiornato dall'istanza condivisa.
"""

import subprocess
import sys
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime

from bench import config, runner


@dataclass
class BatchState:
    plan: list = field(default_factory=list)
    batch_id: str = ""
    env: str = ""
    current: int = -1
    current_label: str = ""
    log: deque = field(default_factory=lambda: deque(maxlen=400))
    records: list = field(default_factory=list)
    verifications: list = field(default_factory=list)
    running: bool = False
    stop_requested: bool = False
    error: str = ""
    started_at: datetime | None = None
    finished_at: datetime | None = None


class BatchManager:
    def __init__(self):
        self.state = BatchState()
        self._lock = threading.Lock()

    def start(self, plan, env, verify):
        """plan: lista di (tool, job, dataset, ripetizione)."""
        with self._lock:
            if self.state.running:
                raise runner.RunnerBusy("Un batch è già in esecuzione")
            now = datetime.now()
            self.state = BatchState(plan=plan, env=env, running=True, started_at=now,
                                    batch_id=f"batch-{now:%Y%m%d-%H%M%S}-{env}")
        threading.Thread(target=self._run, args=(verify,), daemon=True).start()

    def stop(self):
        self.state.stop_requested = True

    def _run(self, verify):
        s = self.state
        try:
            with runner.exclusive_lock():
                for i, (tool, job, dataset, rep) in enumerate(s.plan):
                    if s.stop_requested:
                        break
                    s.current = i
                    s.current_label = f"{config.TOOL_LABELS[tool]} · {job} · {dataset} · ripetizione {rep}"
                    s.log.clear()
                    record = runner.run_job(
                        tool, job, dataset, s.env, repetition=rep, batch_id=s.batch_id,
                        on_line=s.log.append, should_stop=lambda: s.stop_requested,
                    )
                    s.records.append(record)

                    # Verifica quando le tre tecnologie hanno appena prodotto lo stesso (job, dataset)
                    if verify and not s.stop_requested and self._group_complete(i):
                        s.verifications.append(self._verify(job, dataset))
        except Exception as e:  # noqa: BLE001 — l'errore viene mostrato nella pagina
            s.error = f"{type(e).__name__}: {e}"
        finally:
            s.running = False
            s.finished_at = datetime.now()

    def _group_complete(self, i):
        """True se il passo i è l'ultimo del gruppo (job, dataset) e il gruppo copre tutte le tecnologie."""
        tool, job, dataset, rep = self.state.plan[i]
        nxt = self.state.plan[i + 1] if i + 1 < len(self.state.plan) else None
        if nxt and (nxt[1], nxt[2]) == (job, dataset):
            return False
        tools = {p[0] for p in self.state.plan if (p[1], p[2]) == (job, dataset)}
        return tools == set(config.TOOLS)

    def _verify(self, job, dataset):
        cmd = [sys.executable, str(config.ROOT_DIR / "verify_outputs.py"), job,
               "--base", config.hdfs_base(self.state.env)]
        r = subprocess.run(cmd, env=config.job_env(self.state.env), stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, text=True)
        return {"job": job, "dataset": dataset, "ok": r.returncode == 0, "output": r.stdout}


def build_plan(tools, jobs, datasets, repetitions):
    """Ordine: dataset → job → tecnologia → ripetizione, così la verifica avviene sugli output appena scritti."""
    return [
        (tool, job, dataset, rep)
        for dataset in datasets
        for job in jobs
        for rep in range(1, repetitions + 1)
        for tool in tools
    ]
