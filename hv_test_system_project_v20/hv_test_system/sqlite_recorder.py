from __future__ import annotations

import json
import os
import queue
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class SQLiteRecorderConfig:
    path: str
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"
    auto_vacuum: str = "INCREMENTAL"  # NONE|FULL|INCREMENTAL
    commit_every_rows: int = 200
    commit_every_ms: int = 500


class SQLiteRecorder:
    """Background SQLite writer for acquisition data.

    Design goals:
      - decouple acquisition from disk I/O (queue + background thread)
      - crash safety: committed batches persist; worst-case loss is < commit window
      - simple schema aligned with Excel rows (wide table)
    """

    def __init__(self, cfg: SQLiteRecorderConfig):
        self.cfg = cfg
        self._q: "queue.Queue[dict]" = queue.Queue(maxsize=20000)
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

        self._conn: Optional[sqlite3.Connection] = None
        self._run_id: str = ""
        self._run_start_ms: int = 0

        # Diagnostics
        self.total_enqueued = 0
        self.total_inserted = 0
        self.last_error: str = ""
        self.last_commit_ts: float = 0.0

    @classmethod
    def from_config(cls, config) -> "SQLiteRecorder":
        def _get(section: str, key: str, default: str) -> str:
            try:
                if config.has_option(section, key):
                    return str(config.get(section, key))
            except Exception:
                pass
            return default

        path = _get("SQLite", "path", os.path.join("data", "session.sqlite"))
        cfg = SQLiteRecorderConfig(
            path=path,
            journal_mode=_get("SQLite", "journal_mode", "WAL"),
            synchronous=_get("SQLite", "synchronous", "NORMAL"),
            auto_vacuum=_get("SQLite", "auto_vacuum", "INCREMENTAL"),
            commit_every_rows=int(float(_get("SQLite", "commit_every_rows", "200")) or 200),
            commit_every_ms=int(float(_get("SQLite", "commit_every_ms", "500")) or 500),
        )
        return cls(cfg)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="SQLiteRecorder", daemon=True)
        self._thread.start()

    def stop(self, *, timeout_s: float = 5.0):
        self._stop.set()
        try:
            self._q.put_nowait({"cmd": "stop"})
        except Exception:
            pass
        if self._thread:
            self._thread.join(timeout=timeout_s)

    def status(self) -> Dict[str, Any]:
        return {
            "path": self.cfg.path,
            "run_id": self._run_id,
            "queue_size": int(getattr(self._q, "qsize", lambda: 0)()),
            "total_enqueued": int(self.total_enqueued),
            "total_inserted": int(self.total_inserted),
            "last_commit_ts": float(self.last_commit_ts),
            "last_error": str(self.last_error or ""),
            "thread_alive": bool(self._thread and self._thread.is_alive()),
        }

    def start_run(self, run_id: str, *, params: Optional[Dict[str, Any]] = None):
        """Open a new logical run (session).

        This records run metadata to runs table. Data rows written while run_id active.
        """
        self._run_id = str(run_id)
        self._run_start_ms = int(time.time() * 1000)
        payload = {
            "cmd": "start_run",
            "run_id": self._run_id,
            "start_ms": self._run_start_ms,
            "params_json": json.dumps(params or {}, ensure_ascii=False),
        }
        self._enqueue(payload)

    def stop_run(self):
        if not self._run_id:
            return
        payload = {
            "cmd": "stop_run",
            "run_id": self._run_id,
            "end_ms": int(time.time() * 1000),
        }
        self._enqueue(payload)
        self._run_id = ""
        self._run_start_ms = 0

    def enqueue_row(self, *, ts_ms: int, row: Dict[str, Any]):
        """Enqueue a single data row.

        The row is expected to contain keys matching columns in `data` table.
        """
        if not self._run_id:
            # If run not started, ignore (safer than creating implicit run)
            return
        payload = {
            "cmd": "row",
            "run_id": self._run_id,
            "ts_ms": int(ts_ms),
            "row": dict(row),
        }
        self._enqueue(payload)

    def _enqueue(self, payload: Dict[str, Any]):
        try:
            self._q.put_nowait(payload)
            self.total_enqueued += 1
        except queue.Full:
            # Drop oldest behavior is tricky; we opt to drop newest to protect acquisition.
            self.last_error = "sqlite queue full, dropping"

    # ---------------- internal ----------------
    def _open(self) -> sqlite3.Connection:
        os.makedirs(os.path.dirname(self.cfg.path) or ".", exist_ok=True)
        # If the main DB file was deleted manually while WAL/SHM remain, SQLite may replay old data.
        # Purge orphaned sidecar files to ensure a truly fresh database on next start.
        if not os.path.exists(self.cfg.path):
            for suffix in ("-wal", "-shm", "-journal"):
                sidecar = self.cfg.path + suffix
                if os.path.isfile(sidecar):
                    try:
                        os.remove(sidecar)
                    except Exception:
                        pass
        conn = sqlite3.connect(self.cfg.path, check_same_thread=False)
        conn.execute("PRAGMA foreign_keys=ON;")
        try:
            conn.execute(f"PRAGMA journal_mode={self.cfg.journal_mode};")
        except Exception:
            pass
        try:
            conn.execute(f"PRAGMA synchronous={self.cfg.synchronous};")
        except Exception:
            pass
        try:
            conn.execute(f"PRAGMA auto_vacuum={self.cfg.auto_vacuum};")
        except Exception:
            pass
        self._init_schema(conn)
        return conn

    @staticmethod
    def _init_schema(conn: sqlite3.Connection):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                start_ms INTEGER NOT NULL,
                end_ms INTEGER,
                params_json TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                ts_ms INTEGER NOT NULL,
                time_text TEXT,
                hv_voltage REAL,
                cathode REAL,
                gate REAL,
                anode REAL,
                backup REAL,
                vacuum REAL,
                keithley_voltage REAL,
                gate_plus_anode REAL,
                anode_cathode_ratio REAL,
                FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_data_run_ts ON data(run_id, ts_ms);")
        conn.commit()

    def _run(self):
        batch = []
        last_commit = time.time()
        try:
            self._conn = self._open()
        except Exception as e:
            self.last_error = f"open sqlite failed: {e}"
            return

        while not self._stop.is_set() or not self._q.empty():
            try:
                item = self._q.get(timeout=0.2)
            except queue.Empty:
                item = None

            if item is None:
                # commit on time
                if batch and (time.time() - last_commit) * 1000 >= self.cfg.commit_every_ms:
                    self._flush_batch(batch)
                    batch.clear()
                    last_commit = time.time()
                continue

            cmd = item.get("cmd")
            if cmd == "stop":
                break
            if cmd == "start_run":
                try:
                    self._conn.execute(
                        "INSERT OR REPLACE INTO runs(run_id, start_ms, end_ms, params_json) VALUES (?, ?, NULL, ?)",
                        (item.get("run_id"), int(item.get("start_ms")), item.get("params_json")),
                    )
                    self._conn.commit()
                except Exception as e:
                    self.last_error = f"start_run failed: {e}"
                continue
            if cmd == "stop_run":
                try:
                    self._conn.execute(
                        "UPDATE runs SET end_ms=? WHERE run_id=?",
                        (int(item.get("end_ms")), item.get("run_id")),
                    )
                    self._conn.commit()
                except Exception as e:
                    self.last_error = f"stop_run failed: {e}"
                continue
            if cmd == "row":
                batch.append(item)
                if len(batch) >= self.cfg.commit_every_rows:
                    self._flush_batch(batch)
                    batch.clear()
                    last_commit = time.time()
                continue

        # final flush
        if batch:
            self._flush_batch(batch)
        try:
            if self._conn:
                self._conn.commit()
                self._conn.close()
        except Exception:
            pass

    def _flush_batch(self, batch_items):
        if not batch_items or not self._conn:
            return
        try:
            rows = []
            for it in batch_items:
                row = it.get("row") or {}
                rows.append(
                    (
                        it.get("run_id"),
                        int(it.get("ts_ms")),
                        row.get("time_text"),
                        row.get("hv_voltage"),
                        row.get("cathode"),
                        row.get("gate"),
                        row.get("anode"),
                        row.get("backup"),
                        row.get("vacuum"),
                        row.get("keithley_voltage"),
                        row.get("gate_plus_anode"),
                        row.get("anode_cathode_ratio"),
                    )
                )
            self._conn.executemany(
                """
                INSERT INTO data(
                    run_id, ts_ms, time_text, hv_voltage, cathode, gate, anode, backup, vacuum,
                    keithley_voltage, gate_plus_anode, anode_cathode_ratio
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self._conn.commit()
            self.total_inserted += len(rows)
            self.last_commit_ts = time.time()
            self.last_error = ""
        except Exception as e:
            self.last_error = f"insert failed: {e}"