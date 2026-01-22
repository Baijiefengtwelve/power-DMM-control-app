from __future__ import annotations

import csv
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class RetentionPolicy:
    keep_days: int = 30
    keep_runs: int = 200
    archive_before_delete: bool = True
    archive_dir: str = os.path.join("data", "archive")
    vacuum_mode: str = "incremental"  # incremental | vacuum


def load_retention_from_config(config) -> RetentionPolicy:
    def _get(section: str, key: str, default: str) -> str:
        try:
            if config.has_option(section, key):
                return str(config.get(section, key))
        except Exception:
            pass
        return default

    enabled = _get("Retention", "enabled", "true").strip().lower() not in ("0", "false", "no")
    if not enabled:
        # disabled retention => keep everything
        return RetentionPolicy(keep_days=10_000, keep_runs=10_000, archive_before_delete=False)

    return RetentionPolicy(
        keep_days=int(float(_get("Retention", "keep_days", "30")) or 30),
        keep_runs=int(float(_get("Retention", "keep_runs", "200")) or 200),
        archive_before_delete=_get("Retention", "archive_before_delete", "true").strip().lower() not in ("0", "false", "no"),
        archive_dir=_get("Retention", "archive_dir", os.path.join("data", "archive")),
        vacuum_mode=_get("Retention", "vacuum_mode", "incremental").strip().lower(),
    )


def db_stats(db_path: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "path": db_path,
        "exists": os.path.isfile(db_path),
        "size_bytes": int(os.path.getsize(db_path)) if os.path.isfile(db_path) else 0,
    }
    if not os.path.isfile(db_path):
        return out
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(1) FROM runs")
        out["runs"] = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(1) FROM data")
        out["rows"] = int(cur.fetchone()[0])
        cur.execute("SELECT MIN(ts_ms), MAX(ts_ms) FROM data")
        mn, mx = cur.fetchone()
        out["min_ts_ms"] = int(mn) if mn is not None else 0
        out["max_ts_ms"] = int(mx) if mx is not None else 0
        conn.close()
    except Exception as e:
        out["error"] = str(e)
    return out


def _select_runs_to_delete(conn: sqlite3.Connection, *, keep_days: int, keep_runs: int) -> List[str]:
    cur = conn.cursor()
    delete_ids: set[str] = set()

    # By runs count: delete older than the newest keep_runs
    if keep_runs >= 0:
        cur.execute("SELECT run_id FROM runs ORDER BY start_ms DESC")
        all_runs = [r[0] for r in cur.fetchall()]
        if len(all_runs) > keep_runs:
            delete_ids.update(all_runs[keep_runs:])

    # By days: delete runs whose start_ms < cutoff
    if keep_days >= 0:
        cutoff_ms = int((time.time() - keep_days * 86400) * 1000)
        cur.execute("SELECT run_id FROM runs WHERE start_ms < ?", (cutoff_ms,))
        delete_ids.update([r[0] for r in cur.fetchall()])

    return sorted(delete_ids)


def _archive_runs_to_csv(conn: sqlite3.Connection, run_ids: List[str], archive_dir: str) -> Tuple[int, int]:
    """Archive selected runs to CSV files.

    Returns: (files_written, rows_written)
    """
    if not run_ids:
        return 0, 0
    os.makedirs(archive_dir, exist_ok=True)
    files_written = 0
    rows_written = 0
    cur = conn.cursor()

    # Table columns (wide schema)
    headers = [
        "run_id",
        "ts_ms",
        "time_text",
        "hv_voltage",
        "cathode",
        "gate",
        "anode",
        "backup",
        "vacuum",
        "keithley_voltage",
        "gate_plus_anode",
        "anode_cathode_ratio",
    ]

    for rid in run_ids:
        fn = f"run_{rid}.csv"
        fp = os.path.join(archive_dir, fn)
        with open(fp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(headers)
            cur.execute(
                """
                SELECT run_id, ts_ms, time_text, hv_voltage, cathode, gate, anode, backup, vacuum,
                       keithley_voltage, gate_plus_anode, anode_cathode_ratio
                FROM data WHERE run_id=? ORDER BY ts_ms ASC
                """,
                (rid,),
            )
            for row in cur.fetchall():
                w.writerow(row)
                rows_written += 1
        files_written += 1

    return files_written, rows_written


def cleanup_db(
    db_path: str,
    *,
    keep_days: int,
    keep_runs: int,
    archive_before_delete: bool,
    archive_dir: str,
    vacuum_mode: str = "incremental",
) -> Dict[str, Any]:
    """Cleanup old runs and reclaim space.

    - Determines runs to delete by both keep_days and keep_runs (union)
    - Optionally archives those runs to CSV
    - Deletes data and runs rows inside a transaction
    - Performs WAL checkpoint and vacuum (incremental or full)
    """
    if not os.path.isfile(db_path):
        return {"ok": True, "message": "db not found", "data": {"deleted_runs": 0, "deleted_rows": 0}}

    t0 = time.time()
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")

    cur = conn.cursor()
    run_ids = _select_runs_to_delete(conn, keep_days=keep_days, keep_runs=keep_runs)
    if not run_ids:
        conn.close()
        return {"ok": True, "message": "nothing to delete", "data": {"deleted_runs": 0, "deleted_rows": 0}}

    archived_files = 0
    archived_rows = 0
    if archive_before_delete:
        try:
            archived_files, archived_rows = _archive_runs_to_csv(conn, run_ids, archive_dir)
        except Exception as e:
            conn.close()
            return {"ok": False, "message": f"archive failed: {e}", "data": None}

    # Delete inside a transaction
    deleted_rows = 0
    deleted_runs = 0
    try:
        conn.execute("BEGIN;")
        # delete data rows
        q_marks = ",".join(["?"] * len(run_ids))
        cur.execute(f"SELECT COUNT(1) FROM data WHERE run_id IN ({q_marks})", tuple(run_ids))
        deleted_rows = int(cur.fetchone()[0])
        cur.execute(f"DELETE FROM data WHERE run_id IN ({q_marks})", tuple(run_ids))
        cur.execute(f"DELETE FROM runs WHERE run_id IN ({q_marks})", tuple(run_ids))
        deleted_runs = len(run_ids)
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return {"ok": False, "message": f"delete failed: {e}", "data": None}

    # Reclaim space / truncate WAL
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    except Exception:
        pass

    vacuum_mode = (vacuum_mode or "incremental").strip().lower()
    try:
        if vacuum_mode == "vacuum":
            conn.execute("VACUUM;")
        else:
            # requires auto_vacuum=INCREMENTAL to be effective
            conn.execute("PRAGMA incremental_vacuum;")
    except Exception:
        pass

    conn.close()
    dt = time.time() - t0

    return {
        "ok": True,
        "message": "OK",
        "data": {
            "deleted_runs": deleted_runs,
            "deleted_rows": deleted_rows,
            "archived_files": archived_files,
            "archived_rows": archived_rows,
            "archive_dir": archive_dir,
            "vacuum_mode": vacuum_mode,
            "elapsed_s": round(dt, 3),
        },
    }
