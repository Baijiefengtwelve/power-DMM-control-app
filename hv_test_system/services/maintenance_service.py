from __future__ import annotations

from PyQt5.QtCore import QThread, pyqtSignal
from ..sqlite_maintenance import cleanup_db, db_stats

class MaintenanceWorker(QThread):
    finished_signal = pyqtSignal(dict)
    
    def __init__(self, db_path, keep_days, keep_runs, archive_before_delete, archive_dir, vacuum_mode):
        super().__init__()
        self.db_path = db_path
        self.keep_days = keep_days
        self.keep_runs = keep_runs
        self.archive_before_delete = archive_before_delete
        self.archive_dir = archive_dir
        self.vacuum_mode = vacuum_mode
        self.result = {}

    def run(self):
        try:
            self.result = cleanup_db(
                self.db_path,
                keep_days=self.keep_days,
                keep_runs=self.keep_runs,
                archive_before_delete=self.archive_before_delete,
                archive_dir=self.archive_dir,
                vacuum_mode=self.vacuum_mode,
            )
        except Exception as e:
            self.result = {"ok": False, "message": str(e), "data": None}
        self.finished_signal.emit(self.result)


class MaintenanceService:
    """Encapsulate maintenance helpers such as SQLite cleanup and plot clearing."""

    def __init__(self, mw):
        self.mw = mw

    def get_db_stats(self) -> dict:
        try:
            return db_stats(self.mw.get_sqlite_db_path())
        except Exception as e:
            return {"path": self.mw.get_sqlite_db_path(), "error": str(e)}

    def cleanup_database(self, *, keep_days: int, keep_runs: int, archive_before_delete: bool, archive_dir: str, vacuum_mode: str):
        if getattr(self.mw, "is_recording", False):
            return {"ok": False, "message": "recording in progress; stop recording before cleanup", "data": None}

        try:
            self.mw.sqlite_recorder.stop(timeout_s=2.0)
        except Exception:
            pass

        result = cleanup_db(
            self.mw.get_sqlite_db_path(),
            keep_days=int(keep_days),
            keep_runs=int(keep_runs),
            archive_before_delete=bool(archive_before_delete),
            archive_dir=str(archive_dir),
            vacuum_mode=str(vacuum_mode or "incremental"),
        )

        try:
            self.mw.sqlite_recorder.start()
        except Exception:
            pass
        return result

    def on_db_cleanup_clicked(self):
        mw = self.mw
        if getattr(mw, "is_recording", False):
            mw.log_message("数据库清理失败: recording in progress; stop recording before cleanup")
            return {"ok": False, "message": "recording in progress"}
            
        try:
            keep_days = int(float(mw.db_keep_days_edit.text() or mw.retention_policy.keep_days))
            keep_runs = int(float(mw.db_keep_runs_edit.text() or mw.retention_policy.keep_runs))
            archive_before_delete = bool(mw.db_archive_chk.isChecked())
            archive_dir = str(mw.db_archive_dir_edit.text() or mw.retention_policy.archive_dir)
            vacuum_mode = str(mw.db_vacuum_mode_combo.currentData() or "incremental")
        except Exception:
            keep_days = int(mw.retention_policy.keep_days)
            keep_runs = int(mw.retention_policy.keep_runs)
            archive_before_delete = bool(mw.retention_policy.archive_before_delete)
            archive_dir = str(mw.retention_policy.archive_dir)
            vacuum_mode = str(mw.retention_policy.vacuum_mode)

        mw.log_message("正在后台清理数据库，操作期间请稍候...")
        try:
            mw.sqlite_recorder.stop(timeout_s=2.0)
        except Exception:
            pass

        self._worker = MaintenanceWorker(
            mw.get_sqlite_db_path(), keep_days, keep_runs, archive_before_delete, archive_dir, vacuum_mode
        )
        self._worker.finished_signal.connect(self._on_cleanup_finished)
        self._worker.start()
        
        return {"ok": True, "message": "background task started"}

    def _on_cleanup_finished(self, result):
        mw = self.mw
        try:
            mw.sqlite_recorder.start()
        except Exception:
            pass

        try:
            if result.get("ok"):
                mw.log_message(
                    f"数据库清理完成: 删除 {result.get('data', {}).get('deleted_runs', 0)} 个 run, "
                    f"{result.get('data', {}).get('deleted_rows', 0)} 行"
                )
            else:
                mw.log_message(f"数据库清理失败: {result.get('message')}")
        except Exception:
            pass

        self.update_db_status_label()

    def update_db_status_label(self):
        mw = self.mw
        if not hasattr(mw, "db_status_label"):
            return ""
        stats = self.get_db_stats()
        size_mb = stats.get("size_bytes", 0) / (1024 * 1024) if stats.get("size_bytes") else 0.0
        message = f"SQLite: {size_mb:.1f} MB, runs={stats.get('runs', '-')}, rows={stats.get('rows', '-')}"
        if stats.get("error"):
            message += f" (error: {stats.get('error')})"
        mw.db_status_label.setText(message)
        return message

    def clear_plots(self):
        if hasattr(self.mw, "plot_service"):
            return self.mw.plot_service.clear_plots()
        try:
            self.mw.data_buffer.clear()
            for plot in self.mw.plots.values():
                plot.setData([], [])
            self.mw.log_message("图表数据已清空")
        except Exception as e:
            self.mw.log_message(f"清空图表错误: {e}")
