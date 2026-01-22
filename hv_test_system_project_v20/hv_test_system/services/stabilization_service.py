from __future__ import annotations

from PyQt5.QtCore import QObject, pyqtSignal


class StabilizationService(QObject):
    """Orchestrates current stabilization (PID) threads.

    Keeps the existing CurrentStabilizationThread/PIDController behavior but
    provides a thin facade so MainWindow does not directly manage thread
    lifecycles.
    """

    log = pyqtSignal(str)
    started = pyqtSignal()
    stopped = pyqtSignal()

    def __init__(self, mw, parent=None):
        super().__init__(parent)
        self.mw = mw

    def start(self):
        # delegate to existing MainWindow method if present, but prefer inline
        try:
            if getattr(self.mw, 'stabilization_thread', None) and self.mw.stabilization_thread.isRunning():
                self.log.emit("稳流已在运行")
                return
            # MainWindow already builds threads in its original code path; call the existing helper.
            if hasattr(self.mw, '_start_current_stabilization_impl'):
                self.mw._start_current_stabilization_impl()
            else:
                # fallback: call the old public method
                self.mw.start_current_stabilization()
            self.started.emit()
        except Exception as e:
            self.log.emit(f"启动稳流失败: {e}")

    def stop(self):
        try:
            if hasattr(self.mw, 'stop_current_stabilization'):
                self.mw.stop_current_stabilization()
            self.stopped.emit()
        except Exception as e:
            self.log.emit(f"停止稳流失败: {e}")
