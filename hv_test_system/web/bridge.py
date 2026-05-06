from __future__ import annotations

import threading
from typing import Any, Dict

from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot, Qt

from ..services import RemoteCommandService

class WebBridge(QObject):
    """
    Thread-safe bridge between FastAPI (background thread) and Qt main thread.

    Web thread:
      - calls submit(action, params) -> waits for result (Future)
      - underlying: emits command_signal (queued to Qt thread)

    Qt thread:
      - handle_command executes actions against MainWindow/services/controllers
      - resolves the Future via pending map
    """

    command_signal = pyqtSignal(object)  # payload: dict
    _result_signal = pyqtSignal(str, object)  # cmd_id, result (dict)

    def __init__(self, main_window):
        super().__init__()
        self.mw = main_window
        self.command_service = getattr(main_window, "remote_command_service", None) or RemoteCommandService(main_window)

        self._lock = threading.Lock()
        self._pending: Dict[str, "concurrent.futures.Future"] = {}

        # Ensure queued execution in Qt thread
        self.command_signal.connect(self.handle_command, type=Qt.QueuedConnection)
        self._result_signal.connect(self._on_result, type=Qt.QueuedConnection)

    def register_future(self, cmd_id: str, fut):
        with self._lock:
            self._pending[cmd_id] = fut

    @pyqtSlot(str, object)
    def _on_result(self, cmd_id: str, result: Any):
        with self._lock:
            fut = self._pending.pop(cmd_id, None)
        if fut is not None and not fut.done():
            fut.set_result(result)

    @pyqtSlot(object)
    def handle_command(self, payload: Dict[str, Any]):
        """
        Runs in Qt thread (queued).
        payload: {"id": str, "action": str, "params": dict}
        """
        cmd_id = str(payload.get("id", ""))
        action = str(payload.get("action", ""))
        params = payload.get("params") or {}
        result = self.command_service.dispatch(action, params)
        self._result_signal.emit(cmd_id, result)

    def _collect_state(self) -> Dict[str, Any]:
        """Collect current state snapshot for the web UI."""
        if hasattr(self.mw, "state_snapshot_service"):
            return self.mw.state_snapshot_service.collect_state()
        return {"flags": {}, "hv": {}, "keithley": {}, "meters": {}, "ui": {}}

    
    def _collect_ui_config(self) -> Dict[str, Any]:
        if hasattr(self.mw, "state_snapshot_service"):
            return self.mw.state_snapshot_service.collect_ui_config()
        return {}

    def _collect_plot(self) -> Dict[str, Any]:
        """Return recent plot arrays (limited) for web chart."""
        if hasattr(self.mw, "state_snapshot_service"):
            return self.mw.state_snapshot_service.collect_plot()
        return {"t": []}
