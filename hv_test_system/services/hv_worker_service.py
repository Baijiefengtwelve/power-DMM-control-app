from __future__ import annotations


class HVWorkerService:
    """Own worker-signal attach/detach and worker log callbacks for HAPS06."""

    def __init__(self, mw):
        self.mw = mw

    def attach_hv_worker_signals(self):
        mw = self.mw
        try:
            worker = getattr(mw.hv_controller, "_worker", None)
            if not worker:
                return
            try:
                worker.io_error.disconnect(self.on_hv_worker_error)
            except Exception:
                pass
            try:
                worker.connected.disconnect(self.on_hv_worker_connected)
            except Exception:
                pass
            try:
                worker.disconnected.disconnect(self.on_hv_worker_disconnected)
            except Exception:
                pass

            worker.io_error.connect(self.on_hv_worker_error)
            worker.connected.connect(self.on_hv_worker_connected)
            worker.disconnected.connect(self.on_hv_worker_disconnected)
        except Exception as exc:
            mw.log_message(f"绑定高压源 worker 信号失败: {exc}")

    def detach_hv_worker_signals(self):
        worker = getattr(self.mw.hv_controller, "_worker", None)
        if not worker:
            return
        try:
            worker.io_error.disconnect(self.on_hv_worker_error)
        except Exception:
            pass
        try:
            worker.connected.disconnect(self.on_hv_worker_connected)
        except Exception:
            pass
        try:
            worker.disconnected.disconnect(self.on_hv_worker_disconnected)
        except Exception:
            pass

    def on_hv_worker_error(self, msg: str):
        self.mw.log_message(f"[HAPS06] {msg}")

    def on_hv_worker_connected(self, port: str):
        self.mw.log_message(f"[HAPS06] 串口已连接: {port}")

    def on_hv_worker_disconnected(self):
        self.mw.log_message("[HAPS06] 串口已断开")
