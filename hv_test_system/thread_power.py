from __future__ import annotations

from .common import *
from .power_protocols import (
    ActualVoltageReadableControllerProtocol,
    SerialRemotePowerControllerProtocol,
)

class HVVoltagePoller(QThread):
    """后台轮询HAPS06实际电压（UI实时更新，但不阻塞主线程）"""

    voltage_updated = pyqtSignal(float)
    poll_error = pyqtSignal(str)

    def __init__(self, hv_controller: ActualVoltageReadableControllerProtocol, interval_ms: int = 500, parent=None):
        super().__init__(parent)
        self.hv_controller = hv_controller
        self.interval_ms = max(100, int(interval_ms))
        self._running = True
        self._qt_signals_suppressed = False
        self._fail_count = 0
        self._last_error_emit = 0.0

    def suppress_qt_signals(self):
        self._qt_signals_suppressed = True

    def _safe_emit(self, signal, *args):
        if self._qt_signals_suppressed:
            return
        try:
            signal.emit(*args)
        except Exception:
            pass

    def stop(self):
        self._running = False
        # 最多等待1.5s，避免关闭程序时卡住
        self.wait(1500)

    def run(self):
        while self._running:
            try:
                if self.hv_controller and getattr(self.hv_controller, "is_connected", False):
                    v = self.hv_controller.read_actual_voltage()
                    if v is not None:
                        self._fail_count = 0
                        self._safe_emit(self.voltage_updated, float(v))
                    else:
                        self._fail_count += 1
                        now = time.time()
                        # 每10秒最多提示一次，并且连续失败>=5次才提示
                        if (now - self._last_error_emit) > 10.0 and self._fail_count >= 5:
                            self._last_error_emit = now
                            self._safe_emit(self.poll_error, "HAPS06实际电压读取失败（无响应或CRC错误）")
                else:
                    # 未连接时不刷错误
                    self._fail_count = 0
            except Exception as e:
                now = time.time()
                if (now - self._last_error_emit) > 10.0:
                    self._last_error_emit = now
                    self._safe_emit(self.poll_error, f"HAPS06轮询异常: {e}")

            self.msleep(self.interval_ms)

class KeithleyVoltagePoller(QThread):
    """后台轮询一个或多个 Keithley 电源电压，避免 UI 主线程被 GPIB 查询阻塞。"""

    voltage_updated = pyqtSignal(float)
    named_voltage_updated = pyqtSignal(str, float)
    poll_error = pyqtSignal(str)

    def __init__(self, controllers_getter, interval_ms: int = 2500, parent=None):
        super().__init__(parent)
        self.controllers_getter = controllers_getter
        self.interval_ms = max(600, int(interval_ms))
        self._running = True
        self._qt_signals_suppressed = False
        self._last_error_emit = 0.0

    def suppress_qt_signals(self):
        self._qt_signals_suppressed = True

    def _safe_emit(self, signal, *args):
        if self._qt_signals_suppressed:
            return
        try:
            signal.emit(*args)
        except Exception:
            pass

    def stop(self):
        self._running = False
        self.wait(1500)

    def _emit_error(self, message: str):
        now = time.time()
        if (now - self._last_error_emit) > 10.0:
            self._last_error_emit = now
            self._safe_emit(self.poll_error, str(message))

    def run(self):
        while self._running:
            try:
                controllers = self.controllers_getter() if callable(self.controllers_getter) else None
                emitted_legacy = False
                if isinstance(controllers, dict):
                    for name, controller in list(controllers.items()):
                        if not self._running:
                            break
                        if controller is None or not bool(getattr(controller, 'is_connected', False)):
                            continue
                        try:
                            v = controller.read_voltage()
                            if v is None:
                                continue
                            fv = float(v)
                            self._safe_emit(self.named_voltage_updated, str(name), fv)
                            if not emitted_legacy:
                                emitted_legacy = True
                                self._safe_emit(self.voltage_updated, fv)
                        except Exception as e:
                            self._emit_error(f"Keithley 电压轮询异常({name}): {e}")
                        self.msleep(60)
                elif controllers is not None and bool(getattr(controllers, 'is_connected', False)):
                    v = controllers.read_voltage()
                    if v is not None:
                        fv = float(v)
                        self._safe_emit(self.voltage_updated, fv)
                # 未连接时静默
            except Exception as e:
                self._emit_error(f"Keithley 电压轮询异常: {e}")

            self.msleep(self.interval_ms)


class HVConnectThread(QThread):
    """后台连接HAPS06，避免UI线程因串口/通讯超时而卡死。

    - 连接串口 + 探测地址（由 HAPS06Controller.connect_serial 完成）
    - 启用远控（可设置较短超时）

    finished: (success, message, port)
    """

    progress = pyqtSignal(str)
    finished = pyqtSignal(bool, str, str)

    def __init__(self, hv_controller: SerialRemotePowerControllerProtocol, port: str, baudrate: int = 9600, remote_timeout_s: float = 1.5, parent=None):
        super().__init__(parent)
        self.hv_controller = hv_controller
        self.port = str(port)
        self.baudrate = int(baudrate)
        self.remote_timeout_s = float(remote_timeout_s)

    def run(self):
        try:
            self.progress.emit(f"连接串口: {self.port} @ {self.baudrate}...")
            ok, msg = self.hv_controller.connect_serial(self.port, self.baudrate)
            if not ok:
                self.finished.emit(False, str(msg), self.port)
                return

            self.progress.emit("启用远程控制(远控)...")
            ok2, msg2 = self.hv_controller.enable_remote_control(timeout_s=self.remote_timeout_s)
            if not ok2:
                try:
                    self.hv_controller.disconnect()
                except Exception:
                    pass
                self.finished.emit(False, f"启用远控失败: {msg2}", self.port)
                return

            self.finished.emit(True, "连接成功并已启用远控", self.port)
        except Exception as e:
            try:
                self.hv_controller.disconnect()
            except Exception:
                pass
            self.finished.emit(False, f"连接异常: {e}", self.port)

