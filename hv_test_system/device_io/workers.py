from __future__ import annotations

import time
import traceback
from dataclasses import dataclass
from queue import Queue, Empty
from typing import Any, Callable, Optional

from PyQt5.QtCore import QThread, pyqtSignal


@dataclass
class _Call:
    fn: Callable[[Any], Any]
    reply_q: Queue
    timeout_s: float


class SerialIOWorker(QThread):
    """Exclusive serial-port I/O worker.

    - Owns the serial.Serial instance.
    - Executes callables sequentially.
    - Prevents concurrent access from UI thread + polling thread.
    """

    io_error = pyqtSignal(str)
    connected = pyqtSignal(str)
    disconnected = pyqtSignal()

    def __init__(self, port: str, baudrate: int, open_kwargs: Optional[dict] = None, parent=None):
        super().__init__(parent)
        self.port = port
        self.baudrate = int(baudrate)
        self.open_kwargs = open_kwargs or {}
        self._queue: Queue[_Call] = Queue()
        self._running = True
        self._ser = None

    def run(self):
        try:
            import serial

            self._ser = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                bytesize=self.open_kwargs.get("bytesize", serial.EIGHTBITS),
                parity=self.open_kwargs.get("parity", serial.PARITY_NONE),
                stopbits=self.open_kwargs.get("stopbits", serial.STOPBITS_ONE),
                timeout=self.open_kwargs.get("timeout", 1),
                write_timeout=self.open_kwargs.get("write_timeout", 1),
                rtscts=self.open_kwargs.get("rtscts", False),
                dsrdtr=self.open_kwargs.get("dsrdtr", False),
                xonxoff=self.open_kwargs.get("xonxoff", False),
            )
            # Some USB-RS232 adapters need a brief settle time after opening,
            # especially on the first open after plugging in.
            try:
                self._ser.setDTR(False)
            except Exception:
                pass
            try:
                self._ser.setRTS(False)
            except Exception:
                pass
            try:
                self._ser.reset_input_buffer()
                self._ser.reset_output_buffer()
            except Exception:
                pass
            time.sleep(self.open_kwargs.get("open_settle_s", 0.2))
            self.connected.emit(self.port)
        except Exception as e:
            self.io_error.emit(f"串口打开失败({self.port}): {e}")
            self._running = False

        while self._running:
            try:
                call = self._queue.get(timeout=0.1)
            except Empty:
                continue
            try:
                if not self._ser or not getattr(self._ser, "is_open", False):
                    raise RuntimeError("串口未打开")
                res = call.fn(self._ser)
                call.reply_q.put((True, res))
            except Exception as e:
                tb = traceback.format_exc(limit=2)
                self.io_error.emit(f"串口I/O错误: {e}\n{tb}")
                call.reply_q.put((False, e))

        try:
            if self._ser and getattr(self._ser, "is_open", False):
                self._ser.close()
        except Exception:
            pass
        self._ser = None
        self.disconnected.emit()

    def stop(self):
        self._running = False
        self.wait(1500)

    def call(self, fn: Callable[[Any], Any], timeout_s: float = 2.0):
        reply_q: Queue = Queue(maxsize=1)
        self._queue.put(_Call(fn=fn, reply_q=reply_q, timeout_s=float(timeout_s)))
        try:
            ok, payload = reply_q.get(timeout=timeout_s)
        except Empty:
            raise TimeoutError("串口I/O超时")
        if ok:
            return payload
        raise payload


class VisaIOWorker(QThread):
    """Exclusive VISA (pyvisa) I/O worker."""

    io_error = pyqtSignal(str)
    connected = pyqtSignal(str)
    disconnected = pyqtSignal()

    def __init__(self, resource_name: str, timeout_ms: int = 5000, parent=None):
        super().__init__(parent)
        self.resource_name = resource_name
        self.timeout_ms = int(timeout_ms)
        self._queue: Queue[_Call] = Queue()
        self._running = True
        self._inst = None

    def run(self):
        try:
            import pyvisa

            rm = pyvisa.ResourceManager()
            self._inst = rm.open_resource(self.resource_name)
            self._inst.timeout = self.timeout_ms
            self.connected.emit(self.resource_name)
        except Exception as e:
            self.io_error.emit(f"VISA打开失败({self.resource_name}): {e}")
            self._running = False

        while self._running:
            try:
                call = self._queue.get(timeout=0.1)
            except Empty:
                continue
            try:
                if self._inst is None:
                    raise RuntimeError("VISA资源未打开")
                res = call.fn(self._inst)
                call.reply_q.put((True, res))
            except Exception as e:
                tb = traceback.format_exc(limit=2)
                self.io_error.emit(f"VISA I/O错误: {e}\n{tb}")
                call.reply_q.put((False, e))

        try:
            if self._inst is not None:
                self._inst.close()
        except Exception:
            pass
        self._inst = None
        self.disconnected.emit()

    def stop(self):
        self._running = False
        self.wait(1500)

    def call(self, fn: Callable[[Any], Any], timeout_s: float = 2.5):
        reply_q: Queue = Queue(maxsize=1)
        self._queue.put(_Call(fn=fn, reply_q=reply_q, timeout_s=float(timeout_s)))
        try:
            ok, payload = reply_q.get(timeout=timeout_s)
        except Empty:
            raise TimeoutError("VISA I/O超时")
        if ok:
            return payload
        raise payload
