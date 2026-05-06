from __future__ import annotations

import datetime

from .common import *
from PyQt5.QtWidgets import QApplication

class CountdownManager:
    """倒计时管理器"""

    def __init__(self, update_callback):
        self.countdown = 0
        self.timer = QTimer()
        self.update_callback = update_callback
        self.timer.timeout.connect(self._update)

    def start(self, seconds):
        """开始倒计时"""
        self.countdown = seconds
        self.timer.start(1000)
        self._update()

    def stop(self):
        """停止倒计时"""
        self.timer.stop()
        self.countdown = 0

    def _update(self):
        """内部更新方法"""
        if self.countdown > 0:
            self.countdown -= 1
            if self.update_callback:
                self.update_callback(self.countdown)
        else:
            self.stop()


class DataSaver(QThread):
    """后台数据保存线程（CSV 版）

    设计目标：
    - 采集线程只负责把行数据 push 到队列，保存线程做批量落盘，减少 UI 卡顿与磁盘 IO 频次。
    - SQLite 作为权威原始日志（可选），CSV 作为高性能、可直接打开的导出格式（无 xlsx 开销）。

    约定：
    - 原始数据：<path>.csv（append）
    - 循环数据：<path>_cycle.csv（overwrite by finalize 或 append by append_cycle_row）
    - 统计数据：<path>_summary.csv（overwrite by finalize）
    """

    save_complete = pyqtSignal()
    convert_complete = pyqtSignal()

    def __init__(self):
        super().__init__()
        self.headers = DATA_HEADERS
        import queue
        self.queue = queue.Queue(maxsize=50000)

        self.csv_path: str | None = None
        self.running = True

        # 批量写入：避免每行都 flush
        self.batch_size = 100
        # 定时 flush：避免持续高频入队导致长期不落盘
        self.flush_interval_sec = 3600.0  # disabled; use batch_size flush
        self._last_flush_ts = time.time()
        self._next_retry_ts = 0.0
        self._retry_interval_sec = 2.0  # avoid tight loop when file is locked
        self._stop_requested = False
        self._qt_signals_suppressed = False
        self._pending_rows: list[list] = []
        self._queue_drain_limit = 64
        self._save_signal_interval_sec = 1.0
        self._last_save_signal_ts = 0.0

        self._lock = threading.RLock()

        # 状态回传（用于 UI 提示）
        self.last_convert_success: bool | None = None
        self.last_convert_message: str = ""
        self.last_stop_ok: bool | None = None
        self.last_stop_message: str = ""

    # -------- public API (thread-safe) --------

    def set_output_path(self, csv_path: str):
        """设置原始数据 CSV 路径（仅记录；真正写入在保存线程内完成）。"""
        self.csv_path = str(csv_path) if csv_path else None

    def add_batch(self, rows: list):
        """追加一批行数据（每行应与 headers 同长度）。"""
        if not rows:
            return
        try:
            # 防御性拷贝：避免调用方传入的 list 在入队后被 clear()/复用。
            #（这是导致“CSV 只有表头”的常见根因之一）
            safe_rows = [list(r) for r in rows]
            import queue
            self.queue.put(("add_batch", safe_rows), timeout=0.5)
        except queue.Full:
            pass
        except Exception:
            pass

    def add_marker_row(self, text: str):
        """写入标记行（用于循环分隔）。"""
        if not text:
            return
        try:
            import queue
            self.queue.put(("marker", text), timeout=0.5)
        except queue.Full:
            pass
        except Exception:
            pass

    def append_cycle_row(self, cycle: int, min_anode, voltage, time_str: str):
        """追加一条循环统计行到 cycle.csv。"""
        try:
            import queue
            self.queue.put(("cycle_row", {
                "cycle": cycle,
                "min_anode": min_anode,
                "voltage": voltage,
                "time": time_str,
            }), timeout=0.5)
        except queue.Full:
            pass
        except Exception:
            pass

    def force_save(self):
        """强制 flush pending rows。"""
        try:
            import queue
            self.queue.put(("flush", None), timeout=0.5)
        except queue.Full:
            pass
        except Exception:
            pass

    def request_convert(self, csv_path: str, anode_min=None, cycle_data=None):
        """生成统计/循环 CSV（不做 xlsx 转换；保留原方法名以兼容旧调用）。"""
        try:
            import queue
            self.queue.put(("finalize", {
                "csv_path": str(csv_path) if csv_path else None,
                "anode_min": anode_min,
                "cycle_data": cycle_data,
            }), timeout=1.0)
        except queue.Full:
            pass
        except Exception:
            pass

    # -------- internal helpers --------

    def _ensure_parent_dir(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    def _needs_header(self, path: str) -> bool:
        if not os.path.exists(path):
            return True
        try:
            return os.path.getsize(path) == 0
        except Exception:
            return True

    def _open_writer(self, path: str):
        """以 append 模式打开，返回 (fh, writer)。"""
        # utf-8-sig: 兼容 Excel 直接打开中文列名不乱码
        fh = open(path, "a", newline="", encoding="utf-8-sig")
        writer = csv.writer(fh)
        return fh, writer

    def _write_header_if_needed(self, writer, path: str):
        if self._needs_header(path):
            writer.writerow(list(self.headers))

    def _append_rows_to_csv(self, path: str, rows: list[list]) -> bool:
        """尝试追加写入 rows 到 CSV。

        返回：
        - True: 写入成功（已落盘）
        - False: 写入失败（常见：文件被 Excel 打开导致 PermissionError）。失败时 *不应* 清空 rows。
        """
        if not path or not rows:
            return True
        self._ensure_parent_dir(path)
        try:
            fh, writer = self._open_writer(path)
            try:
                self._write_header_if_needed(writer, path)
                writer.writerows(rows)
            finally:
                try:
                    fh.close()
                except Exception:
                    pass
            return True
        except PermissionError:
            # Windows 下 CSV 被 Excel 打开时通常是独占锁；此时不能写入。
            return False
        except Exception:
            return False

    def _write_recovery_csv(self, base_csv_path: str, rows: list[list]) -> str | None:
        """当 base_csv_path 被锁定无法写入时，把剩余数据写入 recovery 文件，避免丢数。"""
        if not base_csv_path or not rows:
            return None
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        p = pathlib.Path(base_csv_path)
        out_path = str(p.with_suffix("")) + f"_recovery_{ts}.csv"
        try:
            self._ensure_parent_dir(out_path)
            with open(out_path, "w", newline="", encoding="utf-8-sig") as fh:
                w = csv.writer(fh)
                w.writerow(list(self.headers))
                w.writerows(rows)
            return out_path
        except Exception:
            return None

    def _write_cycle_csv(self, base_csv_path: str, cycle_data: list[dict]):
        if not base_csv_path or not cycle_data:
            return
        p = pathlib.Path(base_csv_path)
        out_path = str(p.with_suffix("")) + "_cycle.csv"
        self._ensure_parent_dir(out_path)
        with open(out_path, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh)
            w.writerow(["cycle", "min_anode", "voltage", "time"])
            for d in cycle_data:
                w.writerow([d.get("cycle"), d.get("min_anode"), d.get("voltage"), d.get("time")])

    def _append_cycle_row_csv(self, base_csv_path: str, row: dict):
        if not base_csv_path or not row:
            return
        p = pathlib.Path(base_csv_path)
        out_path = str(p.with_suffix("")) + "_cycle.csv"
        self._ensure_parent_dir(out_path)
        need_header = self._needs_header(out_path)
        with open(out_path, "a", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh)
            if need_header:
                w.writerow(["cycle", "min_anode", "voltage", "time"])
            w.writerow([row.get("cycle"), row.get("min_anode"), row.get("voltage"), row.get("time")])

    def _write_summary_csv(self, base_csv_path: str, anode_min: dict | None):
        if not base_csv_path:
            return
        p = pathlib.Path(base_csv_path)
        out_path = str(p.with_suffix("")) + "_summary.csv"
        self._ensure_parent_dir(out_path)
        with open(out_path, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh)
            w.writerow(["item", "value"])
            if anode_min:
                w.writerow(["min_anode", anode_min.get("min_anode")])
                w.writerow(["min_anode_voltage", anode_min.get("voltage")])
                w.writerow(["min_anode_time", anode_min.get("time")])

    def _notify_save_complete(self, *, force: bool = False):
        if not self._should_emit_qt_signals():
            return
        now_ts = time.monotonic()
        if (not force) and (now_ts - self._last_save_signal_ts) < self._save_signal_interval_sec:
            return
        self._last_save_signal_ts = now_ts
        self.save_complete.emit()

    def _should_emit_qt_signals(self) -> bool:
        if self._qt_signals_suppressed:
            return False
        try:
            app = QApplication.instance()
        except Exception:
            app = None
        return app is not None or not self._stop_requested

    def suppress_qt_signals(self):
        self._qt_signals_suppressed = True

    def _flush_pending_rows_locked(self, csv_path: str | None = None, *, allow_recovery: bool = False, force_signal: bool = False) -> bool:
        target_path = str(csv_path or self.csv_path or "").strip()
        if not target_path or not self._pending_rows:
            return True

        ok = self._append_rows_to_csv(target_path, self._pending_rows)
        if ok:
            self._pending_rows.clear()
            self._last_flush_ts = time.time()
            self._next_retry_ts = 0.0
            self._notify_save_complete(force=force_signal)
            return True

        if allow_recovery:
            rec = self._write_recovery_csv(target_path, self._pending_rows)
            if rec:
                self._pending_rows.clear()
                self.last_convert_success = True
                self.last_convert_message = f"CSV 被占用，剩余数据已写入: {rec}"
                self._notify_save_complete(force=True)
                return True

        self._next_retry_ts = time.time() + self._retry_interval_sec
        return False

    def _drain_pending_commands(self, first_cmd, first_payload):
        commands = [(first_cmd, first_payload)]
        while len(commands) < self._queue_drain_limit:
            try:
                commands.append(self.queue.get_nowait())
            except Empty:
                break
            except Exception:
                break
        return commands

    # -------- thread loop --------

    def run(self):
        while True:
            try:
                cmd, payload = self.queue.get(timeout=0.2)
            except Exception:
                cmd, payload = None, None

            if cmd is None:
                continue

            stop_requested = False
            for cmd, payload in self._drain_pending_commands(cmd, payload):
                if cmd == "stop":
                    with self._lock:
                        self._flush_pending_rows_locked(allow_recovery=True, force_signal=True)
                    stop_requested = True
                    break

                if cmd == "add_batch":
                    rows = payload or []
                    if not rows:
                        continue
                    now_ts = time.time()
                    with self._lock:
                        self._pending_rows.extend(rows)
                        if (
                            len(self._pending_rows) >= self.batch_size
                            and self.csv_path
                            and self._pending_rows
                            and now_ts >= self._next_retry_ts
                        ):
                            self._flush_pending_rows_locked()
                    continue

                if cmd == "marker":
                    text = str(payload)
                    now_ts = time.time()
                    with self._lock:
                        row = [f"# {text}"] + [""] * (len(self.headers) - 1)
                        self._pending_rows.append(row)
                        if (
                            self.csv_path
                            and len(self._pending_rows) >= self.batch_size
                            and now_ts >= self._next_retry_ts
                        ):
                            self._flush_pending_rows_locked()
                    continue

                if cmd == "flush":
                    with self._lock:
                        if self._pending_rows and self.csv_path:
                            self._flush_pending_rows_locked()
                    continue

                if cmd == "cycle_row":
                    with self._lock:
                        if self.csv_path:
                            self._append_cycle_row_csv(self.csv_path, payload)
                    continue

                if cmd == "finalize":
                    try:
                        csv_path = str((payload or {}).get("csv_path") or self.csv_path or "")
                        anode_min = (payload or {}).get("anode_min")
                        cycle_data = (payload or {}).get("cycle_data")

                        with self._lock:
                            self._flush_pending_rows_locked(
                                csv_path,
                                allow_recovery=True,
                                force_signal=True,
                            )
                            if csv_path:
                                self._write_summary_csv(csv_path, anode_min)
                                if cycle_data:
                                    self._write_cycle_csv(csv_path, cycle_data)

                        self.last_convert_success = True
                        self.last_convert_message = "CSV 统计/循环数据已生成"
                    except Exception as e:
                        self.last_convert_success = False
                        self.last_convert_message = str(e)

                    if self._should_emit_qt_signals():
                        self.convert_complete.emit()

            if stop_requested:
                break

        # shutdown flush
        try:
            with self._lock:
                if self._pending_rows and self.csv_path:
                    self._append_rows_to_csv(self.csv_path, self._pending_rows)
                    self._pending_rows.clear()
        except Exception:
            pass

    def stop(self):
        """请求停止线程：先 flush 再退出。"""
        self._stop_requested = True
        try:
            # 先触发一次 flush，再请求 stop，确保退出前落盘
            self.queue.put(("flush", None))
            self.queue.put(("stop", None))
        except Exception:
            pass
        try:
            self.wait(5000)
        except Exception:
            pass
        if self.isRunning():
            self.last_stop_ok = False
            self.last_stop_message = "data saver stop timed out after 5.0s"
            return False
        self.last_stop_ok = True
        self.last_stop_message = ""
        return True

