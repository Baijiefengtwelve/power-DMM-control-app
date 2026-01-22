from __future__ import annotations

import threading
import uuid
from typing import Any, Dict, Optional

from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot, Qt

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

        def ok(data=None, message="OK"):
            return {"ok": True, "message": message, "data": data}

        def err(message="ERROR", data=None):
            return {"ok": False, "message": message, "data": data}

        try:
            # ---------- Read-only ----------
            if action == "get_state":
                state = self._collect_state()
                self._result_signal.emit(cmd_id, ok(state))
                return

            if action == "get_plot":
                data = self._collect_plot()
                self._result_signal.emit(cmd_id, ok(data))
                return

            if action == "db_stats":
                self._result_signal.emit(cmd_id, ok(self.mw.get_db_stats()))
                return

            if action == "db_cleanup":
                try:
                    keep_days = int(float(params.get("keep_days", getattr(self.mw.retention_policy, "keep_days", 30))) or 30)
                    keep_runs = int(float(params.get("keep_runs", getattr(self.mw.retention_policy, "keep_runs", 200))) or 200)
                    archive_before_delete = bool(params.get("archive_before_delete", True))
                    archive_dir = str(params.get("archive_dir", getattr(self.mw.retention_policy, "archive_dir", "data/archive")))
                    vacuum_mode = str(params.get("vacuum_mode", getattr(self.mw.retention_policy, "vacuum_mode", "incremental")))
                except Exception as e:
                    self._result_signal.emit(cmd_id, err(f"Invalid cleanup params: {e}"))
                    return
                res = self.mw.cleanup_database(
                    keep_days=keep_days,
                    keep_runs=keep_runs,
                    archive_before_delete=archive_before_delete,
                    archive_dir=archive_dir,
                    vacuum_mode=vacuum_mode,
                )
                self._result_signal.emit(cmd_id, res)
                return

            if action == "list_ports":
                try:
                    ports = self.mw.get_available_ports()
                except Exception:
                    # fallback: use existing combobox items
                    ports = []
                    try:
                        ports = [self.mw.hv_port_combo.itemText(i) for i in range(self.mw.hv_port_combo.count())]
                    except Exception:
                        pass
                self._result_signal.emit(cmd_id, ok({"ports": ports}))
                return

            # ---------- Mutations / controls ----------
            if action == "refresh_ports":
                self.mw.refresh_all_ports()
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "set_test_params":
                self.mw.test_params.update({
                    "start_voltage": float(params.get("start_voltage", self.mw.test_params.get("start_voltage", 0.0))),
                    "target_voltage": float(params.get("target_voltage", self.mw.test_params.get("target_voltage", 0.0))),
                    "voltage_step": float(params.get("voltage_step", self.mw.test_params.get("voltage_step", 1.0))),
                    "step_delay": float(params.get("step_delay", self.mw.test_params.get("step_delay", 1.0))),
                    "cycle_time": float(params.get("cycle_time", self.mw.test_params.get("cycle_time", 0.0))),
                })
                self.mw.update_settings_display()
                self._result_signal.emit(cmd_id, ok(self.mw.test_params))
                return

            if action == "set_stabilization_params":
                # Coerce/validate using the same semantics as the Qt dialog
                sp = getattr(self.mw, "stabilization_params", {})
                try:
                    if "target_current" in params:
                        sp["target_current"] = float(params.get("target_current"))
                    if "stability_range" in params:
                        sp["stability_range"] = float(params.get("stability_range"))
                    if "start_voltage" in params:
                        sp["start_voltage"] = float(params.get("start_voltage"))
                    if "adjust_frequency" in params:
                        sp["adjust_frequency"] = float(params.get("adjust_frequency"))
                    if "max_adjust_voltage" in params:
                        sp["max_adjust_voltage"] = float(params.get("max_adjust_voltage"))

                    if "algorithm" in params:
                        a = str(params.get("algorithm") or "").strip().lower()
                        mapping_a = {
                            "pid": "pid",
                            "pid算法": "pid",
                            "pid控制": "pid",
                            "approach": "approach",
                            "接近": "approach",
                            "接近算法": "approach",
                            "接近算法(1v步进)": "approach",
                        }
                        if a in mapping_a:
                            sp["algorithm"] = mapping_a[a]
                        else:
                            # tolerate unknown values by defaulting to pid
                            sp["algorithm"] = "pid"

                    if "current_source" in params:
                        src = str(params.get("current_source") or "").strip().lower()
                        # Accept both key-form and some common Chinese labels
                        mapping = {
                            "keithley": "keithley",
                            "keithley自身": "keithley",
                            "自身": "keithley",
                            "cathode": "cathode",
                            "阴极": "cathode",
                            "gate": "gate",
                            "栅极": "gate",
                            "anode": "anode",
                            "阳极": "anode",
                            "backup": "backup",
                            "收集极": "backup",
                        }
                        if src in mapping:
                            sp["current_source"] = mapping[src]
                        else:
                            self._result_signal.emit(cmd_id, err(f"Invalid current_source: {src}"))
                            return

                    # Keep any extra keys if you extend stabilization algorithms later
                    for k, v in (params or {}).items():
                        if k not in sp:
                            sp[k] = v

                    self.mw.stabilization_params = sp
                    self.mw.update_settings_display()
                    # Persist to config (Qt dialog does this)
                    try:
                        if hasattr(self.mw, "save_config_from_ui"):
                            self.mw.save_config_from_ui()
                    except Exception:
                        pass

                    self._result_signal.emit(cmd_id, ok(dict(self.mw.stabilization_params)))
                    return
                except Exception as e:
                    self._result_signal.emit(cmd_id, err(f"Invalid stabilization params: {e}"))
                    return

            if action == "set_meter_coeff":
                meter_type = str(params.get("meter_type"))
                coeff = float(params.get("coefficient", 1.0))
                if meter_type in self.mw.meter_data:
                    self.mw.meter_data[meter_type]["coefficient"] = coeff
                    self._result_signal.emit(cmd_id, ok())
                else:
                    self._result_signal.emit(cmd_id, err(f"Unknown meter_type: {meter_type}"))
                return

            if action == "hv_connect":
                port = str(params.get("port", "")).strip()
                if not port:
                    # use current selection
                    try:
                        port = str(self.mw.hv_port_combo.currentText()).strip()
                    except Exception:
                        pass
                if not port:
                    self._result_signal.emit(cmd_id, err("HV port is empty"))
                    return
                # Ensure controller uses selected port
                try:
                    self.mw.hv_port_combo.setCurrentText(port)
                except Exception:
                    pass
                self.mw.toggle_hv_connection()
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "hv_disconnect":
                if getattr(self.mw.hv_controller, "is_connected", False):
                    self.mw.toggle_hv_connection()
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "keithley_connect":
                # Accept either resource_name (e.g., GPIB0::14::INSTR) or numeric gpib_address (e.g., 14)
                resource_name = params.get("resource_name", None)
                addr = params.get("gpib_address", None)

                try:
                    if resource_name is not None:
                        self.mw.keithley_addr_combo.setCurrentText(str(resource_name))
                    elif addr is not None:
                        self.mw.keithley_addr_combo.setCurrentText(str(addr))
                except Exception:
                    pass

                # Let MainWindow.toggle_keithley_connection handle parsing and controller calls
                self.mw.toggle_keithley_connection()
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "keithley_disconnect":
                if getattr(self.mw.keithley_controller, "is_connected", False):
                    self.mw.toggle_keithley_connection()
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "meter_toggle":
                meter_type = str(params.get("meter_type", ""))
                port = str(params.get("port", "")).strip()
                if port:
                    try:
                        combo = getattr(self.mw, f"{meter_type}_port_combo", None)
                        if combo is not None:
                            combo.setCurrentText(port)
                    except Exception:
                        pass
                self.mw.toggle_meter_connection(meter_type)
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "start_test":
                self.mw.test_service.start(cycle=False)
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "start_cycle_test":
                self.mw.test_service.start(cycle=True)
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "stop_test":
                self.mw.test_service.stop()
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "reset_voltage":
                self.mw.reset_voltage()
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "start_stabilization":
                self.mw.stabilization_service.start()
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "stop_stabilization":
                self.mw.stabilization_service.stop()
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "set_record_path":
                path = str(params.get("path","")).strip()
                if not path:
                    self._result_signal.emit(cmd_id, err("path is empty"))
                    return
                self.mw.save_path = path
                try:
                    if hasattr(self.mw, "path_label"):
                        self.mw.path_label.setText(path)
                except Exception:
                    pass
                self._result_signal.emit(cmd_id, ok({"path": path}))
                return

            if action == "toggle_recording":
                # MainWindow has start/stop recording methods
                if hasattr(self.mw, "toggle_record"):
                    self.mw.toggle_record()
                elif hasattr(self.mw, "toggle_record"):
                    self.mw.toggle_record()
                else:
                    self._result_signal.emit(cmd_id, err("Recording API not found on MainWindow (expected toggle_record)"))
                    return
                self._result_signal.emit(cmd_id, ok())
                return

            if action == "clear_chart":
                if hasattr(self.mw, "clear_plots"):
                    self.mw.clear_plots()
                elif hasattr(self.mw, "data_buffer"):
                    try:
                        self.mw.data_buffer.clear()
                    except Exception:
                        pass
                self._result_signal.emit(cmd_id, ok())
                return

            self._result_signal.emit(cmd_id, err(f"Unknown action: {action}"))
        except Exception as e:
            self._result_signal.emit(cmd_id, err(str(e)))

    def _collect_state(self) -> Dict[str, Any]:
        """Collect current state snapshot for the web UI."""
        hv_connected = bool(getattr(self.mw.hv_controller, "is_connected", False))
        keithley_connected = bool(getattr(self.mw.keithley_controller, "is_connected", False))

        # Actual HV (HAPS06) output voltage cache in MainWindow
        hv_v = float(getattr(self.mw, "_hv_v_cache", 0.0) or 0.0)
        # Keithley voltage from controller
        keithley_v = float(getattr(self.mw.keithley_controller, "current_voltage", 0.0) or 0.0)

        # Meter data snapshot
        meters = {}
        try:
            for k, v in self.mw.meter_data.items():
                meters[k] = dict(v)
        except Exception:
            pass


        # Add connection flags for meters (aligned with desktop UI button text)
        for _k in ["cathode", "gate", "anode", "backup", "vacuum"]:
            try:
                btn = getattr(self.mw, f"{_k}_connect_btn", None)
                if btn is not None and _k in meters:
                    meters[_k]["connected"] = (btn.text() == "断开")
            except Exception:
                pass

        return {
            "flags": {
                "is_testing": bool(getattr(self.mw, "is_testing", False)),
                "is_recording": bool(getattr(self.mw, "is_recording", False)),
                "is_stabilizing": bool(getattr(self.mw, "stabilization_running", False)) or bool(getattr(self.mw, "is_stabilizing", False)),
            },
            "hv": {
                "connected": hv_connected,
                "port": getattr(self.mw.hv_controller, "_last_port", ""),
                "voltage": hv_v,
            },
            "keithley": {
                "connected": keithley_connected,
                "gpib_address": getattr(self.mw.keithley_controller, "gpib_address", None),
                "voltage": keithley_v,
            },
            "meters": meters,
            "test_params": dict(getattr(self.mw, "test_params", {})),
            "stabilization_params": dict(getattr(self.mw, "stabilization_params", {})),
            "ui": self._collect_ui_config(),
        }

    
    def _collect_ui_config(self) -> Dict[str, Any]:
        """
        Collect UI-selected configuration values (ports, coefficients, record path, etc.).
        This allows the web UI to auto-fill controls using the same configuration as the desktop UI.
        """
        ui: Dict[str, Any] = {}

        # HV
        try:
            ui["hv_port"] = self.mw.hv_port_combo.currentText()
        except Exception:
            ui["hv_port"] = getattr(self.mw.hv_controller, "_last_port", "")
        try:
            ui["hv_baudrate"] = self.mw.hv_baudrate_combo.currentText()
        except Exception:
            ui["hv_baudrate"] = ""

        # Keithley (resource string shown in GUI)
        try:
            ui["keithley_resource"] = self.mw.keithley_addr_combo.currentText()
        except Exception:
            ui["keithley_resource"] = ""

        # Meters
        meters: Dict[str, Any] = {}
        for key in ["cathode", "gate", "anode", "backup", "vacuum"]:
            try:
                port_combo = getattr(self.mw, f"{key}_port_combo", None)
                port = port_combo.currentText() if port_combo else ""
            except Exception:
                port = ""
            try:
                coeff_edit = getattr(self.mw, f"{key}_coeff", None)
                coeff = float(coeff_edit.text()) if coeff_edit else 1.0
            except Exception:
                coeff = 1.0

            # Connected state inferred from button text (aligned with desktop behavior)
            try:
                btn = getattr(self.mw, f"{key}_connect_btn", None)
                connected = bool(btn and btn.text() == "断开")
            except Exception:
                connected = False

            meters[key] = {"port": port, "coefficient": coeff, "connected": connected}
        ui["meters"] = meters

        # Vacuum params (channel/baud) from config
        try:
            ui["vacuum_channel"] = int(self.mw.config.get("Multimeter", "vacuum_channel", fallback="3"))
        except Exception:
            ui["vacuum_channel"] = 3
        try:
            ui["vacuum_baudrate"] = int(self.mw.config.get("Multimeter", "vacuum_baudrate", fallback="19200"))
        except Exception:
            ui["vacuum_baudrate"] = 19200

        # Record path
        try:
            ui["record_path"] = self.mw.record_path_edit.text()
        except Exception:
            try:
                ui["record_path"] = self.mw.config.get("DataRecord", "save_path", fallback="")
            except Exception:
                ui["record_path"] = ""

        # Plot colors (align web chart colors with desktop pyqtgraph)
        try:
            colors = {}
            if getattr(self.mw, "config", None) is not None and self.mw.config.has_section("PlotColors"):
                for k, v in self.mw.config.items("PlotColors"):
                    colors[str(k).strip()] = str(v).strip()
            ui["plot_colors"] = colors
        except Exception:
            ui["plot_colors"] = {}

        # Retention settings for SQLite maintenance (used by web form default values)
        try:
            rp = getattr(self.mw, "retention_policy", None)
            ui["retention"] = {
                "keep_days": int(getattr(rp, "keep_days", 30)),
                "keep_runs": int(getattr(rp, "keep_runs", 200)),
                "archive_before_delete": bool(getattr(rp, "archive_before_delete", True)),
                "archive_dir": str(getattr(rp, "archive_dir", "data/archive")),
                "vacuum_mode": str(getattr(rp, "vacuum_mode", "incremental")),
            }
        except Exception:
            ui["retention"] = {
                "keep_days": 30,
                "keep_runs": 200,
                "archive_before_delete": True,
                "archive_dir": "data/archive",
                "vacuum_mode": "incremental",
            }

        return ui

    def _collect_plot(self) -> Dict[str, Any]:
        """Return recent plot arrays (limited) for web chart."""
        try:
            arrays = self.mw.data_buffer.get_plot_data()
            # arrays order: time, cathode, gate, anode, backup, keithley_voltage, vacuum, gate_plus_anode, ratio
            keys = ["t","cathode","gate","anode","backup","keithley_voltage","vacuum","gate_plus_anode","ratio"]
            out = {}
            for k, arr in zip(keys, arrays):
                out[k] = arr.tolist()
            return out
        except Exception:
            return {"t": []}