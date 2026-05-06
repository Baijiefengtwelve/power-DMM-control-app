from __future__ import annotations

from ..parameter_models import AUTO_POWER_SOURCE_NAME


class StateSnapshotService:
    """Build stable state snapshots for web and remote consumers."""

    METER_KEYS = ("cathode", "gate", "anode", "backup", "vacuum")
    DEFAULT_RETENTION = {
        "keep_days": 30,
        "keep_runs": 200,
        "archive_before_delete": True,
        "archive_dir": "data/archive",
        "vacuum_mode": "incremental",
    }

    def __init__(self, mw):
        self.mw = mw
        self._cached_plot_revision = -1
        self._cached_plot_payload = {"t": []}

    def _collect_meter_state(self):
        meters = {}
        try:
            for key, value in self.mw.meter_data.items():
                meters[key] = dict(value)
        except Exception:
            return meters

        for key in self.METER_KEYS:
            if key not in meters:
                continue
            try:
                meters[key]["connected"] = bool(self.mw.is_meter_connected(key))
            except Exception:
                meters[key]["connected"] = False
        return meters

    def collect_state(self):
        keithley_names = self._connected_keithley_names()
        return {
            "flags": self._collect_flag_state(),
            "hv": self._collect_hv_state(),
            "keithley": self._collect_keithley_state(keithley_names),
            "meters": self._collect_meter_state(),
            "test_params": dict(getattr(self.mw, "test_params", {})),
            "stabilization_params": dict(getattr(self.mw, "stabilization_params", {})),
            "power_source_names": list(self.mw.list_power_source_names(include_auto=True)),
            "ui": self.collect_ui_config(),
        }

    def _collect_flag_state(self):
        return {
            "is_testing": bool(getattr(self.mw, "is_testing", False)),
            "is_recording": bool(getattr(self.mw, "is_recording", False)),
            "is_stabilizing": bool(getattr(self.mw, "stabilization_running", False))
            or bool(getattr(self.mw, "is_stabilizing", False)),
        }

    def _collect_hv_state(self):
        return {
            "connected": bool(getattr(self.mw.hv_controller, "is_connected", False)),
            "port": getattr(self.mw.hv_controller, "_last_port", ""),
            "voltage": float(getattr(self.mw, "_hv_v_cache", 0.0) or 0.0),
        }

    def _connected_keithley_names(self):
        try:
            return list(self.mw._get_connected_keithley_names())
        except Exception:
            return []

    def _collect_keithley_state(self, keithley_names):
        connected = bool(keithley_names) or bool(getattr(self.mw.keithley_controller, "is_connected", False))
        voltage = getattr(
            self.mw,
            "_keithley_v_cache",
            getattr(self.mw.keithley_controller, "current_voltage", 0.0),
        )
        return {
            "connected": connected,
            "gpib_address": getattr(self.mw.keithley_controller, "gpib_address", None),
            "voltage": float(voltage or 0.0),
            "connected_names": keithley_names,
        }

    def collect_ui_config(self):
        ui = {}
        ui.update(self._collect_power_ui_config())
        ui["meters"] = self._collect_meter_ui_config()
        ui.update(self._collect_vacuum_ui_config())
        ui["record_path"] = self._read_record_path()
        ui["plot_colors"] = self._collect_plot_colors()
        ui["plot_settings"] = self._collect_plot_settings()
        ui["retention"] = self._collect_retention_ui_config()
        return ui

    def _collect_power_ui_config(self):
        return {
            "hv_port": self._safe_widget_text("hv_port_combo", fallback=getattr(self.mw.hv_controller, "_last_port", "")),
            "hv_baudrate": self._safe_widget_text("hv_baudrate_combo", fallback=""),
            "keithley_resource": self._safe_widget_text("keithley_addr_combo", fallback=""),
            "power_source_names": self._collect_power_source_names(),
        }

    def _safe_widget_text(self, attr_name: str, fallback=""):
        try:
            return getattr(self.mw, attr_name).currentText()
        except Exception:
            return fallback

    def _collect_power_source_names(self):
        try:
            return list(self.mw.list_power_source_names(include_auto=True))
        except Exception:
            return [AUTO_POWER_SOURCE_NAME]

    def _collect_meter_ui_config(self):
        meters = {}
        for key in self.METER_KEYS:
            meters[key] = {
                "port": self._read_meter_port(key),
                "coefficient": self._read_meter_coefficient(key),
                "connected": self._is_meter_connected(key),
            }
        return meters

    def _read_meter_port(self, meter_type: str):
        try:
            return self.mw.get_meter_port(meter_type)
        except Exception:
            return ""

    def _read_meter_coefficient(self, meter_type: str) -> float:
        try:
            coeff_edit = getattr(self.mw, f"{meter_type}_coeff", None)
            return float(coeff_edit.text()) if coeff_edit else 1.0
        except Exception:
            return 1.0

    def _is_meter_connected(self, meter_type: str) -> bool:
        try:
            return bool(self.mw.is_meter_connected(meter_type))
        except Exception:
            return False

    def _collect_vacuum_ui_config(self):
        return {
            "vacuum_type": self._safe_call("get_vacuum_type", default="CM52"),
            "vacuum_channel": int(float(self._safe_call("get_vacuum_channel", default="3") or "3")),
            "vacuum_baudrate": int(float(self._safe_call("get_vacuum_baudrate", default="19200") or "19200")),
            "vacuum_unit": self._safe_call("get_vacuum_unit", default="Pa"),
        }

    def _safe_call(self, method_name: str, *, default):
        try:
            return getattr(self.mw, method_name)()
        except Exception:
            return default

    def _read_record_path(self):
        try:
            return self.mw.get_record_file_path()
        except Exception:
            return ""

    def _collect_plot_colors(self):
        try:
            colors = {}
            if getattr(self.mw, "config", None) is not None and self.mw.config.has_section("PlotColors"):
                for key, value in self.mw.config.items("PlotColors"):
                    colors[str(key).strip()] = str(value).strip()
            return colors
        except Exception:
            return {}

    def _collect_plot_settings(self):
        try:
            max_points = None
            if getattr(self.mw, "config", None) is not None and self.mw.config.has_option("PlotSettings", "max_points"):
                raw_value = self.mw.config.get("PlotSettings", "max_points")
                max_points = self.mw.data_buffer.normalize_max_points(raw_value)
            return {"max_points": max_points}
        except Exception:
            return {"max_points": None}

    def _collect_retention_ui_config(self):
        try:
            policy = getattr(self.mw, "retention_policy", None)
            return {
                "keep_days": int(getattr(policy, "keep_days", self.DEFAULT_RETENTION["keep_days"])),
                "keep_runs": int(getattr(policy, "keep_runs", self.DEFAULT_RETENTION["keep_runs"])),
                "archive_before_delete": bool(
                    getattr(policy, "archive_before_delete", self.DEFAULT_RETENTION["archive_before_delete"])
                ),
                "archive_dir": str(getattr(policy, "archive_dir", self.DEFAULT_RETENTION["archive_dir"])),
                "vacuum_mode": str(getattr(policy, "vacuum_mode", self.DEFAULT_RETENTION["vacuum_mode"])),
            }
        except Exception:
            return dict(self.DEFAULT_RETENTION)

    def collect_plot(self):
        try:
            current_revision = getattr(self.mw.data_buffer, "revision", 0)
            if current_revision == self._cached_plot_revision:
                return self._cached_plot_payload
            arrays = self.mw.data_buffer.get_plot_data()
            keys = [
                "t",
                "cathode",
                "gate",
                "anode",
                "backup",
                "keithley_voltage",
                "vacuum",
                "gate_plus_anode",
                "ratio",
            ]
            payload = {}
            for key, array in zip(keys, arrays):
                payload[key] = array.tolist() if hasattr(array, "tolist") else list(array)
            self._cached_plot_revision = current_revision
            self._cached_plot_payload = payload
            return payload
        except Exception:
            return {"t": []}
