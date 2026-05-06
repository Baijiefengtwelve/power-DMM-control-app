from __future__ import annotations

import math
import time

from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import QColorDialog, QDialog
import pyqtgraph as pg

from ..ui_dialogs import PlotSettingsDialog


class PlotService:
    """Own plot rendering, plot data refresh, and plot configuration."""

    DEFAULT_COLORS = {
        "cathode": "#E74C3C",
        "gate": "#2ECC71",
        "anode": "#3498DB",
        "backup": "#F39C12",
        "keithley_voltage": "#9B59B6",
        "gate_plus_anode": "#E67E22",
        "anode_cathode_ratio": "#1ABC9C",
        "vacuum": "#7F8C8D",
    }
    COLOR_SETTINGS_SERIES = [
        ("cathode", "阴极"),
        ("gate", "栅极"),
        ("anode", "阳极"),
        ("backup", "收集极"),
        ("keithley_voltage", "稳流电源电压"),
        ("gate_plus_anode", "栅极+阳极+收集极"),
        ("anode_cathode_ratio", "(阳极/阴极)×100"),
        ("vacuum", "真空"),
    ]
    PLOT_KEYS = (
        "cathode",
        "gate",
        "anode",
        "backup",
        "keithley_voltage",
        "gate_plus_anode",
        "anode_cathode_ratio",
    )
    RENDER_MIN_INTERVAL_S = 0.15

    def __init__(self, mw):
        self.mw = mw
        self._last_render_ts = 0.0
        self._last_render_revision = -1

    def default_plot_colors(self):
        return dict(self.DEFAULT_COLORS)

    def get_plot_color(self, key, fallback="#000000"):
        try:
            if self.mw.config and self.mw.config.has_section("PlotColors"):
                value = self.mw.config.get("PlotColors", key, fallback=fallback)
                if value:
                    return str(value).strip()
        except Exception:
            pass
        return fallback

    def get_plot_max_points(self):
        try:
            raw_value = self.mw.config.get("PlotSettings", "max_points", fallback="0")
        except Exception:
            raw_value = "0"
        return self.mw.data_buffer.normalize_max_points(raw_value)

    def save_plot_settings_to_config(self, colors_dict, max_points):
        try:
            payload = {
                "PlotColors": dict(colors_dict or {}),
                "PlotSettings": {"max_points": str(max_points or 0)},
            }
            self.mw.config_manager.save_config(payload)
            self.mw.config = self.mw.config_manager.load_config()
            return True
        except Exception as exc:
            self.mw.log_message(f"Failed to save plot settings: {exc}")
            return False

    def save_plot_colors_to_config(self, colors_dict):
        return self.save_plot_settings_to_config(colors_dict, self.get_plot_max_points())

    def apply_plot_colors(self):
        plots = getattr(self.mw, "plots", None)
        if not isinstance(plots, dict):
            return
        defaults = self.default_plot_colors()
        for key, item in plots.items():
            try:
                color = self.get_plot_color(key, defaults.get(key, "#000000"))
                item.setPen(pg.mkPen(color=color, width=1.5))
            except Exception:
                pass

    def apply_plot_settings(self, *, max_points=None):
        try:
            self.mw.data_buffer.reconfigure(max_points=max_points)
            self._update_plot_curves_if_due(force=True)
        except Exception as exc:
            self.mw.log_message(f"Failed to apply plot settings: {exc}")

    def show_plot_settings(self):
        try:
            defaults = self.default_plot_colors()
            current = {
                key: self.get_plot_color(key, defaults.get(key, "#000000"))
                for key, _ in self.COLOR_SETTINGS_SERIES
            }
            dialog = PlotSettingsDialog(
                self.mw,
                series=self.COLOR_SETTINGS_SERIES,
                current_colors=current,
                current_max_points=self.get_plot_max_points(),
            )
            if dialog.exec_() != QDialog.Accepted:
                return False

            new_colors, max_points = dialog.get_settings()
            for key, _ in self.COLOR_SETTINGS_SERIES:
                if key not in new_colors:
                    new_colors[key] = defaults.get(key, current.get(key, "#000000"))

            if self.save_plot_settings_to_config(new_colors, max_points):
                self.apply_plot_colors()
                self.apply_plot_settings(max_points=max_points)
                self.mw.log_message("Plot settings saved and applied.")
                return True
        except Exception as exc:
            self.mw.log_message(f"Failed to open plot settings: {exc}")
        return False

    def show_plot_color_settings(self):
        return self.show_plot_settings()

    def choose_single_plot_color(self, key: str, label: str = "曲线"):
        try:
            current = self.get_plot_color(key, self.default_plot_colors().get(key, "#000000"))
            color = QColorDialog.getColor(QColor(current), self.mw, f"选择颜色 - {label}")
            if not color.isValid():
                return False
            merged = dict(self.default_plot_colors())
            try:
                if self.mw.config and self.mw.config.has_section("PlotColors"):
                    for option in self.mw.config.options("PlotColors"):
                        merged[option] = self.mw.config.get("PlotColors", option)
            except Exception:
                pass
            merged[key] = color.name().upper()
            if self.save_plot_settings_to_config(merged, self.get_plot_max_points()):
                self.apply_plot_colors()
                self.mw.log_message(f"{label} plot color updated")
                return True
        except Exception as exc:
            self.mw.log_message(f"Failed to set {label} color: {exc}")
        return False

    def update_plots(self):
        try:
            meter_states = self._read_meter_states()
            cathode_state = meter_states["cathode"]
            gate_state = meter_states["gate"]
            anode_state = meter_states["anode"]
            backup_state = meter_states["backup"]
            vacuum_state = meter_states["vacuum"]

            try:
                stab_name = self.mw._get_record_power_name("stabilization")
                keithley_voltage = self.mw.power_catalog_service.get_record_power_voltage(stab_name)
            except Exception:
                keithley_voltage = None

            if keithley_voltage is None:
                keithley_voltage = float(getattr(self.mw, "_keithley_v_cache", 0.0) or 0.0)
            else:
                keithley_voltage = float(keithley_voltage)

            hv_voltage = float(getattr(self.mw, "_hv_v_cache", 0.0) or 0.0)

            self.mw.data_buffer.add_data(
                float(cathode_state.get("value", 0.0) or 0.0),
                float(gate_state.get("value", 0.0) or 0.0),
                float(anode_state.get("value", 0.0) or 0.0),
                float(backup_state.get("value", 0.0) or 0.0),
                keithley_voltage,
                float(vacuum_state.get("value", 0.0) or 0.0),
                meter_kinds={
                    "cathode": cathode_state.get("kind", ""),
                    "gate": gate_state.get("kind", ""),
                    "anode": anode_state.get("kind", ""),
                    "backup": backup_state.get("kind", ""),
                },
            )
            if not bool(getattr(self.mw, "is_recording", False)):
                self._enqueue_influx(
                    cathode_state=cathode_state,
                    gate_state=gate_state,
                    anode_state=anode_state,
                    backup_state=backup_state,
                    vacuum_state=vacuum_state,
                    keithley_voltage=keithley_voltage,
                    hv_voltage=hv_voltage,
                )
            self._update_plot_curves_if_due()
        except Exception:
            pass

    def clear_plots(self):
        try:
            self.mw.data_buffer.clear()
            for plot in getattr(self.mw, "plots", {}).values():
                plot.setData([], [])
            self.mw.log_message("Plot data cleared")
            return True
        except Exception as exc:
            self.mw.log_message(f"Failed to clear plot data: {exc}")
            return False

    def _read_meter_states(self):
        mutex = getattr(self.mw, "data_mutex", None)
        if mutex is not None:
            mutex.lock()
        try:
            meter_data = getattr(self.mw, "meter_data", {})
            return {
                "cathode": dict(meter_data.get("cathode", {})),
                "gate": dict(meter_data.get("gate", {})),
                "anode": dict(meter_data.get("anode", {})),
                "backup": dict(meter_data.get("backup", {})),
                "vacuum": dict(meter_data.get("vacuum", {})),
            }
        finally:
            if mutex is not None:
                mutex.unlock()

    def _update_plot_curves(self):
        current_revision = getattr(self.mw.data_buffer, "revision", 0)
        self._last_render_revision = current_revision
        self._last_render_ts = time.time()

        (
            time_data,
            cathode_data,
            gate_data,
            anode_data,
            backup_data,
            keithley_voltage_data,
            vacuum_data,
            gate_plus_anode_data,
            anode_cathode_ratio_data,
        ) = self.mw.data_buffer.get_plot_data()

        series_map = {
            "cathode": cathode_data,
            "gate": gate_data,
            "anode": anode_data,
            "backup": backup_data,
            "keithley_voltage": keithley_voltage_data,
            "gate_plus_anode": gate_plus_anode_data,
            "anode_cathode_ratio": anode_cathode_ratio_data,
        }
        for key in self.PLOT_KEYS:
            plot_item = getattr(self.mw, "plots", {}).get(key)
            if plot_item is None:
                continue
            self._set_curve_data(plot_item, time_data, series_map[key])

        vacuum_plot = getattr(self.mw, "plots", {}).get("vacuum")
        if vacuum_plot is not None:
            self._set_curve_data(vacuum_plot, time_data, vacuum_data)

    def _update_plot_curves_if_due(self, *, force: bool = False):
        revision = getattr(self.mw.data_buffer, "revision", 0)
        if not force and revision == self._last_render_revision:
            return
        if not force and (time.time() - self._last_render_ts) < self.RENDER_MIN_INTERVAL_S:
            return
        self._update_plot_curves()

    def _set_curve_data(self, plot_item, x_data, y_data):
        try:
            plot_item.setData(x_data, y_data, skipFiniteCheck=True)
            return
        except TypeError:
            pass
        except Exception:
            pass
        try:
            plot_item.setData(x_data, y_data)
        except Exception:
            pass

    def _enqueue_influx(
        self,
        *,
        cathode_state,
        gate_state,
        anode_state,
        backup_state,
        vacuum_state,
        keithley_voltage: float,
        hv_voltage: float,
    ):
        gate_plus_anode_value, gate_plus_anode_unit = self._combine_measurements(gate_state, anode_state, backup_state)
        ratio_value = self._compute_ratio(cathode_state, anode_state)

        try:
            hv_port = self.mw.hv_port_combo.currentText() if hasattr(self.mw, "hv_port_combo") else ""
        except Exception:
            hv_port = ""
        try:
            keithley_addr = self.mw.keithley_addr_combo.currentText() if hasattr(self.mw, "keithley_addr_combo") else ""
        except Exception:
            keithley_addr = ""

        try:
            self.mw.influx_writer.enqueue(
                fields={
                    "cathode": float(cathode_state.get("value", 0.0) or 0.0),
                    "cathode_unit": str(cathode_state.get("unit", "") or ""),
                    "gate": float(gate_state.get("value", 0.0) or 0.0),
                    "gate_unit": str(gate_state.get("unit", "") or ""),
                    "anode": float(anode_state.get("value", 0.0) or 0.0),
                    "anode_unit": str(anode_state.get("unit", "") or ""),
                    "backup": float(backup_state.get("value", 0.0) or 0.0),
                    "backup_unit": str(backup_state.get("unit", "") or ""),
                    "vacuum": float(vacuum_state.get("value", 0.0) or 0.0),
                    "keithley_voltage": float(keithley_voltage),
                    "hv_vout": float(hv_voltage),
                    "gate_plus_anode": gate_plus_anode_value,
                    "gate_plus_anode_unit": gate_plus_anode_unit,
                    "anode_cathode_ratio": ratio_value,
                    "is_testing": bool(getattr(self.mw, "is_testing", False)),
                    "is_stabilizing": bool(getattr(self.mw, "is_stabilizing", False)),
                    "is_recording": bool(getattr(self.mw, "is_recording", False)),
                },
                tags={
                    "hv_port": hv_port,
                    "keithley": keithley_addr,
                    "session": str(getattr(self.mw, "session_id", "")),
                    "run": str(getattr(self.mw, "current_run_id", "") or ""),
                },
                timestamp_ns=time.time_ns(),
            )
        except Exception:
            pass

    @staticmethod
    def _combine_measurements(*states):
        units = {str(state.get("unit", "") or "") for state in states if str(state.get("unit", "") or "")}
        if len(units) > 1:
            return math.nan, ""
        value = sum(float(state.get("value", 0.0) or 0.0) for state in states)
        return float(value), next(iter(units)) if units else ""

    @staticmethod
    def _compute_ratio(cathode_state, anode_state):
        cathode_unit = str(cathode_state.get("unit", "") or "")
        anode_unit = str(anode_state.get("unit", "") or "")
        if cathode_unit and anode_unit and cathode_unit != anode_unit:
            return math.nan
        cathode_value = float(cathode_state.get("value", 0.0) or 0.0)
        if cathode_value == 0.0:
            return math.nan
        anode_value = float(anode_state.get("value", 0.0) or 0.0)
        return (anode_value / cathode_value) * 100.0
