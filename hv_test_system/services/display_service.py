from __future__ import annotations

import time

from ..measurement_units import format_measurement_value


class DisplayService:
    """Own non-business UI refresh paths for labels, status text, and cached values."""

    METER_TYPES = ("cathode", "gate", "anode", "backup", "vacuum")
    VOLTAGE_STATE_COLORS = {
        "connected": "#34a853",
        "pending": "#5f6368",
        "disconnected": "#ea4335",
    }

    def __init__(self, mw):
        self.mw = mw
        self._status_override_message = ""
        self._status_override_until = 0.0
        self._last_sqlite_drop_count = 0
        self._last_influx_drop_count = 0

    def _show_status_message(self, message):
        try:
            if hasattr(self.mw, "show_status_message"):
                self.mw.show_status_message(str(message))
            else:
                self.mw.status_bar.showMessage(str(message))
        except Exception:
            pass

    def _push_status_override(self, message: str, *, duration_s: float = 6.0):
        self._status_override_message = str(message)
        self._status_override_until = time.time() + max(0.0, float(duration_s))
        self._show_status_message(self._status_override_message)

    def _show_active_status_override(self) -> bool:
        if not self._status_override_message:
            return False
        if time.time() >= self._status_override_until:
            self._status_override_message = ""
            self._status_override_until = 0.0
            return False
        self._show_status_message(self._status_override_message)
        return True

    def _poll_writer_warnings(self):
        sqlite_status = {}
        influx_status = {}
        sqlite_recorder = getattr(self.mw, "sqlite_recorder", None)
        influx_writer = getattr(self.mw, "influx_writer", None)
        try:
            if sqlite_recorder is not None:
                sqlite_status = sqlite_recorder.status()
        except Exception:
            sqlite_status = {}
        try:
            if influx_writer is not None:
                influx_status = influx_writer.status()
        except Exception:
            influx_status = {}

        sqlite_dropped = int(sqlite_status.get("total_dropped", 0) or 0)
        influx_dropped = int(influx_status.get("total_dropped", 0) or 0)

        if sqlite_dropped > self._last_sqlite_drop_count:
            dropped_now = sqlite_dropped - self._last_sqlite_drop_count
            self._last_sqlite_drop_count = sqlite_dropped
            message = f"SQLite recorder dropped {dropped_now} rows (total {sqlite_dropped})"
            self.mw.log_message(message)
            self._push_status_override(message)
            return True
        self._last_sqlite_drop_count = sqlite_dropped

        if influx_dropped > self._last_influx_drop_count:
            dropped_now = influx_dropped - self._last_influx_drop_count
            self._last_influx_drop_count = influx_dropped
            error_text = str(influx_status.get("last_error", "") or "").strip()
            message = f"Influx writer dropped {dropped_now} rows (total {influx_dropped})"
            if error_text:
                message = f"{message}: {error_text}"
            self.mw.log_message(message)
            self._push_status_override(message)
            return True
        self._last_influx_drop_count = influx_dropped
        return False

    def set_power_voltage_cache(self, name: str, voltage: float):
        clean_name = str(name or "").strip()
        if not clean_name:
            return
        try:
            self.mw.power_voltage_cache[clean_name] = float(voltage)
            self.mw.power_voltage_ts[clean_name] = time.time()
        except Exception:
            pass

    def get_power_voltage_cache(self, name: str):
        clean_name = str(name or "").strip()
        if not clean_name:
            return None, 0.0
        try:
            return self.mw.power_voltage_cache.get(clean_name), float(self.mw.power_voltage_ts.get(clean_name, 0.0))
        except Exception:
            return None, 0.0

    def set_voltage_label_state(self, label, text: str, state: str = "disconnected"):
        label.setText(text)
        color = self.VOLTAGE_STATE_COLORS.get(state, "#1d1d1f")
        bg_colors = {
            "connected": "#e6f4ea",
            "pending": "#f1f3f4",
            "disconnected": "#fce8e6",
        }
        bg_color = bg_colors.get(state, "#ffffff")
        label.setStyleSheet(
            f"font-size: 12pt; font-weight: bold; color: {color}; "
            f"padding: 4px; background-color: {bg_color}; "
            f"border: 1px solid {color}; border-radius: 4px;"
        )

    def refresh_power_slot(self, slot_index: int, value_label):
        name = self.mw._display_power_name(slot_index)
        device = self.mw._find_power_device(name)
        if not device:
            self.set_voltage_label_state(value_label, "未配置", "pending")
            return
        if not self.mw.is_power_device_connected(name):
            self.set_voltage_label_state(value_label, "未连接", "disconnected")
            return
        cached_value, ts = self.get_power_voltage_cache(name)
        if ts > 0 and cached_value is not None:
            self.set_voltage_label_state(value_label, f"{float(cached_value):.1f} V", "connected")
            return
        self.set_voltage_label_state(value_label, "读取中...", "pending")

    def refresh_power_voltage_slots(self):
        try:
            self.mw.update_power_display_titles()
        except Exception:
            pass
        try:
            if hasattr(self.mw, "hv_voltage_label"):
                self.refresh_power_slot(0, self.mw.hv_voltage_label)
        except Exception:
            pass
        try:
            if hasattr(self.mw, "keithley_voltage_label"):
                self.refresh_power_slot(1, self.mw.keithley_voltage_label)
        except Exception:
            pass

    def update_keithley_voltage(self):
        try:
            self.refresh_power_voltage_slots()
        except Exception:
            pass

    def update_keithley_voltage_display(self, voltage, power_name: str | None = None):
        try:
            name = str(power_name or self.mw.connected_power_name_by_type.get("Keithley 248") or "").strip()
            if name:
                self.set_power_voltage_cache(name, float(voltage))
        except Exception:
            pass
        self.refresh_power_voltage_slots()

    def update_hv_voltage(self):
        try:
            if getattr(self.mw.hv_controller, "is_connected", False):
                voltage = float(getattr(self.mw, "_hv_v_cache", self.mw.hv_controller.actual_voltage))
                name = str(
                    self.mw.connected_power_name_by_type.get("HAPS06")
                    or self.mw.pending_haps06_power_name
                    or ""
                ).strip()
                if name:
                    self.set_power_voltage_cache(name, voltage)
            self.refresh_power_voltage_slots()
        except Exception:
            pass

    def update_hv_voltage_display(self, voltage, power_name: str | None = None):
        try:
            name = str(
                power_name
                or self.mw.connected_power_name_by_type.get("HAPS06")
                or self.mw.pending_haps06_power_name
                or ""
            ).strip()
            if name:
                self.set_power_voltage_cache(name, float(voltage))
        except Exception:
            pass
        self.refresh_power_voltage_slots()

    def update_status_display(self):
        try:
            self._poll_writer_warnings()
            if self._show_active_status_override():
                return
            if self.mw.countdown_manager.countdown == 0:
                self.mw.countdown_label.setText("")
                if getattr(self.mw.hv_controller, "is_connected", False):
                    voltage = self.mw.hv_controller.actual_voltage
                    self._show_status_message(f"高压电源运行中 - 当前电压: {voltage:.1f} V")
                else:
                    self._show_status_message("系统运行中 - 未连接高压电源")
        except Exception as exc:
            self.mw.log_message(f"Failed to update status display: {exc}")

    def update_countdown_display(self, countdown):
        try:
            if countdown > 0:
                self.mw.countdown_label.setText(f"循环等待: {countdown}秒")
            else:
                self.mw.countdown_label.setText("")
        except Exception as exc:
            self.mw.log_message(f"Failed to update countdown display: {exc}")

    def format_meter_text(self, meter_type: str, value, unit: str, kind: str = "") -> str:
        return format_measurement_value(value, unit)

    def update_meter_displays(self):
        try:
            for meter_type in self.METER_TYPES:
                value_label = getattr(self.mw, f"{meter_type}_value_label")
                self.mw.data_mutex.lock()
                try:
                    value = self.mw.meter_data[meter_type]["value"]
                    unit = self.mw.meter_data[meter_type]["unit"]
                    kind = self.mw.meter_data[meter_type].get("kind", "")
                finally:
                    self.mw.data_mutex.unlock()

                current_text = value_label.text()
                new_text = self.format_meter_text(meter_type, value, unit, kind)
                if current_text != new_text:
                    value_label.setText(new_text)
        except Exception:
            pass
