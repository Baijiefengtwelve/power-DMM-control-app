from __future__ import annotations

import time

from ..measurement_units import normalize_meter_measurement


class MeterDataService:
    """Own meter data normalization and meter state updates."""

    VACUUM_SCALE_TO_PA = {
        "mbar": 100.0,
        "mb": 100.0,
        "millibar": 100.0,
        "bar": 1.0e5,
        "torr": 133.32236842105263,
        "mtorr": 0.13332236842105263,
        "pa": 1.0,
    }

    def __init__(self, mw):
        self.mw = mw
        self._alarm_state = {}

    def handle_meter_data(self, data):
        try:
            meter_type = str(data["meter_name"]).strip()
            value = float(data["value"]) * self._read_meter_coefficient(meter_type)
            unit = str(data.get("unit", "") or "")
            kind = str(data.get("kind", "") or "")

            if meter_type == "vacuum":
                value, unit = self._normalize_vacuum_value(value, unit)
                kind = "vacuum"
            else:
                normalized = normalize_meter_measurement(value, unit, kind)
                value = normalized.value
                unit = normalized.unit
                kind = normalized.kind

            now = time.time()
            self._write_meter_state(meter_type, value, unit, kind, now)
            self._refresh_live_label_if_due(meter_type, value, unit, kind, now)
            self._process_threshold_alarm(meter_type, value, unit, kind, now)
            return {"meter_type": meter_type, "value": value, "unit": unit, "kind": kind}
        except Exception as exc:
            self.mw.log_message(f"Failed to process meter data: {exc}")
            return None

    def _read_meter_coefficient(self, meter_type: str) -> float:
        try:
            coeff_edit = getattr(self.mw, f"{meter_type}_coeff")
            return float(coeff_edit.text())
        except Exception:
            return 1.0

    def _normalize_vacuum_value(self, value, unit):
        try:
            normalized_unit = str(unit).strip().lower()
        except Exception:
            normalized_unit = "pa"

        try:
            numeric_value = float(value)
        except Exception:
            numeric_value = value

        scale = self.VACUUM_SCALE_TO_PA.get(normalized_unit)
        if scale is None:
            return numeric_value, "Pa"
        return float(numeric_value) * float(scale), "Pa"

    def _write_meter_state(self, meter_type: str, value, unit: str, kind: str, now: float):
        mutex = getattr(self.mw, "data_mutex", None)
        if mutex is not None:
            mutex.lock()
        try:
            self.mw.meter_data[meter_type]["value"] = float(value)
            self.mw.meter_data[meter_type]["unit"] = str(unit)
            self.mw.meter_data[meter_type]["kind"] = str(kind)
            self.mw.meter_data[meter_type]["timestamp"] = now
            self.mw.meter_data[meter_type]["valid"] = True
        finally:
            if mutex is not None:
                mutex.unlock()

    def _refresh_live_label_if_due(self, meter_type: str, value, unit: str, kind: str, now: float):
        if now - float(getattr(self.mw, "last_meter_update_time", 0.0)) <= float(
            getattr(self.mw, "meter_update_interval", 0.0)
        ):
            return

        try:
            value_label = getattr(self.mw, f"{meter_type}_value_label")
        except Exception:
            return

        try:
            formatter = getattr(self.mw.display_service, "format_meter_text")
            value_label.setText(formatter(meter_type, value, unit, kind))
        except Exception:
            value_label.setText(f"{value:.3f} {unit}")
        self.mw.last_meter_update_time = now

    def _process_threshold_alarm(self, meter_type: str, value, unit: str, kind: str, now: float):
        if str(kind or "").strip().lower() != "vacuum" and str(meter_type or "").strip().lower() != "vacuum":
            return
        if not self._read_safety_bool("vacuum_alarm_enabled", True):
            return
        self._handle_vacuum_alarm(float(value), str(unit or "Pa"), now)

    def _handle_vacuum_alarm(self, value_pa: float, unit: str, now: float):
        threshold_pa = self._read_safety_float("vacuum_alarm_max_pa", 1e-3)
        cooldown_s = max(0.0, self._read_safety_float("vacuum_alarm_cooldown_s", 10.0))
        action = self._read_safety_text("vacuum_alarm_action", "warn").strip().lower()
        state = self._alarm_state.setdefault(
            "vacuum",
            {"active": False, "last_alert_ts": 0.0},
        )

        if value_pa <= threshold_pa:
            if state.get("active"):
                self._emit_alarm_message(
                    f"真空度已恢复到安全范围: 当前 {value_pa:.3e} {unit}",
                    timeout_ms=3000,
                )
            state["active"] = False
            return

        should_alert = (not state.get("active")) or (
            (now - float(state.get("last_alert_ts", 0.0) or 0.0)) >= cooldown_s
        )
        if not should_alert:
            state["active"] = True
            return

        message = (
            f"真空度超限告警: 当前 {value_pa:.3e} {unit}，"
            f"阈值 {threshold_pa:.3e} Pa"
        )
        state["active"] = True
        state["last_alert_ts"] = now
        self._emit_alarm_message(message, timeout_ms=5000)
        self._apply_alarm_action(action, message)

    def _apply_alarm_action(self, action: str, message: str):
        normalized = str(action or "warn").strip().lower()
        if normalized in {"", "warn"}:
            return
        if normalized == "stop_test" and bool(getattr(self.mw, "is_testing", False)):
            self.mw.log_message("真空超限，已按配置停止测试")
            try:
                self.mw.stop_test()
            except Exception as exc:
                self.mw.log_message(f"自动停止测试失败: {exc}")
            return
        if normalized == "stop_stabilization" and bool(getattr(self.mw, "is_stabilizing", False)):
            self.mw.log_message("真空超限，已按配置停止稳流")
            try:
                self.mw.stop_current_stabilization()
            except Exception as exc:
                self.mw.log_message(f"自动停止稳流失败: {exc}")
            return
        if normalized == "emergency_stop":
            self.mw.log_message("真空超限，已按配置执行紧急停止")
            try:
                self.mw.emergency_stop()
            except Exception as exc:
                self.mw.log_message(f"自动紧急停止失败: {exc}")
            return
        self.mw.log_message(f"未知真空告警动作，已忽略: {action}; {message}")

    def _emit_alarm_message(self, message: str, *, timeout_ms: int):
        self.mw.log_message(message)
        try:
            self.mw.show_status_message(message, timeout_ms=timeout_ms)
        except Exception:
            pass

    def _read_safety_text(self, option: str, default):
        config = getattr(self.mw, "config", None)
        if config is None:
            return str(default)
        try:
            return str(config.get("Safety", option, fallback=default))
        except Exception:
            return str(default)

    def _read_safety_float(self, option: str, default: float) -> float:
        try:
            return float(self._read_safety_text(option, default))
        except Exception:
            return float(default)

    def _read_safety_bool(self, option: str, default: bool) -> bool:
        config = getattr(self.mw, "config", None)
        if config is not None:
            try:
                return bool(config.getboolean("Safety", option, fallback=default))
            except Exception:
                pass
        value = self._read_safety_text(option, default)
        return str(value).strip().lower() not in {"0", "false", "no", "off"}
