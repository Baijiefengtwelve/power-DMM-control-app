from __future__ import annotations

import threading
import time

from PyQt5.QtCore import QObject, pyqtSignal

from ..power_protocols import PowerControllerProtocol


class TestService(QObject):
    """Orchestrate single and cycle high-voltage tests."""

    INVALID_RANGE_MESSAGE = "\u9519\u8bef: \u8d77\u59cb\u7535\u538b\u548c\u76ee\u6807\u7535\u538b\u4e0d\u80fd\u76f8\u540c"
    INVALID_STEP_MESSAGE = "\u9519\u8bef: \u7535\u538b\u589e\u5e45\u5fc5\u987b\u5927\u4e8e0"
    ENABLE_FAILED_MESSAGE = "\u542f\u7528\u6d4b\u8bd5\u7535\u6e90\u5931\u8d25"
    ENABLE_ERROR_MESSAGE = "\u542f\u7528\u6d4b\u8bd5\u7535\u6e90\u5f02\u5e38"
    START_FAILED_MESSAGE = "\u6d4b\u8bd5\u542f\u52a8\u5931\u8d25"
    RUN_FAILED_MESSAGE = "\u6d4b\u8bd5\u8fd0\u884c\u5f02\u5e38"
    CONVERSION_BUSY_MESSAGE = "记录后处理尚未完成，请稍后再开始测试"
    VACUUM_CHECK_MISSING_MESSAGE = "真空计已连接，但当前没有有效读数，测试前检查未通过"
    POWER_PING_FAILED_MESSAGE = "测试前电源通信检查失败，无法读取当前电压"
    AUTO_RECORD_MESSAGES = {
        False: "\u5355\u6b21\u6d4b\u8bd5\u5df2\u81ea\u52a8\u5f00\u59cb\u8bb0\u5f55\u6570\u636e",
        True: "\u5faa\u73af\u6d4b\u8bd5\u5df2\u81ea\u52a8\u5f00\u59cb\u8bb0\u5f55\u6570\u636e",
    }
    TEST_MODE_NAMES = {
        False: "\u5355\u6b21\u6d4b\u8bd5",
        True: "\u5faa\u73af\u6d4b\u8bd5",
    }

    log = pyqtSignal(str)
    started = pyqtSignal(bool)
    finished = pyqtSignal()
    state_change = pyqtSignal(dict)

    def __init__(self, mw, parent=None):
        super().__init__(parent)
        self.mw = mw

    def start(self, cycle: bool):
        self._start_test(cycle=cycle)

    def stop(self):
        self.mw.is_testing = False

    def run_with_controller(
        self,
        controller: PowerControllerProtocol,
        start_voltage,
        target_voltage,
        voltage_step,
        step_delay,
        cycle_time,
        is_cycle,
    ):
        return self._run_test(
            controller,
            start_voltage,
            target_voltage,
            voltage_step,
            step_delay,
            cycle_time,
            is_cycle,
        )

    def _start_test(self, cycle: bool = False):
        try:
            params = self._read_test_params(cycle)
            if not self._validate_test_request(params):
                return

            self._configure_test_mode(params["start_voltage"], params["target_voltage"])
            controller, source_key = self._resolve_controller_or_abort()
            if controller is None:
                return
            if not self._run_preflight_checks(controller, source_key, params):
                return

            self._activate_test_runtime(controller, source_key, cycle)
            self._start_auto_recording_if_needed(cycle)
            self._initialize_cycle_state(cycle)
            self._log_test_start(cycle, source_key)
            self._spawn_test_thread(controller, params, cycle)
            self.started.emit(cycle)
        except Exception as exc:
            self.log.emit(f"{self.START_FAILED_MESSAGE}: {exc}")

    def _read_test_params(self, cycle: bool):
        return {
            "start_voltage": self.mw.test_params["start_voltage"],
            "target_voltage": self.mw.test_params["target_voltage"],
            "voltage_step": self.mw.test_params["voltage_step"],
            "step_delay": self.mw.test_params["step_delay"],
            "cycle_time": self.mw.test_params["cycle_time"] if cycle else 0,
        }

    def _validate_test_request(self, params) -> bool:
        if getattr(self.mw, "is_converting", False):
            self.log.emit(self.CONVERSION_BUSY_MESSAGE)
            return False
        if params["start_voltage"] == params["target_voltage"]:
            self.log.emit(self.INVALID_RANGE_MESSAGE)
            return False
        if params["voltage_step"] <= 0:
            self.log.emit(self.INVALID_STEP_MESSAGE)
            return False
        return True

    def _selected_source_interlock_error(self) -> str:
        return self.mw.power_catalog_service.validate_selected_power_interlock(
            test_source_name=self.mw._get_selected_power_name("test"),
            stabilization_source_name=self.mw._get_selected_power_name("stabilization"),
        )

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

    def _show_preflight_status(self, message: str, *, timeout_ms: int = 5000):
        try:
            self.mw.show_status_message(message, timeout_ms=timeout_ms)
        except Exception:
            pass

    def _run_preflight_checks(self, controller, source_key: str, params) -> bool:
        self._warn_if_step_delay_is_too_short(params)
        if not self._check_vacuum_preflight():
            self.mw.update_power_action_buttons()
            return False
        if not self._check_power_connectivity(controller, source_key):
            self.mw.update_power_action_buttons()
            return False
        return True

    def _warn_if_step_delay_is_too_short(self, params):
        warning_threshold = self._read_safety_float("short_step_delay_warning_s", 0.5)
        try:
            step_delay = float(params.get("step_delay", 0.0) or 0.0)
        except Exception:
            return
        if step_delay >= warning_threshold:
            return
        message = (
            f"警告: 当前步进延时 {step_delay:g}s 低于建议值 {warning_threshold:g}s，"
            "万用表读数可能尚未稳定"
        )
        self.log.emit(message)
        self._show_preflight_status(message)

    def _check_vacuum_preflight(self) -> bool:
        if not self._should_check_vacuum_preflight():
            return True

        vacuum_state = self._read_meter_snapshot("vacuum")
        threshold_pa = self._read_safety_float("preflight_vacuum_max_pa", 1e-3)
        max_age_s = max(0.0, self._read_safety_float("preflight_vacuum_max_age_s", 5.0))
        now = time.time()

        if not vacuum_state.get("valid", False):
            self.log.emit(self.VACUUM_CHECK_MISSING_MESSAGE)
            self._show_preflight_status(self.VACUUM_CHECK_MISSING_MESSAGE)
            return False

        timestamp = float(vacuum_state.get("timestamp", 0.0) or 0.0)
        if max_age_s > 0 and timestamp > 0 and (now - timestamp) > max_age_s:
            message = (
                f"真空计读数已超过 {max_age_s:g}s 未更新，"
                "测试前检查未通过"
            )
            self.log.emit(message)
            self._show_preflight_status(message)
            return False

        try:
            value_pa = float(vacuum_state.get("value", 0.0) or 0.0)
        except Exception:
            self.log.emit(self.VACUUM_CHECK_MISSING_MESSAGE)
            self._show_preflight_status(self.VACUUM_CHECK_MISSING_MESSAGE)
            return False

        if value_pa > threshold_pa:
            message = (
                f"真空度检查未通过: 当前 {value_pa:.3e} Pa，"
                f"要求不高于 {threshold_pa:.3e} Pa"
            )
            self.log.emit(message)
            self._show_preflight_status(message)
            return False
        return True

    def _should_check_vacuum_preflight(self) -> bool:
        try:
            checker = getattr(self.mw, "is_meter_connected", None)
            if callable(checker):
                return bool(checker("vacuum"))
        except Exception:
            pass
        try:
            return bool(self.mw.meter_data.get("vacuum", {}).get("valid", False))
        except Exception:
            return False

    def _read_meter_snapshot(self, meter_type: str) -> dict:
        mutex = getattr(self.mw, "data_mutex", None)
        if mutex is not None:
            try:
                mutex.lock()
            except Exception:
                mutex = None
        try:
            return dict(getattr(self.mw, "meter_data", {}).get(meter_type, {}) or {})
        finally:
            if mutex is not None:
                try:
                    mutex.unlock()
                except Exception:
                    pass

    def _check_power_connectivity(self, controller, source_key: str) -> bool:
        source_name = self.mw._power_source_name(source_key)
        voltage = self._read_controller_voltage(controller)
        if voltage is None:
            message = f"{self.POWER_PING_FAILED_MESSAGE}: {source_name}"
            self.log.emit(message)
            self._show_preflight_status(message)
            return False

        self.log.emit(
            f"测试前电源通信检查通过: {source_name} 当前电压 {float(voltage):.3f}V"
        )
        return True

    def _read_controller_voltage(self, controller):
        for method_name in ("read_actual_voltage", "read_voltage", "read_set_voltage"):
            try:
                reader = getattr(controller, method_name, None)
            except Exception:
                reader = None
            if not callable(reader):
                continue
            try:
                value = reader()
            except Exception:
                continue
            if value is None:
                continue
            try:
                return float(value)
            except Exception:
                continue
        return None

    def _configure_test_mode(self, start_voltage, target_voltage):
        if start_voltage < target_voltage:
            self.mw.test_mode = "\u5347\u538b"
            self.log.emit(
                f"\u68c0\u6d4b\u5230\u5347\u538b\u6d4b\u8bd5\u6a21\u5f0f: {start_voltage}V -> {target_voltage}V"
            )
        else:
            self.mw.test_mode = "\u964d\u538b"
            self.log.emit(
                f"\u68c0\u6d4b\u5230\u964d\u538b\u6d4b\u8bd5\u6a21\u5f0f: {start_voltage}V -> {target_voltage}V"
            )

    def _resolve_controller_or_abort(self):
        selection_error = self._selected_source_interlock_error()
        if selection_error:
            self.log.emit(selection_error)
            self.mw.update_power_action_buttons()
            return None, None

        controller, source_key, err = self.mw._resolve_power_controller("test")
        if controller is None:
            self.log.emit(f"\u9519\u8bef: {err}")
            return None, None

        runtime_error = self.mw.power_catalog_service.runtime_power_interlock_error("test", source_key)
        if runtime_error:
            self.log.emit(runtime_error)
            self.mw.update_power_action_buttons()
            return None, None
        return controller, source_key

    def _activate_test_runtime(self, controller, source_key, cycle: bool):
        self.mw.active_test_controller = controller
        self.mw.active_test_power_source = source_key
        self.mw.is_testing = True
        self.mw.is_cycle_testing = cycle
        self.mw.test_run_started_at = time.time()
        self.mw.test_run_source_name = self.mw._power_source_name(source_key)
        self.mw.test_run_cycle_mode = bool(cycle)
        self.mw._test_run_stabilization_baseline = {
            "completion": int(getattr(self.mw, "stabilization_completion_count", 0) or 0),
            "failure": int(getattr(self.mw, "stabilization_failure_count", 0) or 0),
        }
        self.mw.last_test_run_summary = ""
        self.state_change.emit({"testing": True, "cycle": cycle, "countdown_stop": True})

    def _start_auto_recording_if_needed(self, cycle: bool):
        if getattr(self.mw, "is_converting", False):
            self.log.emit(self.CONVERSION_BUSY_MESSAGE)
            return
        if self.mw.has_record_file_path() and not self.mw.is_recording:
            self.mw.auto_recording = True
            self.mw.toggle_record()
            self.log.emit(self.AUTO_RECORD_MESSAGES[cycle])

    def _initialize_cycle_state(self, cycle: bool):
        if not cycle:
            return
        self.mw.current_cycle = 0
        self.mw.cycle_data = []
        self.mw.current_cycle_anode_data = []

    def _log_test_start(self, cycle: bool, source_key: str):
        mode_name = self.TEST_MODE_NAMES[cycle]
        source_name = self.mw._power_source_name(source_key)
        self.log.emit(f"\u5f00\u59cb{mode_name}... \u5f53\u524d\u6d4b\u8bd5\u7535\u6e90: {source_name}")

    def _spawn_test_thread(self, controller, params, cycle: bool):
        thread = threading.Thread(
            target=self.run_with_controller,
            args=(
                controller,
                params["start_voltage"],
                params["target_voltage"],
                params["voltage_step"],
                params["step_delay"],
                params["cycle_time"],
                cycle,
            ),
            daemon=True,
        )
        thread.start()

    def _run_test(
        self,
        controller: PowerControllerProtocol,
        start_voltage,
        target_voltage,
        voltage_step,
        step_delay,
        cycle_time,
        is_cycle,
    ):
        try:
            if not self._enable_controller_or_abort(controller, is_cycle):
                return
            self._run_cycle_loop(
                controller,
                start_voltage,
                target_voltage,
                voltage_step,
                step_delay,
                cycle_time,
                is_cycle,
            )
        except Exception as exc:
            self.log.emit(f"{self.RUN_FAILED_MESSAGE}: {exc}")
        finally:
            self._clear_runtime_state(is_cycle)
            self.finished.emit()

    def _enable_controller_or_abort(self, controller, is_cycle: bool) -> bool:
        try:
            ok_hv, msg_hv = controller.enable_high_voltage()
        except Exception as exc:
            self.log.emit(f"{self.ENABLE_ERROR_MESSAGE}: {exc}")
            self._mark_test_stopped(is_cycle)
            return False

        if ok_hv:
            self.log.emit(f"\u6d4b\u8bd5\u7535\u6e90\u5df2\u5c31\u7eea: {msg_hv}")
            return True

        self.log.emit(f"{self.ENABLE_FAILED_MESSAGE}: {msg_hv}")
        self._mark_test_stopped(is_cycle)
        return False

    def _run_cycle_loop(
        self,
        controller,
        start_voltage,
        target_voltage,
        voltage_step,
        step_delay,
        cycle_time,
        is_cycle,
    ):
        cycle_count = 0
        while self._should_continue_cycle_loop(is_cycle, cycle_count):
            cycle_count += 1
            self.mw.current_cycle = cycle_count
            self.log.emit(f"\u5f00\u59cb\u7b2c {cycle_count} \u8f6e\u6d4b\u8bd5")

            self._write_cycle_marker_if_needed(cycle_count, is_cycle)
            self._prepare_cycle_recording(is_cycle)

            if not self._set_cycle_start_voltage(controller, start_voltage, step_delay):
                break

            ramp_failed = self._run_voltage_ramp(
                controller,
                start_voltage,
                target_voltage,
                voltage_step,
                step_delay,
            )
            if not self.mw.is_testing:
                break

            if not is_cycle:
                self._finalize_single_test(controller, ramp_failed)
                break

            if not self._finalize_cycle_iteration(controller, cycle_time):
                break

    def _should_continue_cycle_loop(self, is_cycle: bool, cycle_count: int) -> bool:
        return self.mw.is_testing and (is_cycle or cycle_count == 0)

    def _write_cycle_marker_if_needed(self, cycle_count: int, is_cycle: bool):
        if not (is_cycle and self.mw.is_recording):
            return
        try:
            self.mw.data_saver.add_marker_row(f"\u7b2c{cycle_count}\u6b21\u5faa\u73af")
            self.log.emit(f"\u5df2\u5199\u5165\u7b2c{cycle_count}\u6b21\u5faa\u73af\u6807\u8bb0\u884c")
        except Exception as exc:
            self.log.emit(f"\u5199\u5165\u5faa\u73af\u6807\u8bb0\u884c\u5931\u8d25: {exc}")

    def _prepare_cycle_recording(self, is_cycle: bool):
        if is_cycle and self.mw.is_recording:
            self.mw.current_cycle_anode_data = []

    def _set_cycle_start_voltage(self, controller, start_voltage, step_delay) -> bool:
        ok, msg = controller.set_voltage_only(start_voltage)
        if not ok:
            self.log.emit(f"\u8bbe\u7f6e\u8d77\u59cb\u7535\u538b\u5931\u8d25: {msg}")
            return False

        self._publish_test_voltage_update(controller, start_voltage)
        self.log.emit(f"\u8bbe\u7f6e\u8d77\u59cb\u7535\u538b: {start_voltage:.1f}V - {msg}")
        time.sleep(step_delay * 0.5)
        time.sleep(step_delay)
        return True

    def _run_voltage_ramp(
        self,
        controller,
        start_voltage,
        target_voltage,
        voltage_step,
        step_delay,
    ) -> bool:
        if self.mw.is_cycle_testing and self.mw.is_recording:
            self.mw.cycle_recording_active = True
            self.log.emit("\u6d4b\u8bd5\u671f\u95f4\u6570\u636e\u8bb0\u5f55\u5df2\u6fc0\u6d3b")

        if self.mw.test_mode == "\u5347\u538b":
            return self._run_ramp(controller, start_voltage, target_voltage, voltage_step, step_delay, 1)
        return self._run_ramp(controller, start_voltage, target_voltage, voltage_step, step_delay, -1)

    def _run_ramp(
        self,
        controller,
        start_voltage,
        target_voltage,
        voltage_step,
        step_delay,
        direction: int,
    ) -> bool:
        current_voltage = start_voltage
        while self._should_continue_ramp(current_voltage, target_voltage, direction):
            ok, msg = controller.set_voltage_only(current_voltage)
            if ok:
                self._publish_test_voltage_update(controller, current_voltage)
                self.log.emit(f"\u8bbe\u7f6e\u7535\u538b: {current_voltage:.1f}V - {msg}")
            else:
                self.log.emit(f"\u8bbe\u7f6e\u7535\u538b\u5931\u8d25: {msg}")
                return True
            current_voltage += voltage_step * direction
            time.sleep(step_delay)
        return False

    def _should_continue_ramp(self, current_voltage, target_voltage, direction: int) -> bool:
        if not self.mw.is_testing:
            return False
        if direction > 0:
            return current_voltage <= target_voltage
        return current_voltage >= target_voltage

    def _finalize_single_test(self, controller, ramp_failed: bool):
        try:
            if ramp_failed:
                self.log.emit(
                    "\u5355\u6b21\u6d4b\u8bd5\u672a\u5b8c\u6574\u5230\u8fbe\u76ee\u6807\u7535\u538b\uff0c\u5c1d\u8bd5\u5b89\u5168\u5f52\u96f6\u8f93\u51fa"
                )
            else:
                self.log.emit("\u5355\u6b21\u6d4b\u8bd5\u5b8c\u6210\uff0c\u5b89\u5168\u5f52\u96f6\u8f93\u51fa")
            ok, msg = controller.stop_output()
            if ok:
                self._publish_test_voltage_update(controller, 0.0)
                self.log.emit(f"\u7535\u538b\u5df2\u5b89\u5168\u5f52\u96f6 - {msg}")
            else:
                self.log.emit(f"\u7535\u538b\u5f52\u96f6\u5931\u8d25: {msg}")
        except Exception as exc:
            self.log.emit(f"\u5355\u6b21\u6d4b\u8bd5\u5b89\u5168\u5f52\u96f6\u5931\u8d25: {exc}")

    def _finalize_cycle_iteration(self, controller, cycle_time) -> bool:
        self._save_cycle_min_if_needed()
        self._pause_cycle_recording_if_needed()
        self.log.emit(f"\u5230\u8fbe\u76ee\u6807\u7535\u538b\uff0c\u5b89\u5168\u5f52\u96f6\u7b49\u5f85 {cycle_time} \u79d2")
        drop_ok, drop_msg = controller.stop_output()
        if not drop_ok:
            self.log.emit(f"\u5f52\u96f6\u5931\u8d25: {drop_msg}")
            return False

        self._publish_test_voltage_update(controller, 0.0)
        self.log.emit(f"\u5f52\u96f6\u6210\u529f: {drop_msg}")
        self._run_cycle_countdown(cycle_time)
        return True

    def _publish_test_voltage_update(self, controller, voltage):
        mw = self.mw
        try:
            value = float(voltage)
        except Exception:
            return

        source_name = str(getattr(mw, "active_test_power_source", "") or "").strip()
        power_type = self._detect_test_power_type(source_name)

        try:
            if controller is not None:
                if hasattr(controller, "current_voltage"):
                    controller.current_voltage = value
                if hasattr(controller, "actual_voltage"):
                    controller.actual_voltage = value
                if hasattr(controller, "set_voltage_value"):
                    controller.set_voltage_value = value
        except Exception:
            pass

        now_ts = time.time()
        try:
            if power_type == "Keithley 248":
                mw._keithley_v_cache = value
                mw._keithley_v_ts = now_ts
                if hasattr(mw, "update_keithley_voltage_display"):
                    mw.update_keithley_voltage_display(value, power_name=source_name or None)
                    return
            elif power_type == "HAPS06":
                mw._hv_v_cache = value
                mw._hv_v_ts = now_ts
                if hasattr(mw, "update_hv_voltage_display"):
                    mw.update_hv_voltage_display(value, power_name=source_name or None)
                    return
        except Exception:
            pass

        try:
            if source_name:
                mw._set_power_voltage_cache(source_name, value)
        except Exception:
            pass
        try:
            mw.refresh_power_voltage_slots()
        except Exception:
            pass

    def _detect_test_power_type(self, source_name: str) -> str:
        clean_name = str(source_name or "").strip()
        if not clean_name:
            return ""

        try:
            finder = getattr(self.mw, "_find_power_device", None)
            device = finder(clean_name) if callable(finder) else None
        except Exception:
            device = None

        raw_type = str((device or {}).get("type", "") or "").strip().lower()
        if not raw_type:
            return ""
        if "keithley" in raw_type or "248" in raw_type or "2290" in raw_type:
            return "Keithley 248"
        return "HAPS06"

    def _save_cycle_min_if_needed(self):
        if self.mw.is_recording and self.mw.current_cycle_anode_data:
            try:
                self.mw.calculate_and_save_cycle_min()
            except Exception as exc:
                self.log.emit(f"\u8ba1\u7b97\u5e76\u8bb0\u5f55\u5faa\u73af\u6700\u5c0f\u503c\u5931\u8d25: {exc}")

    def _pause_cycle_recording_if_needed(self):
        if self.mw.is_recording:
            self.mw.cycle_recording_active = False
            self.log.emit("\u7b49\u5f85\u671f\u95f4\u6570\u636e\u8bb0\u5f55\u5df2\u6682\u505c")

    def _run_cycle_countdown(self, cycle_time):
        total_seconds = max(0, int(float(cycle_time or 0)))
        try:
            self.state_change.emit({"countdown_start": total_seconds, "countdown_tick": total_seconds})
        except Exception:
            pass

        end_t = time.time() + max(0.0, float(cycle_time or 0))
        last_remaining = None
        while self.mw.is_testing:
            remaining = max(0, int(end_t - time.time() + 0.999))
            if remaining != last_remaining:
                last_remaining = remaining
                try:
                    self.state_change.emit({"countdown_tick": remaining})
                except Exception:
                    pass
            if time.time() >= end_t:
                break
            time.sleep(0.2)

        try:
            self.state_change.emit({"countdown_stop": True})
        except Exception:
            pass

    def _mark_test_stopped(self, is_cycle: bool):
        self.mw.is_testing = False
        self.mw.is_cycle_testing = False
        if is_cycle and getattr(self.mw, "is_recording", False):
            self.mw.cycle_recording_active = False

    def _clear_runtime_state(self, is_cycle: bool):
        self._mark_test_stopped(is_cycle)
        self.mw.active_test_controller = None
        self.mw.active_test_power_source = None
        self.state_change.emit({"testing": False, "cycle": is_cycle, "countdown_stop": True})

    def _calculate_and_save_cycle_min(self):
        try:
            self.mw.calculate_and_save_cycle_min()
        except Exception as exc:
            self.log.emit(f"\u8ba1\u7b97\u5e76\u8bb0\u5f55\u5faa\u73af\u6700\u5c0f\u503c\u5931\u8d25: {exc}")
