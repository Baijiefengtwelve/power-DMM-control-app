from __future__ import annotations

import time

from PyQt5.QtCore import QThread, pyqtSignal

from .common import logger
from .measurement_units import (
    STANDARD_CURRENT_UNIT,
    format_measurement_value,
    infer_measurement_kind,
    standard_unit_for_kind,
)
from .power_protocols import PowerControllerProtocol


class PIDController:
    """PID controller with deadband and basic anti-windup."""

    def __init__(self, kp=0.05, ki=0.01, kd=0.0):
        self.kp = kp
        self.ki = ki
        self.kd = kd

        self.integral = 0.0
        self.previous_error = 0.0
        self.integral_limit = 1e6
        self.output_limit = 50.0

    def calculate(self, setpoint, measured, dt=1.0, deadband=0.0):
        if dt is None or dt <= 0:
            dt = 1.0

        error = float(setpoint) - float(measured)
        if deadband and abs(error) <= float(deadband):
            self.integral *= 0.9
            self.previous_error = error
            return 0.0

        if self.previous_error != 0 and (error * self.previous_error) < 0:
            self.integral = 0.0

        p_term = self.kp * error
        d_term = self.kd * (error - self.previous_error) / dt
        pre_output = p_term + d_term

        integral_candidate = self.integral + error * dt
        if self.ki != 0:
            self.integral_limit = max(self.integral_limit, abs(2.0 * self.output_limit / self.ki))
        if integral_candidate > self.integral_limit:
            integral_candidate = self.integral_limit
        elif integral_candidate < -self.integral_limit:
            integral_candidate = -self.integral_limit

        i_term = self.ki * integral_candidate
        output = pre_output + i_term

        output_sat = output
        if output_sat > self.output_limit:
            output_sat = self.output_limit
        elif output_sat < -self.output_limit:
            output_sat = -self.output_limit

        if output != output_sat:
            if (output_sat >= self.output_limit and error > 0) or (output_sat <= -self.output_limit and error < 0):
                pass
            else:
                self.integral = integral_candidate
        else:
            self.integral = integral_candidate

        self.previous_error = error
        return float(output_sat)

    def reset(self):
        self.integral = 0.0
        self.previous_error = 0.0


class CurrentStabilizationThread(QThread):
    """Feedback stabilization thread for current or voltage sources."""

    SOURCE_LABELS = {
        "keithley": "Keithley",
        "cathode": "阴极",
        "gate": "栅极",
        "anode": "阳极",
        "backup": "收集极",
    }
    KIND_LABELS = {
        "current": "电流",
        "voltage": "电压",
    }

    update_voltage_signal = pyqtSignal(float)
    update_status_signal = pyqtSignal(str)
    stabilization_complete_signal = pyqtSignal()

    def __init__(self, keithley_controller: PowerControllerProtocol, meter_data, data_mutex, params):
        super().__init__()
        self.keithley_controller = keithley_controller
        self.meter_data = meter_data
        self.data_mutex = data_mutex
        self.params = params
        self.running = False
        self.pid = PIDController()
        self.status_emit_interval_sec = 1.0
        self._last_status_ts = 0.0
        self._last_status_message = ""
        self._status_emit_callback = self.update_status_signal.emit
        self._set_voltage = 0.0
        self._qt_signals_suppressed = False

    def suppress_qt_signals(self):
        self._qt_signals_suppressed = True

    def _emit_status(self, message: str, *, force: bool = False, min_interval: float | None = None):
        if self._qt_signals_suppressed:
            return
        text = str(message)
        now_ts = time.monotonic()
        interval = self.status_emit_interval_sec if min_interval is None else max(0.0, float(min_interval))
        if not force and text == self._last_status_message and (now_ts - self._last_status_ts) < interval:
            return
        if not force and (now_ts - self._last_status_ts) < interval:
            return

        self._last_status_ts = now_ts
        self._last_status_message = text
        try:
            self._status_emit_callback(text)
        except Exception:
            pass

    def _emit_voltage(self, value: float):
        if self._qt_signals_suppressed:
            return
        try:
            self.update_voltage_signal.emit(float(value))
        except Exception:
            pass

    def _emit_complete(self):
        if self._qt_signals_suppressed:
            return
        try:
            self.stabilization_complete_signal.emit()
        except Exception:
            pass

    def _safe_shutdown_output(self):
        controller = self.keithley_controller
        if controller is None or not bool(getattr(controller, "is_connected", False)):
            return

        try:
            stop_output = getattr(controller, "stop_output", None)
            if callable(stop_output):
                stop_output()
                return
        except Exception:
            pass

        try:
            set_voltage_only = getattr(controller, "set_voltage_only", None)
            if callable(set_voltage_only):
                set_voltage_only(0.0)
            else:
                set_voltage = getattr(controller, "set_voltage", None)
                if callable(set_voltage):
                    set_voltage(0.0)
        except Exception:
            pass

        try:
            disable_hv = getattr(controller, "disable_high_voltage", None)
            if callable(disable_hv):
                disable_hv()
        except Exception:
            pass

    def _configure_pid_from_params(self):
        for param_key, attr_name in (
            ("pid_kp", "kp"),
            ("pid_ki", "ki"),
            ("pid_kd", "kd"),
        ):
            try:
                value = float(self.params.get(param_key, getattr(self.pid, attr_name)))
            except Exception:
                continue
            setattr(self.pid, attr_name, value)

    def run(self):
        self.running = True
        self._emit_status("开始稳流控制...", force=True)

        try:
            self._configure_pid_from_params()
            self.pid.output_limit = float(self.params.get("max_adjust_voltage", self.pid.output_limit))
        except Exception:
            pass
        self.pid.reset()

        target_value = float(self.params.get("target_current", 0.0))
        deadband = float(self.params.get("stability_range", 0.0))
        algo = str(self.params.get("algorithm", "pid") or "pid").strip().lower()
        if algo not in ("pid", "approach"):
            algo = "pid"

        adjust_period = float(self.params.get("adjust_frequency", 1.0))
        if adjust_period < 0.5:
            adjust_period = 0.5
        sleep_period = adjust_period
        feedback_loss_limit = max(1, int(float(self.params.get("feedback_loss_limit", 10)) or 10))
        feedback_loss_count = 0

        stable_notified = False
        feedback_kind = ""
        feedback_unit = ""

        start_v = float(self.params.get("start_voltage", 0.0))
        if self.keithley_controller.is_connected:
            success, message = self.keithley_controller.set_voltage(start_v)
            if success:
                self._emit_status(f"设置起始电压: {start_v:.1f}V", force=True)
            else:
                self._emit_status(f"设置起始电压失败: {message}", force=True)
                self.running = False
                return

        polarity = -1.0 if start_v < 0 else 1.0
        self._set_voltage = start_v
        set_u = abs(polarity * self._set_voltage)
        self._emit_voltage(self._set_voltage)

        filt_alpha = float(self.params.get("current_filter_alpha", 0.3))
        if filt_alpha <= 0 or filt_alpha >= 1:
            filt_alpha = 0.3
        feedback_filt = None

        slope_est = None
        slope_alpha = 0.4
        last_u_for_slope = None
        last_value_for_slope = None

        coarse_mode = False
        coarse_enter_mult = float(self.params.get("coarse_enter_mult", 6.0))
        coarse_exit_mult = float(self.params.get("coarse_exit_mult", 2.5))
        if coarse_exit_mult >= coarse_enter_mult:
            coarse_exit_mult = max(1.5, coarse_enter_mult * 0.5)

        sign_flip_window = []
        last_err_sign = 0

        settle_time = float(self.params.get("settle_time", sleep_period))
        if settle_time < 0:
            settle_time = 0.0
        last_set_time = time.time()

        success, message = self.keithley_controller.enable_high_voltage()
        if success:
            self._emit_status("高压输出已开启", force=True)
        else:
            self._emit_status(f"开启高压输出失败: {message}", force=True)
            self.running = False
            return

        time.sleep(max(0.3, settle_time))

        last_loop_t = time.time()
        while self.running:
            now_loop_t = time.time()
            dt_loop = max(1e-3, now_loop_t - last_loop_t)
            last_loop_t = now_loop_t

            try:
                reading = self.get_feedback_reading()
                if reading is None:
                    feedback_loss_count += 1
                    self._emit_status("无法获取反馈值", min_interval=max(2.0, sleep_period))
                    if feedback_loss_count >= feedback_loss_limit:
                        self._emit_status("反馈信号连续丢失，已执行安全关断", force=True)
                        self._safe_shutdown_output()
                        self.running = False
                        break
                    time.sleep(sleep_period)
                    continue
                feedback_loss_count = 0
                feedback_value = float(reading["value"])
                reading_kind = str(reading["kind"])
                reading_unit = str(reading["unit"])
                metric_label = self._kind_label(reading_kind)

                if reading_kind not in ("current", "voltage") or not reading_unit:
                    self._emit_status("反馈源当前不是电压或电流数据", min_interval=max(2.0, sleep_period))
                    time.sleep(sleep_period)
                    continue

                if reading_kind != feedback_kind or reading_unit != feedback_unit:
                    feedback_kind = reading_kind
                    feedback_unit = reading_unit
                    stable_notified = False
                    feedback_filt = None
                    slope_est = None
                    last_u_for_slope = None
                    last_value_for_slope = None
                    sign_flip_window = []
                    last_err_sign = 0
                    self._reset_feedback_state()
                    self._emit_status(
                        f"反馈源: {self._source_label()}，当前按 {feedback_unit} 解释目标值与稳定范围",
                        force=True,
                    )

                error = target_value - feedback_value
                if deadband and abs(error) <= deadband:
                    if not stable_notified:
                        stable_notified = True
                        self._emit_status(
                            (
                                f"{metric_label}进入稳定区间: {self._format_feedback(feedback_value, feedback_unit)} "
                                f"(目标 {target_value:g} {feedback_unit}, ±{deadband:g} {feedback_unit})"
                            ),
                            force=True,
                        )
                        self._emit_complete()

                    try:
                        self.pid.calculate(target_value, feedback_value, dt=dt_loop, deadband=deadband)
                    except Exception:
                        pass
                    time.sleep(sleep_period)
                    continue

                stable_notified = False

                if algo == "approach":
                    lower = target_value - deadband
                    upper = target_value + deadband
                    du = 0.0
                    try:
                        if feedback_value < lower:
                            du = 1.0
                        elif feedback_value > upper:
                            du = -1.0
                    except Exception:
                        du = 0.0

                    if (time.time() - last_set_time) < settle_time:
                        time.sleep(sleep_period)
                        continue

                    try:
                        eff_max_step = float(self.params.get("max_adjust_voltage", 50.0))
                        if eff_max_step > 0:
                            if du > eff_max_step:
                                du = eff_max_step
                            elif du < -eff_max_step:
                                du = -eff_max_step
                    except Exception:
                        eff_max_step = 0.0

                    if abs(float(du)) < 1e-9:
                        self._emit_status(
                            f"[接近] {metric_label}={self._format_feedback(feedback_value, feedback_unit)}, 保持 Vset={self._set_voltage:.1f}V",
                            min_interval=max(1.5, sleep_period),
                        )
                        time.sleep(sleep_period)
                        continue

                    set_u = float(set_u) + float(du)
                    if set_u < 0:
                        set_u = 0.0

                    new_voltage = polarity * set_u
                    success, message = self.keithley_controller.set_voltage(new_voltage)
                    if success:
                        self._set_voltage = new_voltage
                        self._emit_voltage(new_voltage)
                        last_set_time = time.time()
                        self._emit_status(
                            (
                                f"[接近] {metric_label}={self._format_feedback(feedback_value, feedback_unit)}, "
                                f"目标={target_value:g} {feedback_unit}, ΔV={du:.0f}V, Vset={new_voltage:.1f}V"
                            ),
                            min_interval=max(1.0, sleep_period),
                        )
                    else:
                        self._emit_status(f"[接近] 设置电压失败: {message}", min_interval=max(2.0, sleep_period))

                    time.sleep(sleep_period)
                    continue

                if feedback_filt is None:
                    feedback_filt = feedback_value
                else:
                    feedback_filt = float(filt_alpha) * feedback_value + (1.0 - float(filt_alpha)) * float(feedback_filt)

                if last_u_for_slope is not None and abs(float(set_u) - float(last_u_for_slope)) > 1e-9:
                    delta_value = float(feedback_filt) - float(last_value_for_slope)
                    delta_u = float(set_u) - float(last_u_for_slope)
                    slope = delta_value / delta_u
                    if slope > 1e-9 and slope < 1e6:
                        if slope_est is None:
                            slope_est = slope
                        else:
                            slope_est = slope_alpha * slope + (1.0 - slope_alpha) * slope_est

                abs_err = abs(error)
                db = float(deadband) if deadband and deadband > 0 else 1e-9
                enter_th = coarse_enter_mult * db
                exit_th = coarse_exit_mult * db

                if coarse_mode:
                    if abs_err < exit_th:
                        coarse_mode = False
                else:
                    if abs_err > enter_th:
                        coarse_mode = True

                err_sign = 1 if error > 0 else (-1 if error < 0 else 0)
                now_t = time.time()
                if last_err_sign != 0 and err_sign != 0 and err_sign != last_err_sign and abs_err > db:
                    sign_flip_window.append(now_t)
                    sign_flip_window = [t for t in sign_flip_window if now_t - t <= 12.0]
                last_err_sign = err_sign

                max_step = float(self.pid.output_limit) if self.pid.output_limit else 0.0
                flips = len(sign_flip_window)
                osc_decay = 1.0
                if flips >= 3:
                    osc_decay = max(0.1, 0.5 ** (flips - 2))
                eff_max_step = max_step * osc_decay if max_step > 0 else 0.0

                if (time.time() - last_set_time) < settle_time:
                    du = 0.0
                else:
                    if coarse_mode and eff_max_step > 0:
                        if slope_est is not None and slope_est > 0:
                            du = float(error) / float(slope_est)
                        else:
                            k_coarse = eff_max_step / max(enter_th, 1e-9)
                            du = float(k_coarse) * float(error)

                        if du > eff_max_step:
                            du = eff_max_step
                        elif du < -eff_max_step:
                            du = -eff_max_step
                    else:
                        du = self.pid.calculate(
                            target_value,
                            feedback_filt,
                            dt=dt_loop,
                            deadband=deadband,
                        )

                if error < 0 and du > 0:
                    du = 0.0
                if error > 0 and du < 0:
                    du = 0.0

                if du != 0.0:
                    eff_min_step = 1.0
                    if isinstance(eff_max_step, (int, float)) and eff_max_step > 0:
                        eff_min_step = min(1.0, float(eff_max_step))
                    if abs(float(du)) < eff_min_step:
                        du = eff_min_step if float(du) > 0 else -eff_min_step

                if abs(float(du)) < 1e-9:
                    self._emit_status(
                        f"{metric_label}={self._format_feedback(feedback_value, feedback_unit)}, 保持 Vset={self._set_voltage:.1f}V",
                        min_interval=max(1.5, sleep_period),
                    )
                    time.sleep(sleep_period)
                    continue

                last_u_for_slope = float(set_u)
                last_value_for_slope = float(feedback_filt) if feedback_filt is not None else feedback_value

                set_u = float(set_u) + float(du)
                if set_u < 0:
                    set_u = 0.0

                new_voltage = polarity * set_u
                success, message = self.keithley_controller.set_voltage(new_voltage)
                if success:
                    self._set_voltage = new_voltage
                    self._emit_voltage(new_voltage)
                    last_set_time = time.time()
                    self._emit_status(
                        (
                            f"{metric_label}={self._format_feedback(feedback_value, feedback_unit)}, "
                            f"目标={target_value:g} {feedback_unit}, ΔV={du:.2f}V, Vset={new_voltage:.1f}V"
                        ),
                        min_interval=max(1.0, sleep_period),
                    )
                else:
                    self._emit_status(f"设置电压失败: {message}", min_interval=max(2.0, sleep_period))

                time.sleep(sleep_period)
            except Exception as exc:
                self._emit_status(f"稳流控制错误: {exc}", min_interval=max(2.0, sleep_period))
                time.sleep(sleep_period)

        self._emit_status("稳流控制结束", force=True)

    def get_feedback_reading(self):
        """Return the current feedback reading in normalized units."""

        try:
            source = self._source_key()
            if source == "keithley":
                value = self.keithley_controller.read_current()
                if value is None:
                    return None
                return {
                    "value": float(value),
                    "unit": STANDARD_CURRENT_UNIT,
                    "kind": "current",
                    "source": source,
                }

            mutex = self.data_mutex
            if mutex is not None:
                mutex.lock()
            try:
                meter_state = dict(self.meter_data.get(source, {}) or {})
            finally:
                if mutex is not None:
                    mutex.unlock()

            if not meter_state:
                return None

            valid = bool(meter_state.get("valid", False))
            ts = float(meter_state.get("timestamp", 0.0) or 0.0)
            timeout_s = float(self.params.get("meter_timeout_s", 3.0))
            if (not valid) or ts <= 0 or (time.time() - ts > timeout_s):
                return None

            kind = infer_measurement_kind(meter_state.get("kind"), meter_state.get("unit"))
            if kind not in ("current", "voltage"):
                return None

            unit = standard_unit_for_kind(kind) or str(meter_state.get("unit", "")).strip()
            value = float(meter_state.get("value", 0.0))
            return {
                "value": value,
                "unit": unit,
                "kind": kind,
                "source": source,
            }
        except Exception as exc:
            logger.info(f"获取反馈值错误: {exc}")
            return None

    def stop(self):
        """Stop stabilization and best-effort shut down the output."""

        self.running = False

        try:
            if self.keithley_controller and self.keithley_controller.is_connected:
                try:
                    self._safe_shutdown_output()
                except Exception:
                    pass
                try:
                    self.keithley_controller.disable_high_voltage()
                except Exception:
                    pass
        except Exception as exc:
            logger.info(f"停止稳流置零/关高压失败: {exc}")

        try:
            self._emit_voltage(0.0)
        except Exception:
            pass



