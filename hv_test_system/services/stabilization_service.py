from __future__ import annotations

from PyQt5.QtCore import QObject, pyqtSignal

from ..thread_stabilization import CurrentStabilizationThread


class StabilizationService(QObject):
    """Own the stabilization thread lifecycle."""

    log = pyqtSignal(str)
    started = pyqtSignal()
    stopped = pyqtSignal()

    def __init__(self, mw, parent=None):
        super().__init__(parent)
        self.mw = mw

    def _disconnect_signal(self, signal):
        if signal is None:
            return
        try:
            signal.disconnect()
        except Exception:
            pass

    def _disconnect_thread_signals(self, thread):
        if thread is None:
            return
        self._disconnect_signal(getattr(thread, "update_voltage_signal", None))
        self._disconnect_signal(getattr(thread, "update_status_signal", None))
        self._disconnect_signal(getattr(thread, "stabilization_complete_signal", None))
        self._disconnect_signal(getattr(thread, "finished", None))

    def _normalized_params(self, params_override=None) -> dict:
        base = type(self.mw.stabilization_params)(dict(self.mw.stabilization_params))
        if params_override:
            if hasattr(base, "apply_update"):
                base.apply_update(dict(params_override))
            else:
                base.update(dict(params_override))
        try:
            return base.as_dict()
        except Exception:
            return dict(base)

    def _selected_source_interlock_error(self, stabilization_source_name: str | None = None) -> str:
        return self.mw.power_catalog_service.validate_selected_power_interlock(
            test_source_name=self.mw._get_selected_power_name("test"),
            stabilization_source_name=(
                self.mw._get_selected_power_name("stabilization")
                if stabilization_source_name is None
                else str(stabilization_source_name or "").strip()
            ),
        )

    def _resolve_controller_for_selection(self, selected_name: str):
        return self.mw.power_catalog_service.resolve_power_controller_for_selection(
            "stabilization",
            selected_name,
            allow_auto=True,
        )

    def _validate_start(self, params: dict):
        mw = self.mw
        thread = getattr(mw, "stabilization_thread", None)
        if thread is not None and thread.isRunning():
            self.log.emit("Stabilization is already running")
            return None, None, None

        selection_error = self._selected_source_interlock_error(
            stabilization_source_name=params.get("power_source_name")
        )
        if selection_error:
            self.log.emit(selection_error)
            mw.update_power_action_buttons()
            return None, None, None

        controller, source_key, err = self._resolve_controller_for_selection(
            params.get("power_source_name", "")
        )
        if controller is None:
            self.log.emit(f"Stabilization start failed: {err}")
            mw.update_power_action_buttons()
            return None, None, None

        runtime_error = mw.power_catalog_service.runtime_power_interlock_error("stabilization", source_key)
        if runtime_error:
            self.log.emit(runtime_error)
            mw.update_power_action_buttons()
            return None, None, None

        if (
            str(params.get("current_source", "keithley")) == "keithley"
            and not bool(getattr(controller, "supports_internal_current_readback", False))
        ):
            self.log.emit("Selected power source does not support internal current readback")
            mw.update_power_action_buttons()
            return None, None, None

        return params, controller, source_key

    def _start_thread(self, params: dict, controller, source_key: str):
        mw = self.mw
        mw.active_stabilization_controller = controller
        mw.active_stabilization_power_source = source_key
        mw._stabilization_stop_requested = False
        mw._stabilization_last_completed = False

        thread = CurrentStabilizationThread(controller, mw.meter_data, mw.data_mutex, params)
        thread.update_voltage_signal.connect(self.on_voltage_updated)
        thread.update_status_signal.connect(self.log.emit)
        thread.stabilization_complete_signal.connect(self.on_complete)
        thread.finished.connect(self.handle_thread_finished)

        mw.stabilization_thread = thread
        mw.is_stabilizing = True
        mw.stabilization_running = True
        mw.update_power_action_buttons()
        self.log.emit(f"Starting stabilization on {mw._power_source_name(source_key)}")
        thread.start()
        return True

    def start(self):
        try:
            if self.start_impl():
                self.started.emit()
        except Exception as exc:
            self.log.emit(f"Stabilization start failed: {exc}")

    def start_impl(self, params_override=None):
        params, controller, source_key = self._validate_start(
            self._normalized_params(params_override)
        )
        if controller is None:
            return False
        return self._start_thread(params, controller, source_key)

    def clear_state(self):
        mw = self.mw
        mw.is_stabilizing = False
        mw.stabilization_running = False
        mw.active_stabilization_controller = None
        mw.active_stabilization_power_source = None
        mw.stabilization_thread = None
        try:
            mw.update_power_action_buttons()
        except Exception:
            pass

    def handle_thread_finished(self):
        mw = self.mw
        if not getattr(mw, "is_stabilizing", False) and getattr(mw, "stabilization_thread", None) is None:
            return

        requested = bool(getattr(mw, "_stabilization_stop_requested", False))
        completed = bool(getattr(mw, "_stabilization_last_completed", False))

        if not requested and not completed:
            mw.stabilization_failure_count = int(getattr(mw, "stabilization_failure_count", 0) or 0) + 1

        self.clear_state()
        mw._stabilization_stop_requested = False
        if not requested:
            self.log.emit("Stabilization finished")

    def stop(self):
        try:
            self.stop_impl()
            self.stopped.emit()
        except Exception as exc:
            self.log.emit(f"Stop stabilization failed: {exc}")

    def stop_impl(self):
        mw = self.mw
        thread = getattr(mw, "stabilization_thread", None)
        if thread is None and not getattr(mw, "is_stabilizing", False):
            self.clear_state()
            return

        mw._stabilization_stop_requested = True
        try:
            if thread is not None and hasattr(thread, "suppress_qt_signals"):
                thread.suppress_qt_signals()
        except Exception:
            pass

        try:
            self._disconnect_thread_signals(thread)
        except Exception:
            pass

        try:
            if thread is not None and hasattr(thread, "stop"):
                thread.stop()
        except Exception as exc:
            self.log.emit(f"Stop stabilization thread failed: {exc}")

        try:
            if thread is not None and thread.isRunning():
                thread.wait(2000)
        except Exception:
            pass

        controller = getattr(mw, "active_stabilization_controller", None)
        if controller is not None:
            try:
                controller.stop_output()
            except Exception:
                pass

        self.clear_state()
        self.log.emit("Stabilization stopped")

    def on_voltage_updated(self, voltage):
        mw = self.mw
        try:
            mw.update_keithley_voltage_display(
                voltage,
                getattr(mw, "active_stabilization_power_source", None),
            )
        except Exception:
            try:
                power_name = str(getattr(mw, "active_stabilization_power_source", "") or "").strip()
                if power_name:
                    mw._set_power_voltage_cache(power_name, float(voltage))
                mw.refresh_power_voltage_slots()
            except Exception:
                pass

    def on_complete(self):
        if not bool(getattr(self.mw, "_stabilization_last_completed", False)):
            self.mw._stabilization_last_completed = True
            self.mw.stabilization_completion_count = (
                int(getattr(self.mw, "stabilization_completion_count", 0) or 0) + 1
            )
        self.log.emit("Feedback has entered the stability window")
