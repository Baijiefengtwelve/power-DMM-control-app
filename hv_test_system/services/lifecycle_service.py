from __future__ import annotations

import gc

from PyQt5.QtWidgets import QApplication


class LifecycleService:
    """Own application lifecycle hooks such as quit requests and close cleanup."""

    CLOSE_LOG_MESSAGE = "\u7cfb\u7edf\u5df2\u5b89\u5168\u5173\u95ed"
    TRAY_MESSAGE_TITLE = "\u7a0b\u5e8f\u4ecd\u5728\u8fd0\u884c"
    TRAY_MESSAGE_BODY = (
        "\u5df2\u6700\u5c0f\u5316\u5230\u7cfb\u7edf\u6258\u76d8\uff0c"
        "\u8bf7\u901a\u8fc7\u6258\u76d8\u83dc\u5355\u9000\u51fa\u7a0b\u5e8f\u3002"
    )

    def __init__(self, mw):
        self.mw = mw

    def request_quit(self):
        try:
            self.mw._allow_quit = True
        except Exception:
            pass

        try:
            self.mw.close()
        except Exception:
            pass

        try:
            app = QApplication.instance()
            if app is not None:
                app.quit()
        except Exception:
            pass

    def on_data_saved(self):
        try:
            self.mw.show_status_message("Data saved", 1500)
        except Exception:
            pass

    def on_data_converted(self):
        self.mw.is_converting = False
        try:
            self.mw.record_btn.setEnabled(True)
        except Exception:
            pass
        try:
            self.mw.show_status_message("Record post-processing completed", 3000)
        except Exception:
            pass
        try:
            self.mw.log_message("Record post-processing completed")
        except Exception:
            pass

    def close_event(self, event):
        if self._hide_to_tray_if_needed(event):
            return

        try:
            try:
                self.mw._shutdown_in_progress = True
            except Exception:
                pass
            self.mw.is_testing = False
            self.mw.is_cycle_testing = False

            if getattr(self.mw, "is_stabilizing", False):
                self.mw.stop_current_stabilization()

            self._stop_runtime_timers()

            if getattr(self.mw, "is_recording", False):
                self.mw.recording_service.finalize_on_shutdown()

            self._prepare_background_workers_for_shutdown()

            self._stop_worker(getattr(self.mw, "data_saver", None), "DataSaver")
            self._stop_worker(getattr(self.mw, "influx_writer", None), "InfluxWriter")
            self._safe_call(getattr(self.mw, "sqlite_recorder", None), "stop_run")
            self._stop_worker(getattr(self.mw, "sqlite_recorder", None), "SQLiteRecorder")
            self._safe_call(self.mw, "save_config_from_ui")
            self._safe_call(getattr(self.mw, "device_manager", None), "shutdown")
            self._safe_call(getattr(self.mw, "tray_icon", None), "hide")

            self._clear_runtime_buffers()

            self.mw.log_message(self.CLOSE_LOG_MESSAGE)
            event.accept()
        except Exception as exc:
            print(f"Close application error: {exc}")
            event.accept()

    def _hide_to_tray_if_needed(self, event) -> bool:
        try:
            if getattr(self.mw, "_allow_quit", False):
                return False
            tray_icon = getattr(self.mw, "tray_icon", None)
            if tray_icon is None:
                return False
            try:
                is_visible = getattr(tray_icon, "isVisible", None)
                if callable(is_visible) and not is_visible():
                    return False
            except Exception:
                pass

            event.ignore()
            self.mw.hide()
            try:
                tray_icon.showMessage(
                    self.TRAY_MESSAGE_TITLE,
                    self.TRAY_MESSAGE_BODY,
                    getattr(tray_icon, "Information", 1),
                    3000,
                )
            except Exception:
                pass
            return True
        except Exception:
            return False

    def _stop_runtime_timers(self):
        timer_service = getattr(self.mw, "timer_service", None)
        if timer_service is not None:
            try:
                timer_service.stop_all()
                return
            except Exception:
                pass
        self._safe_call(getattr(self.mw, "hv_voltage_update_timer", None), "stop")
        self._safe_call(self.mw, "stop_hv_voltage_poller")
        self._safe_call(getattr(self.mw, "keithley_voltage_update_timer", None), "stop")
        self._safe_call(self.mw, "stop_keithley_voltage_poller")
        self._safe_call(getattr(self.mw, "data_update_timer", None), "stop")
        self._safe_call(getattr(self.mw, "status_update_timer", None), "stop")
        self._safe_call(getattr(self.mw, "meter_display_timer", None), "stop")
        self._safe_call(getattr(self.mw, "cache_flush_timer", None), "stop")
        self._safe_call(getattr(self.mw, "countdown_manager", None), "stop")
        self._safe_call(getattr(self.mw, "save_timer", None), "stop")

    def _clear_runtime_buffers(self):
        self._safe_clear(getattr(self.mw, "recorded_data", None))
        self._safe_clear(getattr(self.mw, "all_anode_data", None))
        self._safe_clear(getattr(self.mw, "data_cache", None))
        gc.collect()

    def _safe_call(self, target, method_name: str):
        if target is None:
            return
        try:
            method = getattr(target, method_name)
        except Exception:
            return
        try:
            method()
        except Exception:
            pass

    def _disconnect_signal(self, signal):
        if signal is None:
            return
        try:
            signal.disconnect()
        except Exception:
            pass

    def _prepare_background_workers_for_shutdown(self):
        data_saver = getattr(self.mw, "data_saver", None)
        if data_saver is not None:
            self._safe_call(data_saver, "suppress_qt_signals")
            self._disconnect_signal(getattr(data_saver, "save_complete", None))
            self._disconnect_signal(getattr(data_saver, "convert_complete", None))

    def _stop_worker(self, target, label: str) -> bool:
        if target is None:
            return True
        try:
            stop_method = getattr(target, "stop")
        except Exception:
            return True
        try:
            result = stop_method()
        except TypeError:
            try:
                result = stop_method(timeout_s=5.0)
            except Exception as exc:
                self.mw.log_message(f"{label} stop failed: {exc}")
                return False
        except Exception as exc:
            self.mw.log_message(f"{label} stop failed: {exc}")
            return False
        if result is False:
            self.mw.log_message(f"{label} stop timed out")
            return False
        return True

    def _safe_clear(self, value):
        if value is None:
            return
        try:
            value.clear()
        except Exception:
            pass
