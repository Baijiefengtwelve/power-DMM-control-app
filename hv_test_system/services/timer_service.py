from __future__ import annotations

from PyQt5.QtCore import QTimer


class TimerService:
    """Own recurring UI/runtime timers and their coordinated shutdown."""

    TIMER_SPECS = (
        ("hv_voltage_update_timer", "update_hv_voltage", 1000),
        ("keithley_voltage_update_timer", "update_keithley_voltage", 1000),
        ("data_update_timer", "update_plots", 1000),
        ("status_update_timer", "update_status_display", 1000),
        ("meter_display_timer", "update_meter_displays", 500),
        ("cache_flush_timer", "flush_data_cache", 5000),
    )

    def __init__(self, mw, timer_factory=None):
        self.mw = mw
        self.timer_factory = timer_factory or QTimer

    def setup_timers(self):
        for attr_name, callback_name, interval_ms in self.TIMER_SPECS:
            timer = self.timer_factory()
            timer.timeout.connect(getattr(self.mw, callback_name))
            timer.start(interval_ms)
            setattr(self.mw, attr_name, timer)

        self.mw.start_keithley_voltage_poller(interval_ms=2500)

    def stop_all(self):
        for attr_name, _, _ in self.TIMER_SPECS:
            timer = getattr(self.mw, attr_name, None)
            if timer is None:
                continue
            try:
                timer.stop()
            except Exception:
                pass

        try:
            self.mw.stop_hv_voltage_poller()
        except Exception:
            pass
        try:
            self.mw.stop_keithley_voltage_poller()
        except Exception:
            pass
        try:
            self.mw.countdown_manager.stop()
        except Exception:
            pass
        try:
            self.mw.save_timer.stop()
        except Exception:
            pass
