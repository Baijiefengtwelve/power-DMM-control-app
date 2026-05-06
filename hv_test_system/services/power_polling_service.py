from __future__ import annotations

import time

from ..thread_power import HVVoltagePoller, KeithleyVoltagePoller


class PowerPollingService:
    """Own HAPS06 and Keithley voltage pollers plus their cache updates."""

    def __init__(self, mw, *, hv_poller_factory=None, keithley_poller_factory=None):
        self.mw = mw
        self.hv_poller_factory = hv_poller_factory or HVVoltagePoller
        self.keithley_poller_factory = keithley_poller_factory or KeithleyVoltagePoller

    def _disconnect_signal(self, signal):
        if signal is None:
            return
        try:
            signal.disconnect()
        except Exception:
            pass

    def start_hv_voltage_poller(self, interval_ms: int = 500):
        mw = self.mw
        try:
            self.stop_hv_voltage_poller()
            if not bool(getattr(mw.hv_controller, "is_connected", False)):
                return

            mw.hv_voltage_poller = self.hv_poller_factory(mw.hv_controller, interval_ms=interval_ms, parent=mw)
            mw.hv_voltage_poller.voltage_updated.connect(self.on_hv_voltage_polled)
            mw.hv_voltage_poller.poll_error.connect(self.on_hv_poller_error)
            mw.hv_voltage_poller.start()
        except Exception as exc:
            mw.log_message(f"启动高压源电压轮询失败: {exc}")

    def stop_hv_voltage_poller(self):
        poller = getattr(self.mw, "hv_voltage_poller", None)
        if poller is not None:
            try:
                poller.suppress_qt_signals()
            except Exception:
                pass
            self._disconnect_signal(getattr(poller, "voltage_updated", None))
            self._disconnect_signal(getattr(poller, "poll_error", None))
            try:
                poller.stop()
            except Exception:
                pass
        self.mw.hv_voltage_poller = None

    def on_hv_voltage_polled(self, voltage: float):
        mw = self.mw
        try:
            value = float(voltage)
        except Exception:
            return

        mw._hv_v_cache = value
        mw._hv_v_ts = time.time()
        try:
            mw.hv_controller.actual_voltage = value
        except Exception:
            pass
        try:
            name = str(mw.connected_power_name_by_type.get("HAPS06") or mw.pending_haps06_power_name or "").strip()
            if name:
                mw._set_power_voltage_cache(name, value)
        except Exception:
            pass
        mw.refresh_power_voltage_slots()

    def on_hv_poller_error(self, msg: str):
        try:
            self.mw.log_message(str(msg))
        except Exception:
            pass

    def get_connected_keithley_controller_map(self):
        mw = self.mw
        mapping = {}
        skip_names = set()
        try:
            if bool(getattr(mw, "is_testing", False)):
                active_test = str(getattr(mw, "active_test_power_source", "") or "").strip()
                if active_test:
                    skip_names.add(active_test)
            if bool(getattr(mw, "is_stabilizing", False)):
                active_stab = str(getattr(mw, "active_stabilization_power_source", "") or "").strip()
                if active_stab:
                    skip_names.add(active_stab)
        except Exception:
            pass

        try:
            for dev in mw.power_devices:
                name = str(dev.get("name", "")).strip()
                if not name:
                    continue
                if mw.normalize_power_type(dev.get("type")) != "Keithley 248":
                    continue
                if name in skip_names:
                    continue
                controller = getattr(mw, "connected_named_power_controllers", {}).get(name)
                if controller is not None and bool(getattr(controller, "is_connected", False)):
                    mapping[name] = controller
        except Exception:
            pass
        return mapping

    def start_keithley_voltage_poller(self, interval_ms: int = 2500):
        mw = self.mw
        try:
            self.stop_keithley_voltage_poller()
            mw.keithley_voltage_poller = self.keithley_poller_factory(
                self.get_connected_keithley_controller_map,
                interval_ms=interval_ms,
            )
            mw.keithley_voltage_poller.voltage_updated.connect(self.on_keithley_poller_voltage)
            try:
                mw.keithley_voltage_poller.named_voltage_updated.connect(self.on_named_keithley_poller_voltage)
            except Exception:
                pass
            mw.keithley_voltage_poller.poll_error.connect(self.on_keithley_poller_error)
            mw.keithley_voltage_poller.start()
        except Exception:
            mw.keithley_voltage_poller = None

    def stop_keithley_voltage_poller(self):
        poller = getattr(self.mw, "keithley_voltage_poller", None)
        if poller is not None:
            try:
                poller.suppress_qt_signals()
            except Exception:
                pass
            self._disconnect_signal(getattr(poller, "voltage_updated", None))
            self._disconnect_signal(getattr(poller, "named_voltage_updated", None))
            self._disconnect_signal(getattr(poller, "poll_error", None))
            try:
                poller.stop()
            except Exception:
                pass
        self.mw.keithley_voltage_poller = None

    def on_keithley_poller_voltage(self, voltage: float):
        mw = self.mw
        try:
            mw._keithley_v_cache = float(voltage)
            mw._keithley_v_ts = time.time()
        except Exception:
            pass
        mw.refresh_power_voltage_slots()

    def on_named_keithley_poller_voltage(self, name: str, voltage: float):
        mw = self.mw
        try:
            mw._set_power_voltage_cache(name, float(voltage))
        except Exception:
            pass
        preferred = str(mw.connected_power_name_by_type.get("Keithley 248") or "").strip()
        if preferred and str(name).strip() == preferred:
            try:
                mw._keithley_v_cache = float(voltage)
                mw._keithley_v_ts = time.time()
            except Exception:
                pass
        mw.refresh_power_voltage_slots()

    def on_keithley_poller_error(self, msg: str):
        try:
            self.mw.log_message(str(msg))
        except Exception:
            pass
