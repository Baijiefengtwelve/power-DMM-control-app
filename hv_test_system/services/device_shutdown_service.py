from __future__ import annotations


class DeviceShutdownService:
    """Own final device and thread cleanup during app shutdown."""

    def __init__(self, mw):
        self.mw = mw

    def shutdown(self):
        mw = self.mw
        mw.stop_hv_voltage_poller()
        mw.stop_keithley_voltage_poller()
        mw._detach_hv_worker_signals()

        try:
            mw.hv_controller.disconnect()
        except Exception:
            pass

        try:
            for _, controller in list(getattr(mw, "connected_named_power_controllers", {}).items()):
                if controller is mw.hv_controller:
                    continue
                try:
                    if controller is not None and bool(getattr(controller, "is_connected", False)):
                        controller.disconnect()
                except Exception:
                    pass
        except Exception:
            pass

        try:
            mw.connected_named_power_controllers.clear()
        except Exception:
            pass

        mw.connected_power_name_by_type["HAPS06"] = None
        mw.connected_power_name_by_type["Keithley 248"] = None
        mw.pending_haps06_power_name = None

        try:
            mw.keithley_controller.disconnect()
        except Exception:
            pass

        for meter_type in list(getattr(mw, "meter_threads", {}).keys()):
            mw.meter_connection_service.disconnect_meter_thread(meter_type, reset_ui=False)
