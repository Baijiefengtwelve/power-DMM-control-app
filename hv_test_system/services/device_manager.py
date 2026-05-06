from __future__ import annotations


class DeviceManager:
    """Own device connection, port refresh, and poller lifecycle concerns."""

    def __init__(self, mw):
        self.mw = mw
        try:
            setattr(mw, "device_manager", self)
        except Exception:
            pass

    def _get_power_connection_service(self):
        service = getattr(self.mw, "power_connection_service", None)
        if service is not None:
            return service
        from .power_connection_service import PowerConnectionService

        service = PowerConnectionService(self.mw)
        try:
            setattr(self.mw, "power_connection_service", service)
        except Exception:
            pass
        return service

    def _get_power_inventory_service(self):
        service = getattr(self.mw, "power_inventory_service", None)
        if service is not None:
            return service
        from .power_inventory_service import PowerInventoryService

        service = PowerInventoryService(self.mw)
        try:
            setattr(self.mw, "power_inventory_service", service)
        except Exception:
            pass
        return service

    def _get_port_refresh_service(self):
        service = getattr(self.mw, "port_refresh_service", None)
        if service is not None:
            return service
        from .port_refresh_service import PortRefreshService

        service = PortRefreshService(self.mw)
        try:
            setattr(self.mw, "port_refresh_service", service)
        except Exception:
            pass
        return service

    def _get_hv_worker_service(self):
        service = getattr(self.mw, "hv_worker_service", None)
        if service is not None:
            return service
        from .hv_worker_service import HVWorkerService

        service = HVWorkerService(self.mw)
        try:
            setattr(self.mw, "hv_worker_service", service)
        except Exception:
            pass
        return service

    def _get_device_shutdown_service(self):
        service = getattr(self.mw, "device_shutdown_service", None)
        if service is not None:
            return service
        from .device_shutdown_service import DeviceShutdownService

        service = DeviceShutdownService(self.mw)
        try:
            setattr(self.mw, "device_shutdown_service", service)
        except Exception:
            pass
        return service

    def _get_power_polling_service(self):
        service = getattr(self.mw, "power_polling_service", None)
        if service is not None:
            return service
        from .power_polling_service import PowerPollingService

        service = PowerPollingService(self.mw)
        try:
            setattr(self.mw, "power_polling_service", service)
        except Exception:
            pass
        return service

    def is_hv_connecting(self) -> bool:
        thread = getattr(self.mw, "_hv_connect_thread", None)
        if thread is None:
            return False
        try:
            return bool(thread.isRunning())
        except Exception:
            return False

    def is_hv_connecting_for(self, name: str) -> bool:
        clean_name = str(name or "").strip()
        if not clean_name:
            return False
        pending = str(getattr(self.mw, "pending_haps06_power_name", "") or "").strip()
        return self.is_hv_connecting() and pending == clean_name

    def rename_power_device(self, index: int, new_name: str):
        return self._get_power_inventory_service().rename_power_device(index, new_name)

    def update_power_device_field(self, index: int, field: str, value: str):
        return self._get_power_inventory_service().update_power_device_field(index, field, value)

    def is_power_device_connected(self, name: str) -> bool:
        mw = self.mw
        device = mw._find_power_device(name)
        if not device:
            return False

        power_type = mw.normalize_power_type(device.get("type"))
        clean_name = str(name or "").strip()
        if power_type == "HAPS06" and self.is_hv_connecting_for(clean_name):
            return True

        controller = getattr(mw, "connected_named_power_controllers", {}).get(clean_name)
        if controller is None:
            return False
        return bool(getattr(controller, "is_connected", False))

    def connect_named_power_device(self, name: str):
        return self._get_power_connection_service().connect_named_power_device(name)

    def disconnect_named_power_device(self, name: str):
        return self._get_power_connection_service().disconnect_named_power_device(name)

    def get_power_device_status_text(self, name: str) -> str:
        return self.mw.power_catalog_service.get_power_device_status_text(name)

    def update_power_summary_label(self):
        return self.mw.power_catalog_service.update_power_summary_label()

    def refresh_all_ports(self):
        return self._get_port_refresh_service().refresh_all_ports()

    def refresh_ports(self):
        return self._get_port_refresh_service().refresh_ports()

    def refresh_gpib_ports(self):
        return self._get_port_refresh_service().refresh_gpib_ports()

    def start_hv_connection_async(self, port: str, baudrate: int):
        return self._get_power_connection_service().start_hv_connection_async(port, baudrate)

    def on_hv_connect_finished(self, success: bool, message: str, port: str):
        return self._get_power_connection_service().on_hv_connect_finished(success, message, port)

    def toggle_hv_connection(self):
        return self._get_power_connection_service().toggle_hv_connection()

    def toggle_keithley_connection(self):
        return self._get_power_connection_service().toggle_keithley_connection()

    def _create_meter_thread(self, meter_type: str, port: str):
        return self.mw.meter_connection_service.create_meter_thread(meter_type, port)

    def _disconnect_meter_thread(self, meter_type: str, *, reset_ui: bool):
        return self.mw.meter_connection_service.disconnect_meter_thread(meter_type, reset_ui=reset_ui)

    def toggle_meter_connection(self, meter_type: str):
        return self.mw.meter_connection_service.toggle_meter_connection(meter_type)

    def attach_hv_worker_signals(self):
        return self._get_hv_worker_service().attach_hv_worker_signals()

    def detach_hv_worker_signals(self):
        return self._get_hv_worker_service().detach_hv_worker_signals()

    def on_hv_worker_error(self, msg: str):
        return self._get_hv_worker_service().on_hv_worker_error(msg)

    def on_hv_worker_connected(self, port: str):
        return self._get_hv_worker_service().on_hv_worker_connected(port)

    def on_hv_worker_disconnected(self):
        return self._get_hv_worker_service().on_hv_worker_disconnected()

    def start_hv_voltage_poller(self, interval_ms: int = 500):
        return self._get_power_polling_service().start_hv_voltage_poller(interval_ms=interval_ms)

    def stop_hv_voltage_poller(self):
        return self._get_power_polling_service().stop_hv_voltage_poller()

    def on_hv_voltage_polled(self, voltage: float):
        return self._get_power_polling_service().on_hv_voltage_polled(voltage)

    def on_hv_poller_error(self, msg: str):
        return self._get_power_polling_service().on_hv_poller_error(msg)

    def get_connected_keithley_controller_map(self):
        return self._get_power_polling_service().get_connected_keithley_controller_map()

    def start_keithley_voltage_poller(self, interval_ms: int = 2500):
        return self._get_power_polling_service().start_keithley_voltage_poller(interval_ms=interval_ms)

    def stop_keithley_voltage_poller(self):
        return self._get_power_polling_service().stop_keithley_voltage_poller()

    def on_keithley_poller_voltage(self, voltage: float):
        return self._get_power_polling_service().on_keithley_poller_voltage(voltage)

    def on_named_keithley_poller_voltage(self, name: str, voltage: float):
        return self._get_power_polling_service().on_named_keithley_poller_voltage(name, voltage)

    def on_keithley_poller_error(self, msg: str):
        return self._get_power_polling_service().on_keithley_poller_error(msg)

    def shutdown(self):
        return self._get_device_shutdown_service().shutdown()
