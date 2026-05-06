from __future__ import annotations


class PortRefreshService:
    """Own serial/GPIB port refresh flows."""

    METER_TYPES = ("cathode", "gate", "anode", "backup", "vacuum")

    def __init__(self, mw):
        self.mw = mw

    def _has_active_meter_connections(self) -> bool:
        try:
            return bool(getattr(self.mw, "meter_threads", {}))
        except Exception:
            return False

    def refresh_all_ports(self):
        try:
            self.refresh_ports()
            self.refresh_gpib_ports()
            self.mw.log_message("Port list refreshed (serial + GPIB)")
        except Exception as exc:
            self.mw.log_message(f"Failed to refresh port list: {exc}")

    def refresh_ports(self):
        mw = self.mw
        try:
            if self._has_active_meter_connections():
                mw.log_message("Active meter connections detected, skip serial refresh")
                return False

            serial_ports = list(mw.get_serial_port_list())
            meter_options = list(mw.get_meter_device_options())
            meter_device_ids = [option.device_id for option in meter_options]

            if mw.hv_port_combo.isEnabled():
                current_hv_port = mw.hv_port_combo.currentText()
                mw.hv_port_combo.clear()
                mw.hv_port_combo.addItems(serial_ports)
                if current_hv_port:
                    mw.hv_port_combo.setCurrentText(current_hv_port)

            for meter_type in self.METER_TYPES:
                combo = getattr(mw, f"{meter_type}_port_combo")
                if not combo.isEnabled():
                    continue
                current_port = combo.currentText()
                combo.clear()
                if meter_type == "vacuum":
                    combo.addItems(serial_ports)
                else:
                    combo.addItems(meter_device_ids)
                if current_port:
                    combo.setCurrentText(current_port)

            hid_count = sum(1 for option in meter_options if option.device_type == "hid")
            mw.log_message(
                f"Device list refreshed: {len(serial_ports)} serial ports, {hid_count} HID multimeters"
            )
            return True
        except Exception as exc:
            mw.log_message(f"Failed to refresh serial or HID devices: {exc}")
            return False

    def refresh_gpib_ports(self):
        mw = self.mw
        try:
            if not mw.keithley_addr_combo.isEnabled():
                mw.log_message("Keithley already connected, skip GPIB refresh")
                return

            try:
                import pyvisa

                rm = pyvisa.ResourceManager()
                resources = list(rm.list_resources())
                gpib_devices = [resource for resource in resources if "GPIB" in resource.upper()]

                mw.keithley_addr_combo.clear()
                if gpib_devices:
                    for resource in gpib_devices:
                        mw.keithley_addr_combo.addItem(resource)
                    mw.log_message(f"Found {len(gpib_devices)} GPIB resources")
                else:
                    mw.log_message("No GPIB resources found, keep the box editable for manual input")
            except ImportError:
                mw.keithley_addr_combo.clear()
                mw.log_message("pyvisa is not installed, keep the GPIB box editable for manual input")
        except Exception as exc:
            mw.keithley_addr_combo.clear()
            mw.log_message(f"Failed to refresh GPIB resources: {exc}")
