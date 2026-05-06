from __future__ import annotations


class PowerInventoryService:
    """Own named power inventory edits and related runtime alias updates."""

    def __init__(self, mw):
        self.mw = mw

    def rename_power_device(self, index: int, new_name: str):
        mw = self.mw
        if index < 0 or index >= len(mw.power_devices):
            return

        old_name = str(mw.power_devices[index].get("name", "")).strip()
        fixed = mw._ensure_unique_power_name(new_name, exclude_index=index)
        mw.power_devices[index]["name"] = fixed

        if old_name and old_name != fixed:
            try:
                if old_name in mw.connected_named_power_controllers:
                    mw.connected_named_power_controllers[fixed] = mw.connected_named_power_controllers.pop(old_name)
            except Exception:
                pass
            try:
                if old_name in mw.named_keithley_controllers:
                    mw.named_keithley_controllers[fixed] = mw.named_keithley_controllers.pop(old_name)
            except Exception:
                pass

        for power_type in ("HAPS06", "Keithley 248"):
            if mw.connected_power_name_by_type.get(power_type) == old_name:
                mw.connected_power_name_by_type[power_type] = fixed

        if mw.test_params.get("power_source_name") == old_name:
            mw.test_params["power_source_name"] = fixed
        if mw.stabilization_params.get("power_source_name") == old_name:
            mw.stabilization_params["power_source_name"] = fixed

        try:
            mw._refresh_keithley_controller_alias(preferred_name=fixed)
        except Exception:
            pass
        mw.update_settings_display()
        mw.update_power_summary_label()

    def update_power_device_field(self, index: int, field: str, value: str):
        mw = self.mw
        if index < 0 or index >= len(mw.power_devices):
            return

        device = mw.power_devices[index]
        if field == "type":
            new_type = mw.normalize_power_type(value)
            old_type = mw.normalize_power_type(device.get("type"))
            was_connected = mw.is_power_device_connected(device.get("name", ""))
            if was_connected:
                mw.disconnect_named_power_device(device.get("name", ""))
            device["type"] = new_type
            if new_type == "HAPS06" and not str(device.get("baudrate", "")).strip():
                device["baudrate"] = "9600"
            if old_type == "Keithley 248" and new_type != "Keithley 248":
                try:
                    mw.named_keithley_controllers.pop(str(device.get("name", "")).strip(), None)
                except Exception:
                    pass
        elif field in ("address", "baudrate"):
            was_connected = mw.is_power_device_connected(device.get("name", ""))
            if was_connected:
                mw.disconnect_named_power_device(device.get("name", ""))
            device[field] = str(value or "").strip()
        else:
            device[field] = value

        mw.update_power_action_buttons()
        mw.update_power_summary_label()
