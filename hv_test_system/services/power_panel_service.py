from __future__ import annotations


class PowerPanelService:
    """Own power-panel UI actions and lightweight panel-side runtime choices."""

    def __init__(self, mw):
        self.mw = mw

    def update_power_action_buttons(self):
        can_test = self._safe_bool(self.mw._can_execute_test_actions)
        can_stab = self._safe_bool(self.mw._can_execute_stabilization_actions)
        can_emergency_stop = self._can_emergency_stop()

        self._safe_set_enabled(getattr(self.mw, "start_test_btn", None), (not self.mw.is_testing) and can_test)
        self._safe_set_enabled(getattr(self.mw, "cycle_test_btn", None), (not self.mw.is_testing) and can_test)
        self._safe_set_enabled(getattr(self.mw, "manual_set_btn", None), (not self.mw.is_testing) and can_test)
        self._safe_set_enabled(getattr(self.mw, "reset_btn", None), can_emergency_stop)
        self._safe_set_enabled(getattr(self.mw, "stop_test_btn", None), bool(self.mw.is_testing))

        # Keep the settings button available even when no power supply is currently connected.
        self._safe_set_enabled(getattr(self.mw, "current_stabilization_btn", None), not self.mw.is_stabilizing)
        self._safe_set_enabled(
            getattr(self.mw, "start_stabilization_btn", None),
            (not self.mw.is_stabilizing) and can_stab,
        )
        self._safe_set_enabled(getattr(self.mw, "stop_stabilization_btn", None), bool(self.mw.is_stabilizing))

    def set_meter_connection(self, meter_type: str, should_connect: bool):
        try:
            btn = getattr(self.mw, f"{meter_type}_connect_btn")
            is_connected = meter_type in self.mw.meter_threads
            if should_connect and not is_connected:
                btn.setText("连接")
                self.mw.toggle_meter_connection(meter_type)
            elif (not should_connect) and is_connected:
                btn.setText("断开")
                self.mw.toggle_meter_connection(meter_type)
            self.update_power_action_buttons()
        except Exception as exc:
            self.mw.log_message(f"{meter_type}连接切换失败: {exc}")

    def add_power_device(self):
        name = self.mw.power_catalog_service.ensure_unique_power_name(f"电源{len(self.mw.power_devices) + 1}")
        device = {"name": name, "type": "HAPS06", "address": "", "baudrate": "9600"}
        self.mw.power_devices.append(device)
        return device

    def remove_power_device(self, index: int):
        if index < 0 or index >= len(self.mw.power_devices):
            return False

        device = self.mw.power_devices[index]
        try:
            self.mw.disconnect_named_power_device(device.get("name", ""))
        except Exception:
            pass

        del self.mw.power_devices[index]
        self.update_power_action_buttons()
        self.mw.update_power_summary_label()
        return True

    def get_display_keithley_controller(self):
        try:
            controllers = self.mw._get_connected_keithley_controller_map()
            for name in self.mw._power_display_names():
                controller = controllers.get(name)
                if controller is not None and bool(getattr(controller, "is_connected", False)):
                    return controller

            for name in (
                str(self.mw.connected_power_name_by_type.get("Keithley 248") or "").strip(),
                str(getattr(self.mw, "active_stabilization_power_source", "") or "").strip(),
                str(getattr(self.mw, "active_test_power_source", "") or "").strip(),
            ):
                controller = controllers.get(name)
                if controller is not None and bool(getattr(controller, "is_connected", False)):
                    return controller

            for controller in controllers.values():
                if controller is not None and bool(getattr(controller, "is_connected", False)):
                    return controller
        except Exception:
            pass
        return None

    def _safe_bool(self, callback):
        try:
            return bool(callback())
        except Exception:
            return False

    def _safe_set_enabled(self, widget, enabled: bool):
        if widget is None:
            return
        try:
            widget.setEnabled(bool(enabled))
        except Exception:
            pass

    def _can_emergency_stop(self) -> bool:
        if bool(getattr(self.mw, "is_testing", False)):
            return True
        if bool(getattr(self.mw, "is_stabilizing", False)):
            return True
        if bool(getattr(self.mw, "is_recording", False)):
            return True
        try:
            return bool(self.mw._connected_power_sources())
        except Exception:
            return False
