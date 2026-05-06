from __future__ import annotations


class RemoteCommandService:
    """Centralize web/remote command validation and dispatch."""

    def __init__(self, mw):
        self.mw = mw

    @staticmethod
    def ok(data=None, message="OK"):
        return {"ok": True, "message": message, "data": data}

    @staticmethod
    def err(message="ERROR", data=None):
        return {"ok": False, "message": message, "data": data}

    def _save_config_safely(self):
        try:
            if hasattr(self.mw, "save_config_from_ui"):
                self.mw.save_config_from_ui()
        except Exception:
            pass

    def _parameter_snapshot(self, attr_name: str):
        params = getattr(self.mw, attr_name, None)
        if params is None:
            return {}
        try:
            return params.as_dict()
        except Exception:
            try:
                return dict(params)
            except Exception:
                return {}

    def _validate_power_source_interlock(
        self,
        *,
        test_source_name: str | None = None,
        stabilization_source_name: str | None = None,
    ) -> str:
        test_name = (
            self.mw._get_selected_power_name("test")
            if test_source_name is None
            else str(test_source_name or "").strip()
        )
        stab_name = (
            self.mw._get_selected_power_name("stabilization")
            if stabilization_source_name is None
            else str(stabilization_source_name or "").strip()
        )
        return self.mw.power_catalog_service.validate_selected_power_interlock(
            test_source_name=test_name,
            stabilization_source_name=stab_name,
        )

    def _list_ports(self):
        try:
            return {
                "hv_ports": list(self.mw.get_serial_port_list()),
                "meter_devices": list(self.mw.get_meter_device_option_dicts()),
            }
        except Exception:
            hv_ports = []
            try:
                hv_ports = [self.mw.hv_port_combo.itemText(i) for i in range(self.mw.hv_port_combo.count())]
            except Exception:
                pass
            return {"hv_ports": hv_ports, "meter_devices": []}

    def _set_meter_coefficient(self, params):
        meter_type = str(params.get("meter_type", "")).strip()
        coefficient = float(params.get("coefficient", 1.0))
        if meter_type not in getattr(self.mw, "meter_data", {}):
            return self.err(f"Unknown meter_type: {meter_type}")

        try:
            coeff_widget = getattr(self.mw, f"{meter_type}_coeff", None)
            if coeff_widget is not None and hasattr(coeff_widget, "setText"):
                coeff_widget.setText(str(coefficient))
        except Exception:
            pass

        try:
            self.mw.meter_data[meter_type]["coefficient"] = coefficient
        except Exception:
            pass
        self._save_config_safely()
        return self.ok({"meter_type": meter_type, "coefficient": coefficient})

    def _db_cleanup(self, params):
        try:
            keep_days = int(float(params.get("keep_days", getattr(self.mw.retention_policy, "keep_days", 30))) or 30)
            keep_runs = int(float(params.get("keep_runs", getattr(self.mw.retention_policy, "keep_runs", 200))) or 200)
            archive_before_delete = bool(params.get("archive_before_delete", True))
            archive_dir = str(params.get("archive_dir", getattr(self.mw.retention_policy, "archive_dir", "data/archive")))
            vacuum_mode = str(params.get("vacuum_mode", getattr(self.mw.retention_policy, "vacuum_mode", "incremental")))
        except Exception as e:
            return self.err(f"Invalid cleanup params: {e}")
        return self.mw.cleanup_database(
            keep_days=keep_days,
            keep_runs=keep_runs,
            archive_before_delete=archive_before_delete,
            archive_dir=archive_dir,
            vacuum_mode=vacuum_mode,
        )

    def _set_test_params(self, params):
        snapshot = self._parameter_snapshot("test_params")
        try:
            updated = self.mw.apply_test_params(dict(params or {}))
        except Exception as e:
            return self.err(f"Invalid test params: {e}")

        interlock_error = self._validate_power_source_interlock(
            test_source_name=updated.get("power_source_name")
        )
        if interlock_error:
            try:
                self.mw.apply_test_params(snapshot)
            except Exception:
                pass
            self.mw.update_settings_display()
            self.mw.update_power_action_buttons()
            return self.err(interlock_error)

        self.mw.update_settings_display()
        self.mw.update_power_action_buttons()
        self._save_config_safely()
        return self.ok(dict(self.mw.test_params))

    def _set_stabilization_params(self, params):
        snapshot = self._parameter_snapshot("stabilization_params")
        try:
            updated = self.mw.apply_stabilization_params(dict(params or {}))
        except Exception as e:
            return self.err(f"Invalid stabilization params: {e}")

        interlock_error = self._validate_power_source_interlock(
            stabilization_source_name=updated.get("power_source_name")
        )
        if interlock_error:
            try:
                self.mw.apply_stabilization_params(snapshot)
            except Exception:
                pass
            self.mw.update_settings_display()
            self.mw.update_power_action_buttons()
            return self.err(interlock_error)

        self.mw.update_settings_display()
        self.mw.update_power_action_buttons()
        self._save_config_safely()
        return self.ok(dict(self.mw.stabilization_params))

    def _hv_connect(self, params):
        port = str(params.get("port", "")).strip()
        if not port:
            try:
                port = str(self.mw.hv_port_combo.currentText()).strip()
            except Exception:
                port = ""
        if not port:
            return self.err("HV port is empty")
        try:
            self.mw.hv_port_combo.setCurrentText(port)
        except Exception:
            pass
        if "baudrate" in params:
            try:
                self.mw.hv_baudrate_combo.setCurrentText(str(params.get("baudrate", "")).strip())
            except Exception:
                pass
        self.mw.toggle_hv_connection()
        return self.ok()

    def _hv_disconnect(self):
        if bool(getattr(self.mw.hv_controller, "is_connected", False)):
            before = bool(getattr(self.mw.hv_controller, "is_connected", False))
            self.mw.toggle_hv_connection()
            after = bool(getattr(self.mw.hv_controller, "is_connected", False))
            if before and after:
                return self.err("HV disconnect was rejected")
        return self.ok()

    def _keithley_connect(self, params):
        resource_name = params.get("resource_name", None)
        address = params.get("gpib_address", None)
        try:
            if resource_name is not None:
                self.mw.keithley_addr_combo.setCurrentText(str(resource_name))
            elif address is not None:
                self.mw.keithley_addr_combo.setCurrentText(str(address))
        except Exception:
            pass
        self.mw.toggle_keithley_connection()
        return self.ok()

    def _keithley_disconnect(self):
        connected = False
        try:
            connected = bool(self.mw._get_connected_keithley_names()) or bool(
                getattr(self.mw.keithley_controller, "is_connected", False)
            )
        except Exception:
            connected = bool(getattr(self.mw.keithley_controller, "is_connected", False))
        if connected:
            before = bool(self.mw._get_connected_keithley_names()) or bool(
                getattr(self.mw.keithley_controller, "is_connected", False)
            )
            self.mw.toggle_keithley_connection()
            after = bool(self.mw._get_connected_keithley_names()) or bool(
                getattr(self.mw.keithley_controller, "is_connected", False)
            )
            if before and after:
                return self.err("Keithley disconnect was rejected")
        return self.ok()

    def _meter_toggle(self, params):
        meter_type = str(params.get("meter_type", "")).strip()
        port = str(params.get("port", "")).strip()
        if port:
            try:
                self.mw.set_meter_port(meter_type, port)
            except Exception:
                pass
        self.mw.toggle_meter_connection(meter_type)
        return self.ok()

    def _set_record_path(self, params):
        path = str(params.get("path", "")).strip()
        if not path:
            return self.err("path is empty")
        self.mw.set_record_file_path(path)
        self._save_config_safely()
        return self.ok({"path": path})

    def _toggle_recording(self):
        if not hasattr(self.mw, "toggle_record"):
            return self.err("Recording API not found on MainWindow (expected toggle_record)")
        result = self.mw.toggle_record()
        if result is False:
            return self.err("Recording toggle was rejected")
        return self.ok()

    def _emergency_stop(self):
        if not hasattr(self.mw, "emergency_stop"):
            return self.err("Emergency stop API not found on MainWindow")
        self.mw.emergency_stop()
        return self.ok()

    def _clear_chart(self):
        if hasattr(self.mw, "clear_plots"):
            self.mw.clear_plots()
            return self.ok()
        if hasattr(self.mw, "data_buffer"):
            try:
                self.mw.data_buffer.clear()
            except Exception:
                pass
        return self.ok()

    def dispatch(self, action: str, params=None):
        params = params or {}
        action = str(action or "")
        try:
            if action == "get_state":
                return self.ok(self.mw.state_snapshot_service.collect_state())
            if action == "get_plot":
                return self.ok(self.mw.state_snapshot_service.collect_plot())
            if action == "db_stats":
                return self.ok(self.mw.get_db_stats())
            if action == "db_cleanup":
                return self._db_cleanup(params)
            if action == "list_ports":
                return self.ok(self._list_ports())
            if action == "refresh_ports":
                self.mw.refresh_all_ports()
                return self.ok()
            if action == "set_test_params":
                return self._set_test_params(params)
            if action == "set_stabilization_params":
                return self._set_stabilization_params(params)
            if action == "set_meter_coeff":
                return self._set_meter_coefficient(params)
            if action == "hv_connect":
                return self._hv_connect(params)
            if action == "hv_disconnect":
                return self._hv_disconnect()
            if action == "keithley_connect":
                return self._keithley_connect(params)
            if action == "keithley_disconnect":
                return self._keithley_disconnect()
            if action == "meter_toggle":
                return self._meter_toggle(params)
            if action == "start_test":
                interlock_error = self._validate_power_source_interlock()
                if interlock_error:
                    return self.err(interlock_error)
                if not self.mw._can_execute_test_actions():
                    controller, source_name, err = self.mw._resolve_power_controller("test")
                    if controller is None:
                        return self.err(err or "Unable to start test")
                    runtime_error = self.mw.power_catalog_service.runtime_power_interlock_error("test", source_name)
                    if runtime_error:
                        return self.err(runtime_error)
                before = bool(getattr(self.mw, "is_testing", False))
                self.mw.test_service.start(cycle=False)
                return self.ok() if self.mw.is_testing or before else self.err("Test did not start")
            if action == "start_cycle_test":
                interlock_error = self._validate_power_source_interlock()
                if interlock_error:
                    return self.err(interlock_error)
                if not self.mw._can_execute_test_actions():
                    controller, source_name, err = self.mw._resolve_power_controller("test")
                    if controller is None:
                        return self.err(err or "Unable to start cycle test")
                    runtime_error = self.mw.power_catalog_service.runtime_power_interlock_error("test", source_name)
                    if runtime_error:
                        return self.err(runtime_error)
                before = bool(getattr(self.mw, "is_testing", False))
                self.mw.test_service.start(cycle=True)
                return self.ok() if self.mw.is_testing or before else self.err("Cycle test did not start")
            if action == "stop_test":
                self.mw.test_service.stop()
                return self.ok()
            if action == "reset_voltage":
                self.mw.reset_voltage()
                return self.ok()
            if action == "emergency_stop":
                return self._emergency_stop()
            if action == "start_stabilization":
                interlock_error = self._validate_power_source_interlock()
                if interlock_error:
                    return self.err(interlock_error)
                if not self.mw._can_execute_stabilization_actions():
                    controller, source_name, err = self.mw._resolve_power_controller("stabilization")
                    if controller is None:
                        return self.err(err or "Unable to start stabilization")
                    runtime_error = self.mw.power_catalog_service.runtime_power_interlock_error(
                        "stabilization",
                        source_name,
                    )
                    if runtime_error:
                        return self.err(runtime_error)
                before = bool(getattr(self.mw, "is_stabilizing", False))
                self.mw.stabilization_service.start()
                return self.ok() if self.mw.is_stabilizing or before else self.err("Stabilization did not start")
            if action == "stop_stabilization":
                self.mw.stabilization_service.stop()
                return self.ok()
            if action == "set_record_path":
                return self._set_record_path(params)
            if action == "toggle_recording":
                return self._toggle_recording()
            if action == "clear_chart":
                return self._clear_chart()
            return self.err(f"Unknown action: {action}")
        except Exception as e:
            return self.err(str(e))
