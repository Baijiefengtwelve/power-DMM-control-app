from __future__ import annotations

from ..parameter_models import AUTO_POWER_SOURCE_NAME


class ConfigSyncService:
    """Synchronize ConfigParser data with MainWindow widgets and models."""

    METER_TYPES = ("cathode", "gate", "anode", "backup", "vacuum")
    STABILIZATION_FIELDS = (
        "current_source",
        "target_current",
        "stability_range",
        "start_voltage",
        "adjust_frequency",
        "max_adjust_voltage",
        "algorithm",
        "pid_kp",
        "pid_ki",
        "pid_kd",
    )
    TEST_FIELDS = (
        "start_voltage",
        "target_voltage",
        "voltage_step",
        "step_delay",
        "cycle_time",
    )
    MONITORING_DEFAULTS = {
        "influxdb_mode": "v2",
        "influxdb_url": "http://127.0.0.1:8086",
        "influxdb_database": "hv_test",
        "influx_measurement": "hv_test",
        "influx_device": "win10",
        "influx_batch_size": "100",
        "influx_flush_interval_s": "1.0",
        "influx_timeout_s": "3.0",
    }
    SAFETY_DEFAULTS = {
        "preflight_vacuum_max_pa": "1e-3",
        "preflight_vacuum_max_age_s": "5",
        "short_step_delay_warning_s": "0.5",
        "vacuum_alarm_enabled": "true",
        "vacuum_alarm_max_pa": "1e-3",
        "vacuum_alarm_cooldown_s": "10",
        "vacuum_alarm_action": "warn",
    }

    def __init__(self, mw):
        self.mw = mw

    def load_to_ui(self):
        try:
            self._load_power_devices()
            self._load_high_voltage_config()
            self._load_meter_config()
            self._load_vacuum_config()
            self._load_safety_config()
            self._load_keithley_config()
            self._load_stabilization_params()
            self._load_test_params()
            self._load_recording_config()
            self._load_remote_config()
            self._load_retention_config()
            self._refresh_loaded_state()
            self.mw.log_message("Configuration loaded")
        except Exception as exc:
            self.mw.log_message(f"Failed to load configuration: {exc}")

    def _load_power_devices(self):
        self.mw._load_power_devices_from_config()

    def _load_high_voltage_config(self):
        config = self.mw.config
        if not config.has_section("HighVoltage"):
            return
        self.mw.hv_port_combo.setCurrentText(config.get("HighVoltage", "port", fallback=""))
        self.mw.hv_baudrate_combo.setCurrentText(
            config.get("HighVoltage", "baudrate", fallback="9600")
        )

    def _load_meter_config(self):
        for meter_type in self.METER_TYPES:
            self._load_meter_port(meter_type)
            self._load_meter_coefficient(meter_type)

    def _load_meter_port(self, meter_type: str):
        port_value = self.mw.config.get("Multimeter", f"{meter_type}_port", fallback="")
        self.mw.set_meter_port(meter_type, port_value)

    def _load_meter_coefficient(self, meter_type: str):
        coeff_value = self.mw.config.get("Multimeter", f"{meter_type}_coeff", fallback="1.0")
        try:
            getattr(self.mw, f"{meter_type}_coeff").setText(str(coeff_value))
        except Exception:
            pass
        try:
            if meter_type in self.mw.meter_data:
                self.mw.meter_data[meter_type]["coefficient"] = float(coeff_value)
        except Exception:
            pass

    def _load_vacuum_config(self):
        config = self.mw.config
        self.mw.set_vacuum_type(config.get("Multimeter", "vacuum_type", fallback="CM52"))
        self.mw.set_vacuum_channel(config.get("Multimeter", "vacuum_channel", fallback="3"))
        self.mw.set_vacuum_baudrate(config.get("Multimeter", "vacuum_baudrate", fallback="19200"))
        self.mw.set_vacuum_unit(config.get("Multimeter", "vacuum_unit", fallback="Pa"))

    def _load_safety_config(self):
        config = self.mw.config
        self.mw.set_vacuum_alarm_max_pa(
            config.get(
                "Safety",
                "vacuum_alarm_max_pa",
                fallback=self.SAFETY_DEFAULTS["vacuum_alarm_max_pa"],
            )
        )

    def _load_keithley_config(self):
        self.mw.keithley_addr_combo.setCurrentText(
            self.mw.config.get(
                "Keithley248",
                "gpib_address",
                fallback=self.mw.keithley_addr_combo.currentText(),
            )
        )

    def _load_stabilization_params(self):
        params = self._load_param_bundle(
            "Keithley248",
            self.mw.stabilization_params,
            self.STABILIZATION_FIELDS,
            source_name_fallback=self.mw._get_selected_power_name("stabilization"),
        )
        self.mw.apply_stabilization_params(params)

    def _load_test_params(self):
        params = self._load_param_bundle(
            "TestParameters",
            self.mw.test_params,
            self.TEST_FIELDS,
            source_name_fallback=self.mw._get_selected_power_name("test"),
        )
        self.mw.apply_test_params(params)

    def _load_param_bundle(self, section: str, current_values, fields, *, source_name_fallback: str):
        params = {
            "power_source": self.mw.config.get(
                section,
                "power_source",
                fallback=current_values.get("power_source", "auto"),
            ),
            "power_source_name": self.mw.config.get(
                section,
                "power_source_name",
                fallback=source_name_fallback,
            ),
        }
        for field in fields:
            params[field] = self.mw.config.get(section, field, fallback=current_values.get(field))
        return params

    def _load_recording_config(self):
        if hasattr(self.mw, "interval_edit"):
            self.mw.interval_edit.setText(
                self.mw.config.get("TestParameters", "save_interval", fallback="1")
            )
        self.mw.set_record_file_path(
            self.mw.config.get("DataRecord", "save_path", fallback="")
        )

    def _load_remote_config(self):
        self.mw.set_remote_host(
            self.mw.config.get("RemoteControl", "host", fallback="127.0.0.1")
        )
        self.mw.set_remote_port(
            self.mw.config.get("RemoteControl", "port", fallback="8000")
        )

    def _load_retention_config(self):
        try:
            self._load_retention_days_and_runs()
            self._load_retention_archive_settings()
            self._load_retention_vacuum_mode()
        except Exception:
            pass

    def _load_retention_days_and_runs(self):
        if hasattr(self.mw, "db_keep_days_edit") and self.mw.config.has_option("Retention", "keep_days"):
            self.mw.db_keep_days_edit.setText(self.mw.config.get("Retention", "keep_days"))
        if hasattr(self.mw, "db_keep_runs_edit") and self.mw.config.has_option("Retention", "keep_runs"):
            self.mw.db_keep_runs_edit.setText(self.mw.config.get("Retention", "keep_runs"))

    def _load_retention_archive_settings(self):
        if hasattr(self.mw, "db_archive_chk") and self.mw.config.has_option(
            "Retention", "archive_before_delete"
        ):
            retention_flag = str(
                self.mw.config.get("Retention", "archive_before_delete")
            ).strip().lower()
            self.mw.db_archive_chk.setChecked(retention_flag not in ("0", "false", "no"))
        if hasattr(self.mw, "db_archive_dir_edit") and self.mw.config.has_option(
            "Retention", "archive_dir"
        ):
            self.mw.db_archive_dir_edit.setText(self.mw.config.get("Retention", "archive_dir"))

    def _load_retention_vacuum_mode(self):
        if hasattr(self.mw, "db_vacuum_mode_combo") and self.mw.config.has_option(
            "Retention", "vacuum_mode"
        ):
            vacuum_mode = str(self.mw.config.get("Retention", "vacuum_mode")).strip().lower()
            self.mw.db_vacuum_mode_combo.setCurrentIndex(0 if vacuum_mode != "vacuum" else 1)

    def _refresh_loaded_state(self):
        try:
            self.mw.update_db_status_label()
        except Exception:
            pass
        self.mw.update_settings_display()
        self.mw.update_power_summary_label()
        self.mw.update_power_action_buttons()

    def build_config_data(self):
        config_data = {
            "HighVoltage": self._build_high_voltage_section(),
            "Multimeter": self._build_multimeter_section(),
            "Keithley248": self._build_keithley_section(),
            "TestParameters": self._build_test_parameters_section(),
            "DataRecord": self._build_data_record_section(),
            "PowerSources": self.mw._power_devices_config_section(),
            "RemoteControl": self._build_remote_control_section(),
            "Monitoring": self._build_monitoring_section(),
            "Safety": self._build_safety_section(),
        }
        retention_section = self._build_retention_section()
        if retention_section:
            config_data["Retention"] = retention_section
        return config_data

    def _build_high_voltage_section(self):
        return {
            "port": self.mw.hv_port_combo.currentText(),
            "baudrate": self.mw.hv_baudrate_combo.currentText(),
        }

    def _build_multimeter_section(self):
        section = {}
        for meter_type in self.METER_TYPES:
            section[f"{meter_type}_port"] = self.mw.get_meter_port(meter_type)
            section[f"{meter_type}_coeff"] = getattr(self.mw, f"{meter_type}_coeff").text()
        section["vacuum_type"] = self.mw.get_vacuum_type()
        section["vacuum_channel"] = self.mw.get_vacuum_channel()
        section["vacuum_baudrate"] = self.mw.get_vacuum_baudrate()
        section["vacuum_unit"] = self.mw.get_vacuum_unit()
        return section

    def _build_keithley_section(self):
        return {
            "gpib_address": self.mw.keithley_addr_combo.currentText(),
            "power_source": self.mw.stabilization_params.get("power_source", "auto"),
            "power_source_name": self.mw.stabilization_params.get(
                "power_source_name",
                AUTO_POWER_SOURCE_NAME,
            ),
            "current_source": self.mw.stabilization_params["current_source"],
            "target_current": str(self.mw.stabilization_params["target_current"]),
            "stability_range": str(self.mw.stabilization_params["stability_range"]),
            "start_voltage": str(self.mw.stabilization_params["start_voltage"]),
            "adjust_frequency": str(self.mw.stabilization_params["adjust_frequency"]),
            "max_adjust_voltage": str(self.mw.stabilization_params["max_adjust_voltage"]),
            "algorithm": str(self.mw.stabilization_params.get("algorithm", "pid")),
            "pid_kp": str(self.mw.stabilization_params.get("pid_kp", 0.05)),
            "pid_ki": str(self.mw.stabilization_params.get("pid_ki", 0.01)),
            "pid_kd": str(self.mw.stabilization_params.get("pid_kd", 0.0)),
        }

    def _build_test_parameters_section(self):
        return {
            "power_source": self.mw.test_params.get("power_source", "auto"),
            "power_source_name": self.mw.test_params.get(
                "power_source_name",
                AUTO_POWER_SOURCE_NAME,
            ),
            "start_voltage": str(self.mw.test_params["start_voltage"]),
            "target_voltage": str(self.mw.test_params["target_voltage"]),
            "voltage_step": str(self.mw.test_params["voltage_step"]),
            "step_delay": str(self.mw.test_params["step_delay"]),
            "cycle_time": str(self.mw.test_params["cycle_time"]),
            "save_interval": self.mw.interval_edit.text(),
        }

    def _build_data_record_section(self):
        return {"save_path": self.mw.get_record_file_path()}

    def _build_remote_control_section(self):
        return {
            "host": self.mw.get_remote_host(),
            "port": str(self.mw.get_remote_port()),
            "enabled": "true" if self.mw.is_remote_control_enabled() else "false",
        }

    def _build_monitoring_section(self):
        section = {
            "enable_influxdb": "true" if self.mw.is_influx_enabled() else "false",
            "influxdb_org": self.mw.get_influx_org(),
            "influxdb_bucket": self.mw.get_influx_bucket(),
            "influxdb_token": self.mw.get_influx_token(),
        }
        for key, default in self.MONITORING_DEFAULTS.items():
            section[key] = self.mw.config.get("Monitoring", key, fallback=default)
        return section

    def _build_safety_section(self):
        section = {}
        for key, default in self.SAFETY_DEFAULTS.items():
            section[key] = self.mw.config.get("Safety", key, fallback=default)
        section["vacuum_alarm_max_pa"] = str(self.mw.get_vacuum_alarm_max_pa())
        return section

    def _build_retention_section(self):
        try:
            if not (hasattr(self.mw, "db_keep_days_edit") and hasattr(self.mw, "db_keep_runs_edit")):
                return None
            keep_days = int(float(self.mw.db_keep_days_edit.text() or self.mw.retention_policy.keep_days))
            keep_runs = int(float(self.mw.db_keep_runs_edit.text() or self.mw.retention_policy.keep_runs))
            archive_before_delete = (
                bool(getattr(self.mw, "db_archive_chk", None).isChecked())
                if hasattr(self.mw, "db_archive_chk")
                else bool(self.mw.retention_policy.archive_before_delete)
            )
            vacuum_mode = (
                str(getattr(self.mw, "db_vacuum_mode_combo", None).currentData() or "incremental")
                if hasattr(self.mw, "db_vacuum_mode_combo")
                else str(self.mw.retention_policy.vacuum_mode)
            )
            archive_dir = (
                str(getattr(self.mw, "db_archive_dir_edit", None).text())
                if hasattr(self.mw, "db_archive_dir_edit")
                else self.mw.retention_policy.archive_dir
            )
            return {
                "enabled": "true",
                "keep_days": str(keep_days),
                "keep_runs": str(keep_runs),
                "archive_before_delete": "true" if archive_before_delete else "false",
                "archive_dir": archive_dir,
                "vacuum_mode": str(vacuum_mode),
            }
        except Exception:
            return None

    def save_from_ui(self):
        try:
            self.mw.config_manager.save_config(self.build_config_data())
            self.mw.config = self.mw.config_manager.load_config()
            self.mw.log_message("Configuration saved")
        except Exception as exc:
            self.mw.log_message(f"Failed to save configuration: {exc}")
