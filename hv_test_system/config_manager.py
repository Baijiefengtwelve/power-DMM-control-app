from __future__ import annotations

import configparser
import json
import os

from .constants import CONFIG_FILE


class ConfigManager:
    """Manage loading, saving, and generating the default project config."""

    INFLUX_TOKEN_ENV_PLACEHOLDER = "ENV:HV_INFLUX_TOKEN"

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config_file = CONFIG_FILE

    def load_config(self):
        if not os.path.exists(self.config_file):
            self.create_default_config()
        self.config.read(self.config_file, encoding="utf-8")
        if self._merge_missing_defaults():
            with open(self.config_file, "w", encoding="utf-8") as config_file:
                self.config.write(config_file)
        return self.config

    def save_config(self, config_data):
        for section, options in config_data.items():
            if not self.config.has_section(section):
                self.config.add_section(section)
            for key, value in options.items():
                self.config.set(section, key, str(value))

        with open(self.config_file, "w", encoding="utf-8") as config_file:
            self.config.write(config_file)

    def create_default_config(self):
        self.config.read_dict(self._default_config_sections())
        with open(self.config_file, "w", encoding="utf-8") as config_file:
            self.config.write(config_file)

    def _default_config_sections(self):
        return {
            "HighVoltage": self._build_high_voltage_section(),
            "Multimeter": self._build_multimeter_section(),
            "Keithley248": self._build_keithley_section(),
            "TestParameters": self._build_test_parameter_section(),
            "DataRecord": self._build_data_record_section(),
            "PlotColors": self._build_plot_color_section(),
            "PlotSettings": self._build_plot_settings_section(),
            "PowerSources": self._build_power_source_section(),
            "RemoteControl": self._build_remote_control_section(),
            "Monitoring": self._build_monitoring_section(),
            "Safety": self._build_safety_section(),
            "SQLite": self._build_sqlite_section(),
            "Retention": self._build_retention_section(),
        }

    def _merge_missing_defaults(self) -> bool:
        changed = False
        for section, options in self._default_config_sections().items():
            if not self.config.has_section(section):
                self.config.add_section(section)
                changed = True
            for key, value in options.items():
                if self.config.has_option(section, key):
                    continue
                self.config.set(section, key, str(value))
                changed = True
        return changed

    def _build_high_voltage_section(self):
        return {"port": "", "baudrate": "9600"}

    def _build_multimeter_section(self):
        return {
            "cathode_port": "",
            "cathode_coeff": "1.0",
            "gate_port": "",
            "gate_coeff": "1.0",
            "anode_port": "",
            "anode_coeff": "1.0",
            "backup_port": "",
            "backup_coeff": "1.0",
            "vacuum_port": "",
            "vacuum_coeff": "1.0",
            "vacuum_channel": "3",
            "vacuum_baudrate": "19200",
            "vacuum_type": "CM52",
            "vacuum_unit": "Pa",
        }

    def _build_keithley_section(self):
        return {
            "gpib_address": "14",
            "current_source": "cathode",
            "target_current": "1000",
            "stability_range": "5",
            "start_voltage": "100",
            "adjust_frequency": "1",
            "max_adjust_voltage": "50",
            "algorithm": "pid",
            "pid_kp": "0.05",
            "pid_ki": "0.01",
            "pid_kd": "0.0",
        }

    def _build_test_parameter_section(self):
        return {
            "start_voltage": "0",
            "target_voltage": "1000",
            "voltage_step": "10",
            "step_delay": "1",
            "cycle_time": "10",
            "save_interval": "1",
        }

    def _build_data_record_section(self):
        return {"save_path": ""}

    def _build_plot_color_section(self):
        return {
            "cathode": "#E74C3C",
            "gate": "#2ECC71",
            "anode": "#3498DB",
            "backup": "#F39C12",
            "keithley_voltage": "#9B59B6",
            "gate_plus_anode": "#E67E22",
            "anode_cathode_ratio": "#1ABC9C",
            "vacuum": "#7F8C8D",
        }

    def _build_plot_settings_section(self):
        return {"max_points": "0"}

    def _build_power_source_section(self):
        devices = [
            {
                "name": "\u9ad8\u538b\u7535\u6e90",
                "type": "HAPS06",
                "address": "",
                "baudrate": "9600",
            },
            {
                "name": "Keithley\u7535\u6e90",
                "type": "Keithley 248",
                "address": "14",
                "baudrate": "",
            },
        ]
        return {"devices_json": json.dumps(devices, ensure_ascii=False)}

    def _build_remote_control_section(self):
        return {"host": "127.0.0.1", "port": "8000", "enabled": "false"}

    def _build_monitoring_section(self):
        return {
            "enable_influxdb": "false",
            "influxdb_mode": "v2",
            "influxdb_url": "http://127.0.0.1:8086",
            "influxdb_org": "hv_lab",
            "influxdb_bucket": "hv_test",
            "influxdb_token": self.INFLUX_TOKEN_ENV_PLACEHOLDER,
            "influxdb_database": "hv_test",
            "influx_measurement": "hv_test",
            "influx_device": "win10",
            "influx_batch_size": "100",
            "influx_flush_interval_s": "1.0",
            "influx_timeout_s": "3.0",
        }

    def _build_safety_section(self):
        return {
            "preflight_vacuum_max_pa": "1e-3",
            "preflight_vacuum_max_age_s": "5",
            "short_step_delay_warning_s": "0.5",
            "vacuum_alarm_enabled": "true",
            "vacuum_alarm_max_pa": "1e-3",
            "vacuum_alarm_cooldown_s": "10",
            "vacuum_alarm_action": "warn",
        }

    def _build_sqlite_section(self):
        return {
            "path": os.path.join("data", "session.sqlite"),
            "journal_mode": "WAL",
            "synchronous": "NORMAL",
            "auto_vacuum": "INCREMENTAL",
            "commit_every_rows": "200",
            "commit_every_ms": "500",
        }

    def _build_retention_section(self):
        return {
            "enabled": "true",
            "keep_days": "30",
            "keep_runs": "200",
            "archive_before_delete": "true",
            "archive_dir": os.path.join("data", "archive"),
            "vacuum_mode": "incremental",
        }
