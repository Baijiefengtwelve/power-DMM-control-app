from __future__ import annotations

import os

from PyQt5.QtWidgets import QComboBox

from ..meter_devices import (
    canonicalize_meter_device_id,
    find_meter_device_option,
    list_meter_device_dicts,
    list_meter_device_options,
    list_serial_port_names,
)


class PanelStateService:
    """Own widget-backed panel state access for ports, vacuum, remote, and Influx."""

    INFLUX_TOKEN_ENV_VAR = "HV_INFLUX_TOKEN"
    INFLUX_TOKEN_ENV_PLACEHOLDER = "ENV:HV_INFLUX_TOKEN"

    def __init__(self, mw):
        self.mw = mw

    def get_serial_port_list(self):
        return list_serial_port_names()

    def get_meter_device_options(self):
        return list_meter_device_options()

    def get_meter_device_option_dicts(self):
        return list_meter_device_dicts()

    def get_meter_device_option(self, device_id: str):
        return find_meter_device_option(device_id)

    def get_gpib_resource_list(self):
        try:
            import pyvisa

            rm = pyvisa.ResourceManager()
            return [r for r in rm.list_resources() if "GPIB" in r.upper()]
        except Exception:
            return []

    def get_meter_port(self, meter_type: str) -> str:
        try:
            text = getattr(self.mw, f"{meter_type}_port_combo").currentText()
        except Exception:
            return ""
        if str(meter_type or "").strip() == "vacuum":
            return str(text or "")
        return canonicalize_meter_device_id(text)

    def set_meter_port(self, meter_type: str, port_text: str):
        try:
            combo = getattr(self.mw, f"{meter_type}_port_combo")
            value = str(port_text or "").strip()
            if str(meter_type or "").strip() != "vacuum":
                value = canonicalize_meter_device_id(value)
            combo.setCurrentText(value)
        except Exception:
            pass

    def is_meter_connected(self, meter_type: str) -> bool:
        try:
            return meter_type in self.mw.meter_threads
        except Exception:
            return False

    def get_vacuum_type(self) -> str:
        mw = self.mw
        try:
            combo = getattr(mw, "vacuum_type_combo", None)
            if combo is not None:
                value = combo.currentData() if hasattr(combo, "currentData") else combo.currentText()
                value = str(value or combo.currentText() or "").strip()
                if value:
                    return value
        except Exception:
            pass
        return mw.config.get("Multimeter", "vacuum_type", fallback="CM52")

    def set_vacuum_type(self, value: str):
        mw = self.mw
        try:
            norm = str(value or "CM52").strip() or "CM52"
            if not hasattr(mw, "vacuum_type_combo"):
                parent = getattr(mw, "_compat_widget_host", None) or mw
                mw.vacuum_type_combo = QComboBox(parent)
                mw.vacuum_type_combo.addItem("Leybold COMBIVAC CM52", "CM52")
                mw.vacuum_type_combo.addItem("睿宝 ReBorn（Modbus RTU）", "REBORN_RTU")
                mw.vacuum_type_combo.addItem("Agilent XGS-600（RS232）", "AGILENT_XGS600")
                mw.vacuum_type_combo.hide()
            idx = mw.vacuum_type_combo.findData(norm)
            if idx >= 0:
                mw.vacuum_type_combo.setCurrentIndex(idx)
            else:
                mw.vacuum_type_combo.setCurrentText(norm)
        except Exception:
            pass

    def get_vacuum_channel(self) -> str:
        try:
            return str(getattr(self.mw, "vacuum_channel_combo", None).currentText())
        except Exception:
            return self.mw.config.get("Multimeter", "vacuum_channel", fallback="3")

    def set_vacuum_channel(self, value: str):
        mw = self.mw
        try:
            if not hasattr(mw, "vacuum_channel_combo"):
                parent = getattr(mw, "_compat_widget_host", None) or mw
                mw.vacuum_channel_combo = QComboBox(parent)
                mw.vacuum_channel_combo.setEditable(True)
                mw.vacuum_channel_combo.addItems([str(i) for i in range(1, 13)])
                mw.vacuum_channel_combo.hide()
            mw.vacuum_channel_combo.setCurrentText(str(value or "3"))
        except Exception:
            pass

    def get_vacuum_baudrate(self) -> str:
        try:
            return str(getattr(self.mw, "vacuum_baudrate_combo", None).currentText())
        except Exception:
            return self.mw.config.get("Multimeter", "vacuum_baudrate", fallback="19200")

    def set_vacuum_baudrate(self, value: str):
        mw = self.mw
        try:
            if not hasattr(mw, "vacuum_baudrate_combo"):
                parent = getattr(mw, "_compat_widget_host", None) or mw
                mw.vacuum_baudrate_combo = QComboBox(parent)
                mw.vacuum_baudrate_combo.addItems(["9600", "19200", "38400", "57600"])
                mw.vacuum_baudrate_combo.hide()
            mw.vacuum_baudrate_combo.setCurrentText(str(value or "19200"))
        except Exception:
            pass

    def get_vacuum_unit(self) -> str:
        try:
            combo = getattr(self.mw, "vacuum_unit_combo", None)
            if combo is not None:
                return str(combo.currentText() or "Pa")
        except Exception:
            pass
        return self.mw.config.get("Multimeter", "vacuum_unit", fallback="Pa")

    def set_vacuum_unit(self, value: str):
        mw = self.mw
        try:
            if not hasattr(mw, "vacuum_unit_combo"):
                parent = getattr(mw, "_compat_widget_host", None) or mw
                mw.vacuum_unit_combo = QComboBox(parent)
                mw.vacuum_unit_combo.addItems(["Pa", "Torr", "mbar"])
                mw.vacuum_unit_combo.hide()
            mw.vacuum_unit_combo.setCurrentText(str(value or "Pa"))
        except Exception:
            pass

    def get_remote_host(self) -> str:
        return str(self.mw.remote_control_config.get("host", "127.0.0.1"))

    def get_remote_port(self) -> int:
        try:
            return int(self.mw.remote_control_config.get("port", 8000))
        except Exception:
            return 8000

    def set_remote_host(self, host: str):
        host = str(host or "").strip() or "127.0.0.1"
        self.mw.remote_control_config["host"] = host

    def set_remote_port(self, port):
        try:
            self.mw.remote_control_config["port"] = int(float(port))
        except Exception:
            self.mw.remote_control_config["port"] = 8000

    def is_remote_control_enabled(self) -> bool:
        cb = self.mw._service_runtime.get("web_status")
        try:
            return bool(cb()) if callable(cb) else False
        except Exception:
            return False

    def set_remote_control_enabled(self, enabled: bool):
        try:
            if enabled:
                cb = self.mw._service_runtime.get("web_start")
                if callable(cb):
                    cb(self.get_remote_host(), self.get_remote_port())
                else:
                    self.mw.log_message("当前启动方式不支持运行时开启远程控制")
            else:
                cb = self.mw._service_runtime.get("web_stop")
                if callable(cb):
                    cb()
        except Exception as e:
            self.mw.log_message(f"切换远程控制失败: {e}")

    def get_remote_status_text(self) -> str:
        if self.is_remote_control_enabled():
            return f"运行中：http://{self.get_remote_host()}:{self.get_remote_port()}"
        return "已关闭（程序重启后默认保持关闭）"

    def parse_influx_url(self):
        url = self.mw.config.get("Monitoring", "influxdb_url", fallback="http://127.0.0.1:8086")
        host = "127.0.0.1"
        port = "8086"
        try:
            txt = str(url).strip()
            txt = txt.replace("http://", "").replace("https://", "")
            host_port = txt.split("/")[0]
            if ":" in host_port:
                host, port = host_port.rsplit(":", 1)
            else:
                host = host_port
        except Exception:
            pass
        return host, port

    def get_influx_host(self) -> str:
        return self.parse_influx_url()[0]

    def get_influx_port(self) -> str:
        return self.parse_influx_url()[1]

    def set_influx_host_port(self, host: str, port):
        host = str(host or "").strip() or "127.0.0.1"
        try:
            port = int(float(port))
        except Exception:
            port = 8086
        if not self.mw.config.has_section("Monitoring"):
            self.mw.config.add_section("Monitoring")
        self.mw.config.set("Monitoring", "influxdb_url", f"http://{host}:{port}")

    def get_influx_org(self) -> str:
        return self.mw.config.get("Monitoring", "influxdb_org", fallback="hv_lab")

    def set_influx_org(self, value: str):
        if not self.mw.config.has_section("Monitoring"):
            self.mw.config.add_section("Monitoring")
        self.mw.config.set("Monitoring", "influxdb_org", str(value or "hv_lab"))

    def get_influx_bucket(self) -> str:
        return self.mw.config.get("Monitoring", "influxdb_bucket", fallback="hv_test")

    def set_influx_bucket(self, value: str):
        if not self.mw.config.has_section("Monitoring"):
            self.mw.config.add_section("Monitoring")
        self.mw.config.set("Monitoring", "influxdb_bucket", str(value or "hv_test"))

    def get_influx_token(self) -> str:
        env_token = str(os.getenv(self.INFLUX_TOKEN_ENV_VAR, "") or "").strip()
        if env_token:
            return env_token
        token = str(self.mw.config.get("Monitoring", "influxdb_token", fallback="") or "").strip()
        if token == self.INFLUX_TOKEN_ENV_PLACEHOLDER:
            return ""
        return token

    def set_influx_token(self, value: str):
        if not self.mw.config.has_section("Monitoring"):
            self.mw.config.add_section("Monitoring")
        self.mw.config.set("Monitoring", "influxdb_token", str(value or ""))

    def is_influx_enabled(self) -> bool:
        cb = self.mw._service_runtime.get("influx_status")
        try:
            return bool(cb()) if callable(cb) else False
        except Exception:
            return False

    def set_influx_enabled(self, enabled: bool):
        try:
            if enabled:
                cb = self.mw._service_runtime.get("influx_start")
                if callable(cb):
                    cb()
                else:
                    self.mw.log_message("当前启动方式不支持运行时开启 InfluxDB")
            else:
                cb = self.mw._service_runtime.get("influx_stop")
                if callable(cb):
                    cb()
        except Exception as e:
            self.mw.log_message(f"切换 InfluxDB 失败: {e}")

    def get_influx_status_text(self) -> str:
        if self.is_influx_enabled():
            return f"运行中：{self.mw.config.get('Monitoring', 'influxdb_url', fallback='http://127.0.0.1:8086')}"
        return "已关闭（程序重启后默认保持关闭）"
