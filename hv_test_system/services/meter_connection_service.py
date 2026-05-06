from __future__ import annotations

from ..hid_meters import UT61EPlusHIDThread, Victor86EHIDThread
from ..meter_devices import decode_meter_device_id
from ..thread_meters import (
    AgilentXGS600Thread,
    CM52Thread,
    RebornRTUVacuumThread,
    SerialThread,
)


class MeterConnectionService:
    """Own meter-thread creation, connect/disconnect flow, and cleanup."""

    METER_NAMES = {
        "cathode": "阴极",
        "gate": "栅极",
        "anode": "阳极",
        "backup": "收集极",
        "vacuum": "真空",
    }

    def __init__(
        self,
        mw,
        *,
        serial_thread_factory=None,
        ut61eplus_thread_factory=None,
        victor_thread_factory=None,
        cm52_factory=None,
        reborn_rtu_factory=None,
        agilent_xgs600_factory=None,
    ):
        self.mw = mw
        self.serial_thread_factory = serial_thread_factory or SerialThread
        self.ut61eplus_thread_factory = ut61eplus_thread_factory or UT61EPlusHIDThread
        self.victor_thread_factory = victor_thread_factory or Victor86EHIDThread
        self.cm52_factory = cm52_factory or CM52Thread
        self.reborn_rtu_factory = reborn_rtu_factory or RebornRTUVacuumThread
        self.agilent_xgs600_factory = agilent_xgs600_factory or AgilentXGS600Thread

    def create_meter_thread(self, meter_type: str, port: str):
        mw = self.mw
        if meter_type != "vacuum":
            device = decode_meter_device_id(port)
            if device.get("transport") == "hid":
                protocol = str(device.get("protocol") or "").strip().lower()
                if protocol == "ut61eplus":
                    return self.ut61eplus_thread_factory(device, meter_type)
                if protocol == "victor86e":
                    return self.victor_thread_factory(device, meter_type)
                raise ValueError(f"Unsupported HID protocol: {protocol or '<empty>'}")
            return self.serial_thread_factory(str(device.get("port") or port or ""), meter_type)

        vac_type = str(mw.get_vacuum_type() or "CM52").strip()
        try:
            selector = int(float(mw.get_vacuum_channel() or "3"))
        except Exception:
            selector = 3
        try:
            default_baud = "9600" if vac_type in ("REBORN_RTU", "AGILENT_XGS600") else "19200"
            baud = int(float(mw.get_vacuum_baudrate() or default_baud))
        except Exception:
            baud = 9600 if vac_type in ("REBORN_RTU", "AGILENT_XGS600") else 19200

        if vac_type == "REBORN_RTU":
            return self.reborn_rtu_factory(port=port, slave_address=max(1, selector), baudrate=baud, poll_ms=400)
        if vac_type == "AGILENT_XGS600":
            return self.agilent_xgs600_factory(
                port=port,
                sensor_index=max(1, selector),
                baudrate=baud,
                unit=mw.get_vacuum_unit(),
                poll_ms=400,
            )
        return self.cm52_factory(port=port, channel=max(1, selector), baudrate=baud, poll_ms=300)

    def disconnect_meter_thread(self, meter_type: str, *, reset_ui: bool):
        mw = self.mw
        thread = mw.meter_threads.pop(meter_type, None)
        if thread is not None:
            try:
                thread.data_received.disconnect()
            except Exception:
                pass
            try:
                thread.log_message_signal.disconnect()
            except Exception:
                pass
            try:
                thread.stop()
            except Exception:
                pass
            try:
                if thread.isRunning():
                    thread.wait(1500)
                if thread.isRunning():
                    thread.terminate()
                    thread.wait(300)
            except Exception:
                pass

        if not reset_ui:
            return

        try:
            getattr(mw, f"{meter_type}_port_combo").setEnabled(True)
        except Exception:
            pass
        try:
            getattr(mw, f"{meter_type}_connect_btn").setText("连接")
        except Exception:
            pass
        try:
            getattr(mw, f"{meter_type}_value_label").setText("未连接")
        except Exception:
            pass

    def toggle_meter_connection(self, meter_type: str):
        mw = self.mw
        meter_type = str(meter_type or "").strip()
        if meter_type not in self.METER_NAMES:
            mw.log_message(f"Unknown multimeter type: {meter_type}")
            return

        try:
            port_combo = getattr(mw, f"{meter_type}_port_combo")
            connect_btn = getattr(mw, f"{meter_type}_connect_btn")
            value_label = getattr(mw, f"{meter_type}_value_label")
            meter_name = self.METER_NAMES[meter_type]

            if meter_type not in mw.meter_threads:
                port = port_combo.currentText()
                if not port:
                    mw.log_message(f"Error: select a device for {meter_name} first")
                    return

                mw.log_message(f"Connecting {meter_name}: {port}")
                thread = self.create_meter_thread(meter_type, port)
                thread.data_received.connect(mw.handle_meter_data)
                thread.log_message_signal.connect(mw.log_message)
                thread.start()

                mw.meter_threads[meter_type] = thread
                connect_btn.setText("断开")
                port_combo.setEnabled(False)
                value_label.setText("读取中...")
                self._show_status_message(f"{meter_name} connected")
                return

            self.disconnect_meter_thread(meter_type, reset_ui=True)
            mw.log_message(f"{meter_name} disconnected")
            self._show_status_message(f"{meter_name} disconnected")
        except Exception as exc:
            mw.log_message(f"Meter connect/disconnect error: {exc}")

    def _show_status_message(self, message):
        try:
            if hasattr(self.mw, "show_status_message"):
                self.mw.show_status_message(message)
            else:
                self.mw.status_bar.showMessage(str(message))
        except Exception:
            pass
