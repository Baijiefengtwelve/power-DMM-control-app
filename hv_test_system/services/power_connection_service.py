from __future__ import annotations

from ..keithley_controller import Keithley248Controller
from ..thread_power import HVConnectThread


class PowerConnectionService:
    """Own HAPS06 and Keithley connection flows for named power supplies."""

    def __init__(self, mw):
        self.mw = mw

    @property
    def device_manager(self):
        return self.mw.device_manager

    def _disconnect_block_reason(self, name: str) -> str:
        mw = self.mw
        clean_name = str(name or "").strip()
        if not clean_name:
            return ""

        active_test = str(getattr(mw, "active_test_power_source", "") or "").strip()
        if getattr(mw, "is_testing", False) and active_test == clean_name:
            return "该电源正在用于测试，请先停止测试后再断联"

        active_stabilization = str(getattr(mw, "active_stabilization_power_source", "") or "").strip()
        if getattr(mw, "is_stabilizing", False) and active_stabilization == clean_name:
            return "该电源正在用于稳流，请先停止稳流后再断联"
        return ""

    def connect_named_power_device(self, name: str):
        mw = self.mw
        device = mw._find_power_device(name)
        if not device:
            mw.log_message(f"未找到电源配置: {name}")
            return False

        power_type = mw.normalize_power_type(device.get("type"))
        clean_name = str(name or "").strip()
        block_reason = self._disconnect_block_reason(clean_name)
        if block_reason:
            mw.log_message(block_reason)
            self._show_status_message(block_reason)
            return False

        if power_type == "HAPS06":
            if self.device_manager.is_hv_connecting():
                if self.device_manager.is_hv_connecting_for(clean_name):
                    return True
                mw.log_message("高压源正在连接中，请稍候后重试")
                return False

            port = str(device.get("address", "")).strip()
            baud = str(device.get("baudrate", "9600") or "9600").strip()
            if not port:
                mw.log_message(f"电源 {name} 未设置串口")
                return False

            current_name = mw.connected_power_name_by_type.get("HAPS06")
            if current_name and current_name != clean_name:
                if not self.disconnect_named_power_device(current_name):
                    return False

            mw.hv_port_combo.setCurrentText(port)
            mw.hv_baudrate_combo.setCurrentText(baud)
            if bool(getattr(mw.hv_controller, "is_connected", False)):
                if not self.disconnect_named_power_device(
                    mw.connected_power_name_by_type.get("HAPS06") or clean_name
                ):
                    return False

            mw.pending_haps06_power_name = clean_name
            self.toggle_hv_connection()
            return True

        addr = str(device.get("address", "")).strip()
        if not addr:
            mw.log_message(f"电源 {name} 未设置 GPIB 地址")
            return False

        existing = getattr(mw, "connected_named_power_controllers", {}).get(clean_name)
        if existing is not None and bool(getattr(existing, "is_connected", False)):
            mw._refresh_keithley_controller_alias(preferred_name=clean_name)
            self.device_manager.update_power_summary_label()
            mw.update_power_action_buttons()
            return True

        controller = getattr(mw, "named_keithley_controllers", {}).get(clean_name)
        if controller is None:
            controller = Keithley248Controller()
            mw.named_keithley_controllers[clean_name] = controller

        mw.log_message(f"正在连接 Keithley 电源 {name}: {addr}...")
        success, message = controller.connect_gpib(addr)
        if not success:
            mw.log_message(f"Keithley 电源 {name} 连接失败: {message}")
            return False

        mw.connected_named_power_controllers[clean_name] = controller
        mw.connected_power_name_by_type["Keithley 248"] = clean_name
        try:
            mw.keithley_addr_combo.setCurrentText(addr)
            mw.keithley_addr_combo.setEnabled(False)
            mw.keithley_connect_btn.setText("断开")
        except Exception:
            pass

        mw._refresh_keithley_controller_alias(preferred_name=clean_name)
        mw._auto_bind_connected_power_to_modules(clean_name)
        self.device_manager.update_power_summary_label()
        mw.update_power_action_buttons()
        mw.update_settings_display()
        try:
            mw._keithley_v_ts = 0.0
            mw.update_keithley_voltage()
        except Exception:
            pass
        mw.log_message(f"Keithley 电源 {name} 已连接: {message}")
        return True

    def disconnect_named_power_device(self, name: str):
        mw = self.mw
        device = mw._find_power_device(name)
        if not device:
            return False

        power_type = mw.normalize_power_type(device.get("type"))
        clean_name = str(name or "").strip()
        block_reason = self._disconnect_block_reason(clean_name)
        if block_reason:
            mw.log_message(block_reason)
            self._show_status_message(block_reason)
            return False

        if power_type == "HAPS06":
            if self.device_manager.is_hv_connecting_for(clean_name):
                mw.log_message("高压源正在连接中，请等待连接结果")
                return False
            if bool(getattr(mw.hv_controller, "is_connected", False)):
                self.toggle_hv_connection()
            mw.connected_named_power_controllers.pop(clean_name, None)
            old_haps_name = mw.connected_power_name_by_type.get("HAPS06")
            mw.connected_power_name_by_type["HAPS06"] = None
            mw.pending_haps06_power_name = None
            try:
                if old_haps_name:
                    mw.connected_named_power_controllers.pop(old_haps_name, None)
            except Exception:
                pass
        else:
            controller = getattr(mw, "connected_named_power_controllers", {}).pop(clean_name, None)
            if controller is None:
                controller = getattr(mw, "named_keithley_controllers", {}).get(clean_name)
            try:
                if controller is not None and bool(getattr(controller, "is_connected", False)):
                    controller.disconnect()
            except Exception:
                pass

            remaining = mw._get_connected_keithley_names()
            mw.connected_power_name_by_type["Keithley 248"] = remaining[-1] if remaining else None
            mw._refresh_keithley_controller_alias(
                preferred_name=mw.connected_power_name_by_type.get("Keithley 248")
            )
            try:
                if remaining:
                    last_address = str((mw._find_power_device(remaining[-1]) or {}).get("address", ""))
                    mw.keithley_addr_combo.setCurrentText(last_address)
                    mw.keithley_addr_combo.setEnabled(False)
                    mw.keithley_connect_btn.setText("断开")
                else:
                    mw.keithley_addr_combo.setEnabled(True)
                    mw.keithley_connect_btn.setText("连接")
                    mw.keithley_voltage_label.setText("未连接")
                mw.keithley_voltage_label.setStyleSheet(
                    "font-size: 12pt; font-weight: bold; color: #d93025; padding: 4px; background-color: #fce8e6; border: 1px solid #ea4335; border-radius: 4px;"
                )
            except Exception:
                pass

        self.device_manager.update_power_summary_label()
        mw.update_power_action_buttons()
        return True

    def start_hv_connection_async(self, port: str, baudrate: int):
        mw = self.mw
        if self.device_manager.is_hv_connecting():
            mw.log_message("高压源正在连接中，请稍候...")
            return

        try:
            mw.hv_connect_btn.setEnabled(False)
            mw.hv_connect_btn.setText("连接中...")
        except Exception:
            pass
        try:
            mw.hv_port_combo.setEnabled(False)
            mw.hv_baudrate_combo.setEnabled(False)
        except Exception:
            pass
        try:
            mw.hv_refresh_btn.setEnabled(False)
        except Exception:
            pass
        try:
            mw.start_test_btn.setEnabled(False)
            mw.cycle_test_btn.setEnabled(False)
            mw.reset_btn.setEnabled(False)
            mw.manual_set_btn.setEnabled(False)
        except Exception:
            pass
        self._show_status_message("高压源连接中...")

        mw._hv_connect_thread = HVConnectThread(mw.hv_controller, port, baudrate, remote_timeout_s=1.5)
        try:
            mw._hv_connect_thread.progress.connect(lambda message: mw.log_message(f"[HAPS06] {message}"))
        except Exception:
            try:
                mw._hv_connect_thread.progress.connect(mw.log_message)
            except Exception:
                pass
        mw._hv_connect_thread.finished.connect(self.on_hv_connect_finished)
        mw._hv_connect_thread.start()

    def on_hv_connect_finished(self, success: bool, message: str, port: str):
        mw = self.mw
        mw._hv_connect_thread = None
        try:
            mw.hv_connect_btn.setEnabled(True)
        except Exception:
            pass

        if success:
            try:
                mw.hv_connect_btn.setText("断开高压源")
            except Exception:
                pass
            try:
                haps_name = mw.pending_haps06_power_name or mw.connected_power_name_by_type.get("HAPS06")
                mw.connected_power_name_by_type["HAPS06"] = haps_name
                if haps_name:
                    mw.connected_named_power_controllers[haps_name] = mw.hv_controller
                    mw._auto_bind_connected_power_to_modules(haps_name)
            except Exception:
                pass
            try:
                mw.hv_port_combo.setEnabled(False)
                mw.hv_baudrate_combo.setEnabled(False)
            except Exception:
                pass
            try:
                mw.hv_refresh_btn.setEnabled(False)
            except Exception:
                pass

            mw.update_power_action_buttons()
            self.device_manager.update_power_summary_label()
            mw.log_message(f"高压源已连接到: {port}")
            mw.log_message(str(message))
            self.device_manager.attach_hv_worker_signals()
            self._show_status_message(f"高压源已连接 - {port}")
            self.device_manager.start_hv_voltage_poller(interval_ms=800)
            return

        mw.connected_power_name_by_type["HAPS06"] = None
        mw.pending_haps06_power_name = None
        self.device_manager.detach_hv_worker_signals()
        try:
            mw.hv_controller.disconnect()
        except Exception:
            pass
        try:
            mw.hv_port_combo.setEnabled(True)
            mw.hv_baudrate_combo.setEnabled(True)
        except Exception:
            pass
        try:
            mw.hv_refresh_btn.setEnabled(True)
        except Exception:
            pass
        try:
            mw.hv_connect_btn.setText("连接高压源")
        except Exception:
            pass
        try:
            mw.hv_voltage_label.setText("未连接")
            mw.hv_voltage_label.setStyleSheet("font-size: 12pt; font-weight: bold; color: #d93025; padding: 4px; background-color: #fce8e6; border: 1px solid #ea4335; border-radius: 4px;")
        except Exception:
            pass

        mw.update_power_action_buttons()
        self.device_manager.update_power_summary_label()
        mw.log_message(f"高压源连接失败: {message}")
        self._show_status_message("高压源连接失败")

    def toggle_hv_connection(self):
        mw = self.mw
        try:
            if self.device_manager.is_hv_connecting():
                mw.log_message("高压源正在连接中，请稍候...")
                return

            if not bool(getattr(mw.hv_controller, "is_connected", False)):
                port = mw.hv_port_combo.currentText()
                baudrate = int(mw.hv_baudrate_combo.currentText())
                if not port:
                    mw.log_message("错误: 请选择高压源串口")
                    return
                mw.log_message(f"正在连接高压源: {port}, 波特率: {baudrate}...")
                self.start_hv_connection_async(port, baudrate)
                return

            hv_name = str(
                mw.connected_power_name_by_type.get("HAPS06")
                or mw.pending_haps06_power_name
                or ""
            ).strip()
            block_reason = self._disconnect_block_reason(hv_name)
            if block_reason:
                mw.log_message(block_reason)
                self._show_status_message(block_reason)
                return

            mw.countdown_manager.stop()
            mw.countdown_label.setText("")
            self.device_manager.stop_hv_voltage_poller()
            self.device_manager.detach_hv_worker_signals()

            mw.hv_controller.disconnect()
            mw.hv_port_combo.setEnabled(True)
            mw.hv_baudrate_combo.setEnabled(True)
            try:
                mw.hv_refresh_btn.setEnabled(True)
            except Exception:
                pass
            mw.hv_connect_btn.setText("连接高压源")
            old_haps_name = mw.connected_power_name_by_type.get("HAPS06")
            mw.connected_power_name_by_type["HAPS06"] = None
            mw.pending_haps06_power_name = None
            try:
                if old_haps_name:
                    mw.connected_named_power_controllers.pop(old_haps_name, None)
            except Exception:
                pass
            mw.hv_voltage_label.setText("未连接")
            mw.hv_voltage_label.setStyleSheet("font-size: 12pt; font-weight: bold; color: #d93025; padding: 4px; background-color: #fce8e6; border: 1px solid #ea4335; border-radius: 4px;")
            mw.update_power_action_buttons()
            self.device_manager.update_power_summary_label()
            mw.log_message("高压源已断开")
            self._show_status_message("高压源已断开")
        except Exception as exc:
            mw.log_message(f"高压源连接/断开错误: {exc}")

    def toggle_keithley_connection(self):
        mw = self.mw
        try:
            is_connected = bool(mw._get_connected_keithley_names()) or bool(
                getattr(mw.keithley_controller, "is_connected", False)
            )
            if not is_connected:
                resource_name = mw.keithley_addr_combo.currentText()
                if not resource_name:
                    mw.log_message("错误: 请选择或输入 GPIB 地址")
                    return
                target_name = next(
                    (
                        device["name"]
                        for device in mw.power_devices
                        if mw.normalize_power_type(device.get("type")) == "Keithley 248"
                        and str(device.get("address", "")).strip() == str(resource_name).strip()
                    ),
                    None,
                )
                if not target_name:
                    target_name = mw._ensure_unique_power_name("Keithley电源")
                    mw.power_devices.append(
                        {"name": target_name, "type": "Keithley 248", "address": str(resource_name).strip(), "baudrate": ""}
                    )
                ok = self.connect_named_power_device(target_name)
                self._show_status_message(f"Keithley 248{'已连接' if ok else '连接失败'} - {resource_name}")
                return

            target_name = mw.connected_power_name_by_type.get("Keithley 248")
            if not target_name:
                names = mw._get_connected_keithley_names()
                target_name = names[-1] if names else None
            if target_name:
                self.disconnect_named_power_device(target_name)
            self._show_status_message("Keithley 248 已断开")
        except Exception as exc:
            mw.log_message(f"Keithley 248 连接/断开错误: {exc}")

    def _show_status_message(self, message):
        try:
            if hasattr(self.mw, "show_status_message"):
                self.mw.show_status_message(message)
            else:
                self.mw.status_bar.showMessage(str(message))
        except Exception:
            pass
