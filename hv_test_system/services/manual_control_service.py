from __future__ import annotations


class ManualControlService:
    """Own direct operator actions such as setting voltage manually."""

    MIN_VOLTAGE = 0.0
    MAX_VOLTAGE = 10000.0

    def __init__(self, mw):
        self.mw = mw

    def manual_set_voltage(self):
        try:
            target_name = ""
            if hasattr(self.mw, "manual_voltage_target_combo"):
                target_name = str(self.mw.manual_voltage_target_combo.currentText() or "").strip()
            
            if not target_name:
                self.mw.log_message("错误: 请先选择目标电源")
                return False

            sources = self.mw.power_catalog_service.connected_power_sources()
            controller = sources.get(target_name)
            if not controller:
                self.mw.log_message(f"错误: 电源 {target_name} 未连接")
                return False

            if getattr(self.mw, "is_testing", False):
                test_source = str(getattr(self.mw, "active_test_power_source", "") or "").strip()
                if test_source == target_name:
                    self.mw.log_message("警告: 该电源正在执行升压测试，手动设置无效！")
                    return False

            if getattr(self.mw, "is_stabilizing", False):
                stab_source = str(getattr(self.mw, "active_stabilization_power_source", "") or "").strip()
                if stab_source == target_name:
                    self.mw.log_message("警告: 该电源正在执行稳流测试，手动设置无效！")
                    return False

            voltage = self._parse_voltage_input()
            if voltage is None:
                return False

            self.mw.log_message(f"正在手动设置电压: {voltage}V... 目标电源: {target_name}")

            if self._should_enable_hv(controller, voltage):
                try:
                    controller.enable_high_voltage()
                except Exception:
                    pass

            success, message = controller.manual_set_voltage(voltage)
            if success:
                self.mw.log_message(f"手动设置电压成功: {message}")
                try:
                    self.mw.manual_voltage_edit.clear()
                    self.mw.manual_voltage_edit.setFocus()
                except Exception:
                    pass
                return True

            self.mw.log_message(f"手动设置电压失败: {message}")
            return False
        except Exception as exc:
            self.mw.log_message(f"手动设置电压错误: {exc}")
            return False

    def _parse_voltage_input(self):
        voltage_str = str(self.mw.manual_voltage_edit.text() or "").strip()
        if not voltage_str:
            self.mw.log_message("错误: 请输入电压值")
            return None

        try:
            voltage = float(voltage_str)
        except ValueError:
            self.mw.log_message("错误: 请输入有效的电压值")
            return None

        if voltage < self.MIN_VOLTAGE or voltage > self.MAX_VOLTAGE:
            self.mw.log_message("错误: 电压值应在 0-10000V 范围内")
            return None
        return voltage

    def _should_enable_hv(self, controller, voltage: float) -> bool:
        return not (
            getattr(controller, "power_source_key", "") == "keithley"
            and abs(float(voltage)) <= 1e-12
        )
