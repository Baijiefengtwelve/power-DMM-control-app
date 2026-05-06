from __future__ import annotations


class TestControlService:
    """Handle direct test control actions that are triggered from the main window or web commands."""

    def __init__(self, mw):
        self.mw = mw

    def run_test(self, start_voltage, target_voltage, voltage_step, step_delay, cycle_time, is_cycle):
        controller = getattr(self.mw, "active_test_controller", None)
        if controller is None:
            controller, source_key, err = self.mw._resolve_power_controller("test")
            if controller is None:
                self.mw.log_message(f"错误: {err}")
                return
            self.mw.active_test_controller = controller
            self.mw.active_test_power_source = source_key

        return self.mw.test_service.run_with_controller(
            controller,
            start_voltage,
            target_voltage,
            voltage_step,
            step_delay,
            cycle_time,
            is_cycle,
        )

    def calculate_and_save_cycle_min(self):
        try:
            if not self.mw.current_cycle_anode_data:
                return

            min_anode = min(item[0] for item in self.mw.current_cycle_anode_data)
            min_data = next(item for item in self.mw.current_cycle_anode_data if item[0] == min_anode)
            min_voltage = min_data[1]
            min_time = min_data[2]

            self.mw.cycle_data.append(
                {
                    "cycle": self.mw.current_cycle,
                    "min_anode": min_anode,
                    "voltage": min_voltage,
                    "time": min_time,
                }
            )
            try:
                self.mw.data_saver.append_cycle_row(self.mw.current_cycle, min_anode, min_voltage, min_time)
                self.mw.log_message(
                    f"第{self.mw.current_cycle}次循环 - 最小阳极值: {min_anode:.4f}, 对应电压: {min_voltage}, 时间: {min_time}"
                )
                self.mw.current_cycle_anode_data = []
            except Exception as exc:
                self.mw.log_message(f"保存循环数据失败: {exc}")
        except Exception as exc:
            self.mw.log_message(f"计算循环最小值失败: {exc}")

    def update_ui_after_test(self):
        self.mw.active_test_controller = None
        self.mw.active_test_power_source = None
        self.mw.update_power_action_buttons()

        if not self.mw.is_testing:
            self.mw.log_message("测试完成")
        else:
            self.mw.log_message("测试已停止")

    def stop_test(self):
        try:
            self.mw.test_service.stop()
        except Exception:
            self.mw.is_testing = False

        try:
            if self.mw.current_cycle_anode_data:
                self.calculate_and_save_cycle_min()
        except Exception:
            pass

        self.mw.is_cycle_testing = False
        self.mw.countdown_manager.stop()
        self.mw.countdown_label.setText("")

        if self.mw.auto_recording and self.mw.is_recording:
            self.mw.auto_recording = False
            self.mw.toggle_record()
            self.mw.log_message("测试已停止，自动停止记录数据")

        controller = getattr(self.mw, "active_test_controller", None)
        if controller is None:
            controller, _, _ = self.mw._resolve_power_controller("test")
        if controller is not None:
            try:
                controller.stop_output()
            except Exception:
                pass

        self.mw.active_test_controller = None
        self.mw.active_test_power_source = None
        self.mw._on_test_state_change({"testing": False})
        self.mw.log_message("测试停止，输出电压已置零")

    def reset_voltage(self):
        controller, source_key, err = self.mw._resolve_power_controller("test")
        if controller is None:
            self.mw.log_message(f"错误: {err}")
            return

        success, message = controller.stop_output()
        if success:
            self.mw.log_message(f"已通过 {self.mw._power_source_name(source_key)} 将输出安全归零")
        else:
            self.mw.log_message(f"安全归零失败: {message}")

    def emergency_stop(self):
        self.mw.log_message("紧急停止触发，正在安全关断所有输出")

        try:
            self.mw.test_service.stop()
        except Exception:
            pass
        self.mw.is_testing = False
        self.mw.is_cycle_testing = False

        try:
            self.mw.stop_current_stabilization()
        except Exception:
            pass
        self.mw.is_stabilizing = False

        try:
            self.mw.countdown_manager.stop()
            self.mw.countdown_label.setText("")
        except Exception:
            pass

        if self.mw.is_recording:
            try:
                self.mw.auto_recording = False
            except Exception:
                pass
            try:
                self.mw.toggle_record()
            except Exception:
                pass

        for name, controller in list(self.mw._connected_power_sources().items()):
            if controller is None:
                continue
            try:
                success, message = controller.stop_output()
                if success:
                    self.mw.log_message(f"已安全关断 {name}: {message}")
                else:
                    self.mw.log_message(f"关断 {name} 失败: {message}")
            except Exception as exc:
                self.mw.log_message(f"关断 {name} 失败: {exc}")
            try:
                self.mw._set_power_voltage_cache(name, 0.0)
            except Exception:
                pass

        try:
            self.mw.refresh_power_voltage_slots()
        except Exception:
            pass

        self.mw.active_test_controller = None
        self.mw.active_test_power_source = None
        self.mw.active_stabilization_controller = None
        self.mw.active_stabilization_power_source = None
        self.mw._on_test_state_change({"testing": False, "countdown_stop": True})
        try:
            self.mw.update_power_action_buttons()
        except Exception:
            pass
        self.mw.log_message("紧急停止完成，所有已连接电源均已执行安全关断")
