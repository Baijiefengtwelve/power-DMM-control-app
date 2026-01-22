from __future__ import annotations

import threading
import time

from PyQt5.QtCore import QObject, pyqtSignal


class TestService(QObject):
    """Orchestrates single/cycle tests.

    This service intentionally keeps the existing behavior (step logic, logging,
    recording toggles) but moves the long-running test loop out of MainWindow.
    """

    log = pyqtSignal(str)
    started = pyqtSignal(bool)  # cycle?
    finished = pyqtSignal()
    state_change = pyqtSignal(dict)

    def __init__(self, mw, parent=None):
        super().__init__(parent)
        self.mw = mw

    def start(self, cycle: bool):
        """Start a test (single or cycle)."""
        self._start_test(cycle=cycle)

    def stop(self):
        self.mw.is_testing = False

    def _start_test(self, cycle: bool = False):
        try:
            start_v = self.mw.test_params['start_voltage']
            target_v = self.mw.test_params['target_voltage']
            step_v = self.mw.test_params['voltage_step']
            delay = self.mw.test_params['step_delay']
            cycle_time = self.mw.test_params['cycle_time'] if cycle else 0

            if start_v == target_v:
                self.log.emit("错误: 起始电压和目标电压不能相同")
                return
            if step_v <= 0:
                self.log.emit("错误: 电压增幅必须大于0")
                return

            if start_v < target_v:
                self.mw.test_mode = "升压"
                self.log.emit(f"检测到升压测试模式: {start_v}V -> {target_v}V")
            else:
                self.mw.test_mode = "降压"
                self.log.emit(f"检测到降压测试模式: {start_v}V -> {target_v}V")

            self.mw.is_testing = True
            self.mw.is_cycle_testing = cycle

            # UI 状态交给 MainWindow 处理，但通过信号通知
            self.state_change.emit({
                "testing": True,
                "cycle": cycle,
                "countdown_stop": True,
            })

            # 自动开始记录（保持原逻辑）
            if self.mw.path_label.text() and self.mw.path_label.text() != "未选择保存路径" and not self.mw.is_recording:
                self.mw.auto_recording = True
                self.mw.toggle_record()
                self.log.emit(("循环测试" if cycle else "单次测试") + "已自动开始记录数据")

            if cycle:
                self.mw.current_cycle = 0
                self.mw.cycle_data = []
                self.mw.current_cycle_anode_data = []

            self.log.emit(f"开始{'循环测试' if cycle else '单次测试'}...")

            t = threading.Thread(
                target=self._run_test,
                args=(start_v, target_v, step_v, delay, cycle_time, cycle),
                daemon=True,
            )
            t.start()
            self.started.emit(cycle)
        except Exception as e:
            self.log.emit(f"测试启动失败: {e}")

    def _run_test(self, start_voltage, target_voltage, voltage_step, step_delay, cycle_time, is_cycle):
        try:
            cycle_count = 0
            while self.mw.is_testing and (is_cycle or cycle_count == 0):
                cycle_count += 1
                self.mw.current_cycle = cycle_count
                self.log.emit(f"开始第 {cycle_count} 轮测试")
                # 循环测试：写入CSV标记行用于分隔不同循环的数据
                if is_cycle and self.mw.is_recording:
                    try:
                        self.mw.data_saver.add_marker_row(f"第{cycle_count}次循环")
                        self.log.emit(f"已写入第{cycle_count}次循环标记行")
                    except Exception as e:
                        self.log.emit(f"写入循环标记行失败: {e}")

                if is_cycle and self.mw.is_recording:
                    self.mw.current_cycle_anode_data = []

                ok, msg = self.mw.hv_controller.set_voltage_only(start_voltage)
                if ok:
                    self.log.emit(f"设置起始电压: {start_voltage:.1f}V - {msg}")
                    time.sleep(step_delay * 0.5)
                else:
                    self.log.emit(f"设置起始电压失败: {msg}")
                    break

                time.sleep(step_delay)

                if is_cycle and self.mw.is_recording:
                    self.mw.cycle_recording_active = True
                    self.log.emit("测试期间数据记录已激活")

                ramp_failed = False
                if self.mw.test_mode == "升压":
                    current_voltage = start_voltage
                    while current_voltage <= target_voltage and self.mw.is_testing:
                        ok, msg = self.mw.hv_controller.set_voltage_only(current_voltage)
                        self.log.emit((f"设置电压: {current_voltage:.1f}V - {msg}") if ok else f"设置电压失败: {msg}")
                        if not ok:
                            ramp_failed = True
                            break
                        current_voltage += voltage_step
                        time.sleep(step_delay)
                else:
                    current_voltage = start_voltage
                    while current_voltage >= target_voltage and self.mw.is_testing:
                        ok, msg = self.mw.hv_controller.set_voltage_only(current_voltage)
                        self.log.emit((f"设置电压: {current_voltage:.1f}V - {msg}") if ok else f"设置电压失败: {msg}")
                        if not ok:
                            ramp_failed = True
                            break
                        current_voltage -= voltage_step
                        time.sleep(step_delay)

                if not self.mw.is_testing:
                    break

                if not is_cycle:
                    # 单次测试完成：降低到100V（保持原逻辑）
                    try:
                        if ramp_failed:
                            self.log.emit("单次测试未完整到达目标电压，尝试降低电压到100V")
                        else:
                            self.log.emit("单次测试完成，降低电压到100V")
                        ok2, msg2 = self.mw.hv_controller.set_voltage_only(100.0)
                        if ok2:
                            self.log.emit(f"电压已设置为100V - {msg2}")
                        else:
                            self.log.emit(f"电压下降失败: {msg2}")
                    except Exception as e:
                        self.log.emit(f"单次测试复位电压失败: {e}")
                    break

                if is_cycle:
                    if self.mw.is_recording and self.mw.current_cycle_anode_data:
                        try:
                            self.mw.calculate_and_save_cycle_min()
                        except Exception as e:
                            self.log.emit(f"计算并记录循环最小值失败: {e}")

                    if self.mw.is_recording:
                        self.mw.cycle_recording_active = False
                        self.log.emit("等待期间数据记录已暂停")

                    self.log.emit(f"到达目标电压，降至100V等待 {cycle_time} 秒")
                    drop_ok, drop_msg = self.mw.hv_controller.set_voltage_only(100.0)
                    if drop_ok:
                        self.log.emit(f"降压成功: {drop_msg}")
                    else:
                        self.log.emit(f"降压失败: {drop_msg}")
                        break

                    # 倒计时
                    try:
                        self.state_change.emit({"countdown_start": int(cycle_time)})
                    except Exception:
                        pass
                    start_t = time.time()
                    while self.mw.is_testing and (time.time() - start_t) < cycle_time:
                        time.sleep(0.2)
                    try:
                        self.state_change.emit({"countdown_stop": True})
                    except Exception:
                        pass


            # 结束
            self.mw.is_testing = False
            self.mw.is_cycle_testing = False
            if is_cycle and getattr(self.mw, 'is_recording', False):
                self.mw.cycle_recording_active = False
            self.state_change.emit({"testing": False, "cycle": is_cycle, "countdown_stop": True})
        except Exception as e:
            self.log.emit(f"测试运行异常: {e}")
        finally:
            self.finished.emit()

    def _calculate_and_save_cycle_min(self):
        """Compatibility: delegate to MainWindow.calculate_and_save_cycle_min()."""
        try:
            self.mw.calculate_and_save_cycle_min()
        except Exception as e:
            self.log.emit(f"计算并记录循环最小值失败: {e}")
