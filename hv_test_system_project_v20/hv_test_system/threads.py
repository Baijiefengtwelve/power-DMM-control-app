from __future__ import annotations

import datetime

from .common import *
# 类型注解在 Python 默认会被运行时求值；这里需要显式导入，避免 NameError。
from .controllers import HAPS06Controller

class HVVoltagePoller(QThread):
    """后台轮询HAPS06实际电压（UI实时更新，但不阻塞主线程）"""

    voltage_updated = pyqtSignal(float)
    poll_error = pyqtSignal(str)

    def __init__(self, hv_controller: HAPS06Controller, interval_ms: int = 500, parent=None):
        super().__init__(parent)
        self.hv_controller = hv_controller
        self.interval_ms = max(100, int(interval_ms))
        self._running = True
        self._fail_count = 0
        self._last_error_emit = 0.0

    def stop(self):
        self._running = False
        # 最多等待1.5s，避免关闭程序时卡住
        self.wait(1500)

    def run(self):
        while self._running:
            try:
                if self.hv_controller and getattr(self.hv_controller, "is_connected", False):
                    v = self.hv_controller.read_actual_voltage()
                    if v is not None:
                        self._fail_count = 0
                        try:
                            self.voltage_updated.emit(float(v))
                        except Exception:
                            pass
                    else:
                        self._fail_count += 1
                        now = time.time()
                        # 每10秒最多提示一次，并且连续失败>=5次才提示
                        if (now - self._last_error_emit) > 10.0 and self._fail_count >= 5:
                            self._last_error_emit = now
                            self.poll_error.emit("HAPS06实际电压读取失败（无响应或CRC错误）")
                else:
                    # 未连接时不刷错误
                    self._fail_count = 0
            except Exception as e:
                now = time.time()
                if (now - self._last_error_emit) > 10.0:
                    self._last_error_emit = now
                    try:
                        self.poll_error.emit(f"HAPS06轮询异常: {e}")
                    except Exception:
                        pass

            self.msleep(self.interval_ms)

class HVConnectThread(QThread):
    """后台连接HAPS06，避免UI线程因串口/通讯超时而卡死。

    - 连接串口 + 探测地址（由 HAPS06Controller.connect_serial 完成）
    - 启用远控（可设置较短超时）

    finished: (success, message, port)
    """

    progress = pyqtSignal(str)
    finished = pyqtSignal(bool, str, str)

    def __init__(self, hv_controller: HAPS06Controller, port: str, baudrate: int = 9600, remote_timeout_s: float = 1.5, parent=None):
        super().__init__(parent)
        self.hv_controller = hv_controller
        self.port = str(port)
        self.baudrate = int(baudrate)
        self.remote_timeout_s = float(remote_timeout_s)

    def run(self):
        try:
            self.progress.emit(f"连接串口: {self.port} @ {self.baudrate}...")
            ok, msg = self.hv_controller.connect_serial(self.port, self.baudrate)
            if not ok:
                self.finished.emit(False, str(msg), self.port)
                return

            self.progress.emit("启用远程控制(远控)...")
            ok2, msg2 = self.hv_controller.enable_remote_control(timeout_s=self.remote_timeout_s)
            if not ok2:
                try:
                    self.hv_controller.disconnect()
                except Exception:
                    pass
                self.finished.emit(False, f"启用远控失败: {msg2}", self.port)
                return

            self.finished.emit(True, "连接成功并已启用远控", self.port)
        except Exception as e:
            try:
                self.hv_controller.disconnect()
            except Exception:
                pass
            self.finished.emit(False, f"连接异常: {e}", self.port)


class PIDController:
    """PID控制器（带死区、抗积分饱和与方向保护的更稳健实现）"""

    def __init__(self, kp=0.05, ki=0.01, kd=0.0):
        # 默认参数更保守，减少超调；如需更快响应可在代码中调整
        self.kp = kp
        self.ki = ki
        self.kd = kd

        self.integral = 0.0
        self.previous_error = 0.0

        # 积分限幅（避免积分项过大导致越调越离谱）
        self.integral_limit = 1e6

        # 输出限幅（最大单次电压调整）
        self.output_limit = 50.0

    def calculate(self, setpoint, measured, dt=1.0, deadband=0.0):
        """计算PID输出

        - deadband：死区（|error|<=deadband 时输出0，并对积分做轻微衰减）
        - 抗积分饱和：当输出已饱和且误差仍推动同方向饱和时，暂停本次积分累计
        - 过零抑制：误差换向时清空积分，减少“已经过冲仍继续加压”的现象
        """
        if dt is None or dt <= 0:
            dt = 1.0

        error = float(setpoint) - float(measured)

        # 死区：进入稳定范围不调压，并让积分缓慢衰减，避免离开死区后“顶着积分继续加压”
        if deadband and abs(error) <= float(deadband):
            self.integral *= 0.9
            self.previous_error = error
            return 0.0

        # 误差换向（过冲）时，清空积分，抑制继续同方向加压
        if self.previous_error != 0 and (error * self.previous_error) < 0:
            self.integral = 0.0

        # 比例项
        p = self.kp * error

        # 微分项（对误差求导）
        d = self.kd * (error - self.previous_error) / dt

        # 先计算未积分版本的输出，辅助做抗积分饱和判断
        pre_output = p + d

        # 积分候选值
        integral_candidate = self.integral + error * dt

        # 依据输出限幅与Ki，给积分一个更合理的默认限幅
        if self.ki != 0:
            # 让积分项最大贡献不超过 2*output_limit
            self.integral_limit = max(self.integral_limit, abs(2.0 * self.output_limit / self.ki))
        # 积分限幅
        if integral_candidate > self.integral_limit:
            integral_candidate = self.integral_limit
        elif integral_candidate < -self.integral_limit:
            integral_candidate = -self.integral_limit

        # 计算带积分的输出
        i = self.ki * integral_candidate
        output = pre_output + i

        # 输出限幅
        output_sat = output
        if output_sat > self.output_limit:
            output_sat = self.output_limit
        elif output_sat < -self.output_limit:
            output_sat = -self.output_limit

        # 抗积分饱和：已饱和且误差仍推动同方向饱和 -> 不接受本次积分累计
        if output != output_sat:
            if (output_sat >= self.output_limit and error > 0) or (output_sat <= -self.output_limit and error < 0):
                # 保持原积分
                pass
            else:
                self.integral = integral_candidate
        else:
            self.integral = integral_candidate

        self.previous_error = error
        return float(output_sat)

    def reset(self):
        """重置PID控制器"""
        self.integral = 0.0
        self.previous_error = 0.0

class CurrentStabilizationThread(QThread):
    """电流稳定控制线程"""

    update_voltage_signal = pyqtSignal(float)
    update_status_signal = pyqtSignal(str)
    stabilization_complete_signal = pyqtSignal()

    def __init__(self, keithley_controller, meter_data, data_mutex, params):
        super().__init__()
        self.keithley_controller = keithley_controller
        self.meter_data = meter_data
        self.data_mutex = data_mutex
        self.params = params  # 稳流参数
        self.running = False
        self.pid = PIDController()

    def run(self):
        """运行稳流控制（持续运行；仅手动停止）"""
        self.running = True
        self.update_status_signal.emit("开始稳流控制...")

        # PID输出限幅 = 每次最大调整电压
        try:
            self.pid.output_limit = float(self.params.get("max_adjust_voltage", self.pid.output_limit))
        except Exception:
            pass
        self.pid.reset()

        # 基本参数
        target_current = float(self.params.get("target_current", 0.0))
        algo = str(self.params.get("algorithm", "pid") or "pid").strip().lower()
        if algo not in ("pid", "approach"):
            algo = "pid"  # uA
        deadband = float(self.params.get("stability_range", 0.0))  # uA
        adjust_period = float(self.params.get("adjust_frequency", 1.0))  # s
        if adjust_period <= 0:
            adjust_period = 1.0

        # Keithley 248 的读数/面板刷新有延迟，建议最小周期 >= 1s
        sleep_period = max(adjust_period, 1.0)

        # 进入稳定区间后只提示一次；离开后允许再次提示
        stable_notified = False

        # 1) 设置起始电压（以“设定值”为基准做后续增量，避免VOUT滞后导致从0开始）
        start_v = float(self.params.get("start_voltage", 0.0))
        if self.keithley_controller.is_connected:
            success, message = self.keithley_controller.set_voltage(start_v)
            if success:
                self.update_status_signal.emit(f"设置起始电压: {start_v}V")
            else:
                self.update_status_signal.emit(f"设置起始电压失败: {message}")
                self.running = False
                return

        # 统一处理极性：用 u = polarity * V 作为“有效控制量”（通常与电流单调正相关）
        polarity = 1.0
        if start_v < 0:
            polarity = -1.0

        # 用设定值做后续控制基准（不要每次都用VOUT?，它可能滞后约1s）
        self._set_voltage = start_v
        set_u = polarity * self._set_voltage  # 有效控制量（应为非负）
        if set_u < 0:
            set_u = -set_u
        self.update_voltage_signal.emit(self._set_voltage)

        # --- 稳流控制的自适应/抗振荡状态 ---
        # 电流滤波（抑制读数噪声导致的“来回打满步长”）
        filt_alpha = float(self.params.get("current_filter_alpha", 0.3))  # 0~1，越大越跟随
        if filt_alpha <= 0 or filt_alpha >= 1:
            filt_alpha = 0.3
        current_filt = None  # uA

        # 估计 dI/du (uA/V)，用于把误差换算成更合适的 ΔV，避免过冲振荡
        slope_est = None
        slope_alpha = 0.4  # EMA 更新系数
        last_u_for_slope = None
        last_i_for_slope = None

        # 粗调/微调模式带滞回，避免在阈值附近反复切换
        coarse_mode = False
        coarse_enter_mult = float(self.params.get("coarse_enter_mult", 6.0))  # 进入粗调阈值：|e| > mult*deadband
        coarse_exit_mult = float(self.params.get("coarse_exit_mult", 2.5))  # 退出粗调阈值：|e| < mult*deadband
        if coarse_exit_mult >= coarse_enter_mult:
            coarse_exit_mult = max(1.5, coarse_enter_mult * 0.5)

        # 振荡检测：误差符号频繁翻转则自动降低“有效最大步长”
        sign_flip_window = []
        last_err_sign = 0
        # 读数有效性：设定后等待至少 settle_time 再用该读数进行下一次调节
        settle_time = float(self.params.get("settle_time", 1.2))  # s，248 建议 >=1s
        last_set_time = time.time()

        # 2) 开启高压输出
        success, message = self.keithley_controller.enable_high_voltage()
        if success:
            self.update_status_signal.emit("高压输出已开启")
        else:
            self.update_status_signal.emit(f"开启高压输出失败: {message}")
            self.running = False
            return

        # 给电压/电流一点时间稳定（手册建议电压变化后约1s再读更可靠）
        time.sleep(1.0)

        # 3) 稳流控制循环（持续运行）
        while self.running:
            try:
                current_value = self.get_current_value()  # uA
                if current_value is None:
                    self.update_status_signal.emit("无法获取电流值")
                    time.sleep(sleep_period)
                    continue

                # 误差：目标-测量
                error = target_current - float(current_value)

                # 死区：不调压，但持续运行
                if deadband and abs(error) <= deadband:
                    if not stable_notified:
                        stable_notified = True
                        self.update_status_signal.emit(
                            f"电流进入稳定区间: {current_value:.2f}uA (目标 {target_current}uA, ±{deadband}uA)"
                        )
                        # 兼容原逻辑：只在首次进入稳定区间时发一次“完成”信号（不会停止线程）
                        self.stabilization_complete_signal.emit()

                    # 让PID内部积分衰减，避免离开死区后继续“顶着积分加压”
                    try:
                        self.pid.calculate(target_current, current_value, dt=sleep_period, deadband=deadband)
                    except Exception:
                        pass

                    time.sleep(sleep_period)
                    continue

                # 离开稳定区间后，允许再次提示
                stable_notified = False

                # --- 接近算法（1V步进） ---
                # 逻辑：以起始电压为基准，每次按 1V 调整接近目标电流；
                #      I < (target-deadband) -> +1V
                #      I > (target+deadband) -> -1V
                #      区间内保持电压不变
                if algo == "approach":
                    lower = target_current - float(deadband)
                    upper = target_current + float(deadband)
                    du = 0.0
                    try:
                        if float(current_value) < float(lower):
                            du = 1.0
                        elif float(current_value) > float(upper):
                            du = -1.0
                        else:
                            du = 0.0
                    except Exception:
                        du = 0.0

                    # 限幅：每次最大调整电压
                    try:
                        eff_max_step = float(self.params.get("max_adjust_voltage", 50.0))
                        if eff_max_step > 0:
                            if du > eff_max_step:
                                du = eff_max_step
                            elif du < -eff_max_step:
                                du = -eff_max_step
                    except Exception:
                        pass

                    # 更新有效控制量，并保持非负
                    set_u = float(set_u) + float(du)
                    if set_u < 0:
                        set_u = 0.0

                    # 转回实际设定电压
                    new_voltage = polarity * set_u

                    success, message = self.keithley_controller.set_voltage(new_voltage)
                    if success:
                        self._set_voltage = new_voltage
                        self.update_voltage_signal.emit(new_voltage)
                        last_set_time = time.time()
                        self.update_status_signal.emit(
                            f"[接近] I={float(current_value):.2f}uA, 目标={target_current}uA, ΔV={du:.0f}V, Vset={new_voltage:.1f}V"
                        )
                    else:
                        self.update_status_signal.emit(f"[接近] 设置电压失败: {message}")

                    time.sleep(sleep_period)
                    continue

                # PID计算：输出为“本次有效控制量调整量（Δu，单位V）”
                # 目标：扰动后快速回到目标且不振荡
                # 1) 对电流做轻微滤波，抑制噪声
                if current_filt is None:
                    current_filt = float(current_value)
                else:
                    current_filt = float(filt_alpha) * float(current_value) + (1.0 - float(filt_alpha)) * float(
                        current_filt)

                # 2) 更新斜率估计 dI/du（仅在电压真正发生变化且读数有效时）
                if last_u_for_slope is not None and abs(float(set_u) - float(last_u_for_slope)) > 1e-9:
                    di = float(current_filt) - float(last_i_for_slope)
                    du_tmp = float(set_u) - float(last_u_for_slope)
                    slope = di / du_tmp  # uA/V
                    # 期望 slope 为正；剔除异常点
                    if slope > 1e-9 and slope < 1e6:
                        if slope_est is None:
                            slope_est = slope
                        else:
                            slope_est = slope_alpha * slope + (1.0 - slope_alpha) * slope_est

                # 3) 粗调/微调带滞回：误差大时用“基于斜率的快速逼近”，误差小用 PID 微调
                abs_err = abs(error)
                db = float(deadband) if deadband and deadband > 0 else 1e-9
                enter_th = coarse_enter_mult * db
                exit_th = coarse_exit_mult * db

                if coarse_mode:
                    if abs_err < exit_th:
                        coarse_mode = False
                else:
                    if abs_err > enter_th:
                        coarse_mode = True

                # 4) 振荡检测：误差符号频繁翻转则自动降低有效最大步长（避免来回“打满”）
                err_sign = 1 if error > 0 else (-1 if error < 0 else 0)
                now_t = time.time()
                if last_err_sign != 0 and err_sign != 0 and err_sign != last_err_sign and abs_err > db:
                    sign_flip_window.append(now_t)
                    # 只保留最近 12 秒内的翻转
                    sign_flip_window = [t for t in sign_flip_window if now_t - t <= 12.0]
                last_err_sign = err_sign

                max_step = float(self.pid.output_limit) if self.pid.output_limit else 0.0
                # 基础有效步长：发生振荡时逐级衰减（>=3 次翻转就开始衰减）
                flips = len(sign_flip_window)
                osc_decay = 1.0
                if flips >= 3:
                    osc_decay = 0.5 ** (flips - 2)  # 3次->0.5，4次->0.25...
                    osc_decay = max(0.1, osc_decay)
                eff_max_step = max_step * osc_decay if max_step > 0 else 0.0

                # 5) 设定后至少等待 settle_time 才允许下一次调节，避免“读数延迟”导致的误判与振荡
                if (time.time() - last_set_time) < settle_time:
                    du = 0.0
                else:
                    if coarse_mode and eff_max_step > 0:
                        # 基于斜率估计：ΔV ≈ error / (dI/dV)
                        if slope_est is not None and slope_est > 0:
                            du = float(error) / float(slope_est)
                        else:
                            # 无斜率估计时用保守比例粗调（让 |e|=enter_th 对应约 eff_max_step）
                            k_coarse = eff_max_step / max(enter_th, 1e-9)
                            du = float(k_coarse) * float(error)

                        # 限幅到有效最大步长
                        if du > eff_max_step:
                            du = eff_max_step
                        elif du < -eff_max_step:
                            du = -eff_max_step
                    else:
                        # 微调：PID（deadband 内输出0）
                        du = self.pid.calculate(
                            target_current,
                            current_filt,
                            dt=sleep_period,
                            deadband=deadband
                        )

                # 方向保护（默认认为：u↑ -> 电流↑）
                # 当电流已超过目标（error<0）时，禁止继续增加u；反之亦然
                if error < 0 and du > 0:
                    du = 0.0
                if error > 0 and du < 0:
                    du = 0.0

                # 最小电压步进：一旦需要调节，|ΔV| 至少为 1V（受最大步长限幅）
                # 仅在 du != 0 时生效；deadband/数据失效/settle_time 等情况下 du 会被置 0
                if du != 0.0:
                    eff_min_step = 1.0
                    if isinstance(eff_max_step, (int, float)) and eff_max_step > 0:
                        eff_min_step = min(1.0, float(eff_max_step))
                    if abs(float(du)) < eff_min_step:
                        du = eff_min_step if float(du) > 0 else -eff_min_step
                # 记录本次调节前的工作点，用于下一次估计 dI/du
                last_u_for_slope = float(set_u)
                last_i_for_slope = float(current_filt) if current_filt is not None else float(current_value)

                # 更新有效控制量，并保持非负
                set_u = float(set_u) + float(du)
                if set_u < 0:
                    set_u = 0.0

                # 转回实际设定电压
                new_voltage = polarity * set_u

                success, message = self.keithley_controller.set_voltage(new_voltage)
                if success:
                    self._set_voltage = new_voltage
                    self.update_voltage_signal.emit(new_voltage)
                    last_set_time = time.time()
                    self.update_status_signal.emit(
                        f"I={current_value:.2f}uA, 目标={target_current}uA, ΔV={du:.2f}V, Vset={new_voltage:.1f}V"
                    )
                else:
                    self.update_status_signal.emit(f"设置电压失败: {message}")

                time.sleep(sleep_period)

            except Exception as e:
                self.update_status_signal.emit(f"稳流控制错误: {str(e)}")
                time.sleep(sleep_period)

        self.update_status_signal.emit("稳流控制结束")

    def get_current_value(self):
        """获取当前电流值"""
        try:
            if self.params['current_source'] == 'keithley':
                # 使用Keithley自身读取的电流
                return self.keithley_controller.read_current()
            else:
                # 使用万用表数据
                self.data_mutex.lock()
                try:
                    meter_type = self.params['current_source']
                    if meter_type in self.meter_data:
                        # 读取值/单位/时间戳（用于判断断连或停止更新）
                        value = self.meter_data[meter_type]['value']
                        unit = self.meter_data[meter_type]['unit']
                        ts = self.meter_data[meter_type].get('timestamp', 0.0)
                        valid = self.meter_data[meter_type].get('valid', False)

                        # 如果万用表长时间未更新/无效，则保持电压不变（不再调整）
                        timeout_s = float(self.params.get('meter_timeout_s', 3.0))
                        if (not valid) or (ts <= 0) or (time.time() - ts > timeout_s):
                            return None

                        # 根据单位转换
                        if unit == 'mA':
                            return value * 1000  # 转换为uA
                        elif unit == 'A':
                            return value * 1e6  # 转换为uA
                        else:
                            return value  # 假设已经是uA
                finally:
                    self.data_mutex.unlock()
        except Exception as e:
            logger.info(f"获取电流值错误: {str(e)}")
        return None

    def stop(self):
        """停止稳流控制（手动停止；停止后置零并关高压）"""
        # 先让循环退出
        self.running = False

        # best-effort：先把设定电压置零，再关高压
        try:
            if self.keithley_controller and self.keithley_controller.is_connected:
                try:
                    self.keithley_controller.set_voltage(0.0)  # VSET 0
                except Exception:
                    pass
                try:
                    self.keithley_controller.disable_high_voltage()  # HVOF
                except Exception:
                    pass
        except Exception as e:
            logger.info(f"停止稳流置零/关高压失败: {e}")

        # 通知UI归零显示
        try:
            self.update_voltage_signal.emit(0.0)
        except Exception:
            pass

class SerialThread(QThread):
    """万用表串口读取线程"""

    data_received = pyqtSignal(dict)
    log_message_signal = pyqtSignal(str)

    def __init__(self, port, meter_name):
        super().__init__()
        self.port = port
        self.meter_name = meter_name
        self.ser = None
        self._running = True
        self._mutex = QMutex()
        self._stop_event = threading.Event()

    @property
    def running(self):
        """线程运行状态（线程安全）"""
        with QMutexLocker(self._mutex):
            return self._running

    @running.setter
    def running(self, value):
        """设置线程运行状态（线程安全）"""
        with QMutexLocker(self._mutex):
            self._running = value

    def run(self):

        """主循环：读万用表数据；异常时自动断线重连（指数退避）。"""

        # 退避策略：1s -> 2s -> 4s ... 上限 10s

        backoff_s = 1.0

        max_backoff_s = 10.0

        rx_buf = bytearray()  # 串口接收缓冲，用于帧同步


        def _should_stop() -> bool:

            with QMutexLocker(self._mutex):

                return not self._running


        def _close_port():

            try:

                if self.ser:

                    try:

                        if getattr(self.ser, "is_open", False):

                            self.ser.close()

                    except Exception:

                        pass

            finally:

                self.ser = None


        def _sleep_with_stop(total_s: float):

            # 分片休眠，保证 stop 时能快速退出

            ms = int(max(0, total_s) * 1000)

            step = 200

            while ms > 0:

                if _should_stop():

                    return

                self.msleep(min(step, ms))

                ms -= step


        try:

            while not _should_stop():

                # 确保连接

                if not (self.ser and getattr(self.ser, "is_open", False)):

                    _close_port()

                    try:

                        if not self.port:

                            self.log_message_signal.emit(f"串口未设置 ({self.meter_name})，等待重连...")

                            _sleep_with_stop(1.0)

                            continue


                        # NOTE: 万用表串口通讯参数需与原工程一致（19200bps）。
                        # 若误设为 2400，会导致接收字节流乱码/解析失败，UI 将一直停在 0。
                        self.ser = serial.Serial(

                            port=self.port,

                            baudrate=19200,

                            bytesize=8,

                            parity='N',

                            stopbits=1,

                            timeout=0.5

                        )

                        # 连接成功，重置退避

                        backoff_s = 1.0

                        self.log_message_signal.emit(f"串口已连接 ({self.meter_name})")

                    except Exception as e:

                        self.log_message_signal.emit(

                            f"串口连接错误 ({self.meter_name}): {str(e)}；{backoff_s:.0f}s 后重试"

                        )

                        _sleep_with_stop(backoff_s)

                        backoff_s = min(max_backoff_s, backoff_s * 2)

                        continue


                # 已连接：读取（使用缓冲区做帧同步，避免粘包/半包导致一直解析失败）
                try:
                    if self.ser:
                        # 读取当前可用字节；如果没有可用字节，读 1 个字节（由 timeout 控制）
                        chunk = self.ser.read(self.ser.in_waiting or 1)
                        if chunk:
                            rx_buf.extend(chunk)

                        # 读取固定帧：多数万用表该协议每次输出 14 字节（末尾为终止符）。
                        data = self.ser.read(14)
                        if not data or len(data) != 14:
                            continue

                        parsed_data = self.parse_data(data)
                        if parsed_data:
                            parsed_data['meter_name'] = self.meter_name
                            self.data_received.emit(parsed_data)


                except Exception as e:
                    # 读失败：关闭并进入重连
                    if not _should_stop():
                        self.log_message_signal.emit(
                            f"串口读取错误 ({self.meter_name}): {str(e)}；准备重连"
                        )
                    _close_port()
                    _sleep_with_stop(min(2.0, backoff_s))
                    continue

                # 减少CPU占用
                self.msleep(50)



        except Exception as e:

            self.log_message_signal.emit(f"串口线程错误 ({self.meter_name}): {str(e)}")

        finally:

            _close_port()


    def _cleanup(self):
        """清理资源"""
        with QMutexLocker(self._mutex):
            if self.ser:
                try:
                    self.ser.close()
                except:
                    pass
                finally:
                    self.ser = None

    def stop(self):
        """停止线程（线程安全）"""
        self.running = False
        if not self.wait(2000):
            self.terminate()
            self.wait(500)

    def parse_data(self, data):
        try:
            if len(data) != 14 or data[-2:] not in (b'\x0d\x8a', b'\x0d\x0a'):
                return None

            byte1 = data[0]
            byte7 = data[6]
            unit = self.get_unit(byte1, byte7)
            value_str = self.parse_value(data[1:6], byte1, byte7)
            sign = -1 if data[7] == 0x34 else 1

            if value_str is None:
                return None

            value = float(value_str) * sign
            return {'value': value, 'unit': unit}

        except Exception as e:
            error_msg = f"数据解析错误: {str(e)}"
            self.log_message_signal.emit(error_msg)
            return None

    def get_unit(self, byte1, byte7):
        if byte7 == 0x3B:
            if byte1 in [0x34, 0xB4]:
                return "mV"
            else:
                return "V"
        unit_map = {
            0x3D: "uA",
            0xBF: "mA",
            0xB0: "A"
        }
        return unit_map.get(byte7, "UNKNOWN")

    def parse_value(self, data_bytes, byte1, byte7):
        def to_digit(b):
            return chr((b & 0x0F) + 0x30)

        digits = ''.join([to_digit(b) for b in data_bytes])

        if byte7 == 0x3B:
            if byte1 in [0x34, 0xB4]:
                return f"{digits[:3]}.{digits[3:5]}"
            elif byte1 in [0xB0, 0x30]:
                return f"{digits[:1]}.{digits[1:5]}"
            elif byte1 in [0x31, 0xB1]:
                return f"{digits[:2]}.{digits[2:5]}"
            elif byte1 in [0x32, 0xB2]:
                return f"{digits[:3]}.{digits[3:5]}"
            elif byte1 in [0x33, 0xB3]:
                return f"{digits[:4]}.{digits[4]}"

        elif byte7 == 0x3D:
            if byte1 in [0x30, 0xB0]:
                return f"{digits[:3]}.{digits[3:5]}"
            elif byte1 in [0x31, 0xB1]:
                return f"{digits[:4]}.{digits[4]}"

        elif byte7 == 0xBF:
            if byte1 in [0x30, 0xB0]:
                return f"{digits[:2]}.{digits[2:5]}"
            elif byte1 in [0x31, 0xB1]:
                return f"{digits[:3]}.{digits[3:5]}"

        elif byte7 == 0xB0:
            if byte1 in [0x30, 0xB0]:
                return f"{digits[:2]}.{digits[2:5]}"

        return None

class CM52Thread(QThread):
    """
    Leybold COMBIVAC CM 52 真空计 RS232 读取线程（主动查询 RPV）
    - 按说明书：发送 "RPV<channel><CR>"，返回 "b[,][TAB]x.xxxxE±xx"
    - channel: 1=TM1, 2=TM2, 3=IONIVAC
    """
    data_received = pyqtSignal(dict)
    log_message_signal = pyqtSignal(str)

    def __init__(self, port: str, channel: int = 3, baudrate: int = 19200, poll_ms: int = 300, parent=None):
        super().__init__(parent)
        self.port = port
        self.channel = int(channel)
        self.baudrate = int(baudrate)
        self.poll_ms = max(100, int(poll_ms))
        self.ser = None
        self._running = True
        self._mutex = QMutex()

    def stop(self):
        with QMutexLocker(self._mutex):
            self._running = False
        try:
            if self.ser and self.ser.is_open:
                self.ser.close()
        except Exception:
            pass

    def run(self):

        """主循环：主动查询 RPV；异常时自动断线重连（指数退避）。"""

        backoff_s = 1.0

        max_backoff_s = 10.0

        cmd = f"RPV{self.channel}\r".encode("ascii", errors="ignore")


        def _should_stop() -> bool:

            with QMutexLocker(self._mutex):

                return not self._running


        def _close_port():

            try:

                if self.ser:

                    try:

                        if getattr(self.ser, "is_open", False):

                            self.ser.close()

                    except Exception:

                        pass

            finally:

                self.ser = None


        def _sleep_with_stop(total_s: float):

            ms = int(max(0, total_s) * 1000)

            step = 200

            while ms > 0:

                if _should_stop():

                    return

                self.msleep(min(step, ms))

                ms -= step


        try:

            while not _should_stop():

                # 确保连接

                if not (self.ser and getattr(self.ser, "is_open", False)):

                    _close_port()

                    try:

                        if not self.port:

                            self.log_message_signal.emit(f"串口未设置 ({self.meter_name})，等待重连...")

                            _sleep_with_stop(1.0)

                            continue


                        self.ser = serial.Serial(

                            port=self.port,

                            baudrate=self.baudrate,

                            bytesize=8,

                            parity='N',

                            stopbits=1,

                            timeout=0.6,

                            write_timeout=0.6

                        )

                        backoff_s = 1.0

                        self.log_message_signal.emit("CM52 串口已连接")

                    except Exception as e:

                        self.log_message_signal.emit(

                            f"CM52 串口连接错误: {e}；{backoff_s:.0f}s 后重试"

                        )

                        _sleep_with_stop(backoff_s)

                        backoff_s = min(max_backoff_s, backoff_s * 2)

                        continue


                # 已连接：一次查询

                try:

                    try:

                        self.ser.reset_input_buffer()

                    except Exception:

                        pass


                    self.ser.write(cmd)

                    try:

                        self.ser.flush()

                    except Exception:

                        pass


                    raw = self.ser.read_until(b"\r")

                    if not raw:

                        self.msleep(self.poll_ms)

                        continue


                    # 如果后面紧跟 LF，读掉

                    try:

                        if self.ser.in_waiting:

                            nxt = self.ser.read(1)

                            if nxt != b"\n":

                                pass

                    except Exception:

                        pass


                    try:

                        s = raw.decode("ascii", errors="ignore").strip()

                    except Exception:

                        s = ""


                    val = self._parse_rpv(s)

                    if val is not None:

                        self.data_received.emit({

                            "meter_name": "vacuum",

                            "type": "vacuum",

                            "value": float(val),

                            "unit": "Pa",

                            "raw": s

                        })


                except Exception as e:

                    if not _should_stop():

                        self.log_message_signal.emit(f"CM52 读取错误: {e}；准备重连")

                    _close_port()

                    _sleep_with_stop(min(2.0, backoff_s))

                    continue


                self.msleep(self.poll_ms)


        finally:

            _close_port()

    @staticmethod
    @staticmethod

    def _parse_rpv(s: str):
        """解析 CM52 的 RPV 返回行，返回压力值（Pa）或 None。
        典型返回： 'b,	1.2345E-03' / 'b	1.2345E-03' / 'b 1.2345E-03'
        其中 b 为状态字节（此处忽略），第二列为科学计数法压力值。
        """
        if not s:
            return None
        tmp = s.replace(",", " ").replace("	", " ")
        parts = [p for p in tmp.split() if p]
        if len(parts) < 2:
            return None
        try:
            return float(parts[1])
        except Exception:
            return None

class CountdownManager:
    """倒计时管理器"""

    def __init__(self, update_callback):
        self.countdown = 0
        self.timer = QTimer()
        self.update_callback = update_callback
        self.timer.timeout.connect(self._update)

    def start(self, seconds):
        """开始倒计时"""
        self.countdown = seconds
        self.timer.start(1000)
        self._update()

    def stop(self):
        """停止倒计时"""
        self.timer.stop()
        self.countdown = 0

    def _update(self):
        """内部更新方法"""
        if self.countdown > 0:
            self.countdown -= 1
            if self.update_callback:
                self.update_callback(self.countdown)
        else:
            self.stop()


class DataSaver(QThread):
    """后台数据保存线程（CSV 版）

    设计目标：
    - 采集线程只负责把行数据 push 到队列，保存线程做批量落盘，减少 UI 卡顿与磁盘 IO 频次。
    - SQLite 作为权威原始日志（可选），CSV 作为高性能、可直接打开的导出格式（无 xlsx 开销）。

    约定：
    - 原始数据：<path>.csv（append）
    - 循环数据：<path>_cycle.csv（overwrite by finalize 或 append by append_cycle_row）
    - 统计数据：<path>_summary.csv（overwrite by finalize）
    """

    save_complete = pyqtSignal()
    convert_complete = pyqtSignal()

    def __init__(self):
        super().__init__()
        self.headers = DATA_HEADERS
        self.queue = Queue()

        self.csv_path: str | None = None
        self.running = True

        # 批量写入：避免每行都 flush
        self.batch_size = 100
        # 定时 flush：避免持续高频入队导致长期不落盘
        self.flush_interval_sec = 3600.0  # disabled; use batch_size flush
        self._last_flush_ts = time.time()
        self._next_retry_ts = 0.0
        self._retry_interval_sec = 2.0  # avoid tight loop when file is locked
        self._stop_requested = False
        self._pending_rows: list[list] = []

        self._lock = threading.RLock()

        # 状态回传（用于 UI 提示）
        self.last_convert_success: bool | None = None
        self.last_convert_message: str = ""

    # -------- public API (thread-safe) --------

    def set_output_path(self, csv_path: str):
        """设置原始数据 CSV 路径（仅记录；真正写入在保存线程内完成）。"""
        self.csv_path = str(csv_path) if csv_path else None

    def add_batch(self, rows: list):
        """追加一批行数据（每行应与 headers 同长度）。"""
        if not rows:
            return
        try:
            # 防御性拷贝：避免调用方传入的 list 在入队后被 clear()/复用。
            #（这是导致“CSV 只有表头”的常见根因之一）
            safe_rows = [list(r) for r in rows]
            self.queue.put(("add_batch", safe_rows))
        except Exception:
            pass

    def add_marker_row(self, text: str):
        """写入标记行（用于循环分隔）。"""
        if not text:
            return
        try:
            self.queue.put(("marker", text))
        except Exception:
            pass

    def append_cycle_row(self, cycle: int, min_anode, voltage, time_str: str):
        """追加一条循环统计行到 cycle.csv。"""
        try:
            self.queue.put(("cycle_row", {
                "cycle": cycle,
                "min_anode": min_anode,
                "voltage": voltage,
                "time": time_str,
            }))
        except Exception:
            pass

    def force_save(self):
        """强制 flush pending rows。"""
        try:
            self.queue.put(("flush", None))
        except Exception:
            pass

    def request_convert(self, csv_path: str, anode_min=None, cycle_data=None):
        """生成统计/循环 CSV（不做 xlsx 转换；保留原方法名以兼容旧调用）。"""
        try:
            self.queue.put(("finalize", {
                "csv_path": str(csv_path) if csv_path else None,
                "anode_min": anode_min,
                "cycle_data": cycle_data,
            }))
        except Exception:
            pass

    # -------- internal helpers --------

    def _ensure_parent_dir(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    def _needs_header(self, path: str) -> bool:
        if not os.path.exists(path):
            return True
        try:
            return os.path.getsize(path) == 0
        except Exception:
            return True

    def _open_writer(self, path: str):
        """以 append 模式打开，返回 (fh, writer)。"""
        # utf-8-sig: 兼容 Excel 直接打开中文列名不乱码
        fh = open(path, "a", newline="", encoding="utf-8-sig")
        writer = csv.writer(fh)
        return fh, writer

    def _write_header_if_needed(self, writer, path: str):
        if self._needs_header(path):
            writer.writerow(list(self.headers))

    def _append_rows_to_csv(self, path: str, rows: list[list]) -> bool:
        """尝试追加写入 rows 到 CSV。

        返回：
        - True: 写入成功（已落盘）
        - False: 写入失败（常见：文件被 Excel 打开导致 PermissionError）。失败时 *不应* 清空 rows。
        """
        if not path or not rows:
            return True
        self._ensure_parent_dir(path)
        try:
            fh, writer = self._open_writer(path)
            try:
                self._write_header_if_needed(writer, path)
                writer.writerows(rows)
            finally:
                try:
                    fh.close()
                except Exception:
                    pass
            return True
        except PermissionError:
            # Windows 下 CSV 被 Excel 打开时通常是独占锁；此时不能写入。
            return False
        except Exception:
            return False

    def _write_recovery_csv(self, base_csv_path: str, rows: list[list]) -> str | None:
        """当 base_csv_path 被锁定无法写入时，把剩余数据写入 recovery 文件，避免丢数。"""
        if not base_csv_path or not rows:
            return None
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        p = pathlib.Path(base_csv_path)
        out_path = str(p.with_suffix("")) + f"_recovery_{ts}.csv"
        try:
            self._ensure_parent_dir(out_path)
            with open(out_path, "w", newline="", encoding="utf-8-sig") as fh:
                w = csv.writer(fh)
                w.writerow(list(self.headers))
                w.writerows(rows)
            return out_path
        except Exception:
            return None

    def _write_cycle_csv(self, base_csv_path: str, cycle_data: list[dict]):
        if not base_csv_path or not cycle_data:
            return
        p = pathlib.Path(base_csv_path)
        out_path = str(p.with_suffix("")) + "_cycle.csv"
        self._ensure_parent_dir(out_path)
        with open(out_path, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh)
            w.writerow(["cycle", "min_anode", "voltage", "time"])
            for d in cycle_data:
                w.writerow([d.get("cycle"), d.get("min_anode"), d.get("voltage"), d.get("time")])

    def _append_cycle_row_csv(self, base_csv_path: str, row: dict):
        if not base_csv_path or not row:
            return
        p = pathlib.Path(base_csv_path)
        out_path = str(p.with_suffix("")) + "_cycle.csv"
        self._ensure_parent_dir(out_path)
        need_header = self._needs_header(out_path)
        with open(out_path, "a", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh)
            if need_header:
                w.writerow(["cycle", "min_anode", "voltage", "time"])
            w.writerow([row.get("cycle"), row.get("min_anode"), row.get("voltage"), row.get("time")])

    def _write_summary_csv(self, base_csv_path: str, anode_min: dict | None):
        if not base_csv_path:
            return
        p = pathlib.Path(base_csv_path)
        out_path = str(p.with_suffix("")) + "_summary.csv"
        self._ensure_parent_dir(out_path)
        with open(out_path, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh)
            w.writerow(["item", "value"])
            if anode_min:
                w.writerow(["min_anode", anode_min.get("min_anode")])
                w.writerow(["min_anode_voltage", anode_min.get("voltage")])
                w.writerow(["min_anode_time", anode_min.get("time")])

    # -------- thread loop --------

    def run(self):
        while True:
            try:
                cmd, payload = self.queue.get(timeout=0.2)
            except Exception:
                cmd, payload = None, None

            if cmd is None:
                continue

            if cmd == "stop":
                # stop 前务必落盘（避免只写了表头、数据未写入）
                with self._lock:
                    if self.csv_path and self._pending_rows:
                        ok = self._append_rows_to_csv(self.csv_path, self._pending_rows)
                        if ok:
                            self._pending_rows.clear()
                            self.save_complete.emit()
                        else:
                            # 文件可能被 Excel 锁定：写入 recovery 文件避免丢数
                            rec = self._write_recovery_csv(self.csv_path, self._pending_rows)
                            if rec:
                                self._pending_rows.clear()
                                self.last_convert_success = True
                                self.last_convert_message = f"CSV 被占用，剩余数据已写入: {rec}"
                                self.save_complete.emit()
                break

            if cmd == "add_batch":
                rows = payload or []
                if not rows:
                    continue
                now_ts = time.time()
                with self._lock:
                    self._pending_rows.extend(rows)
                    need_flush = (len(self._pending_rows) >= self.batch_size)
                    if need_flush and self.csv_path and self._pending_rows:
                        # If the CSV is locked (e.g., opened in Excel), avoid retrying too frequently.
                        if now_ts < self._next_retry_ts:
                            continue

                        ok = self._append_rows_to_csv(self.csv_path, self._pending_rows)
                        if ok:
                            self._pending_rows.clear()
                            self._last_flush_ts = now_ts
                            self._next_retry_ts = 0.0
                            self.save_complete.emit()
                        else:
                            # CSV 被占用时：保留 pending，不清空，等待下次重试
                            self._next_retry_ts = now_ts + self._retry_interval_sec
                continue

            elif cmd == "marker":
                text = str(payload)
                now_ts = time.time()
                with self._lock:
                    # marker row: first column comment, rest empty
                    row = [f"# {text}"] + [""] * (len(self.headers) - 1)
                    self._pending_rows.append(row)

                    # Do NOT write immediately; keep the same batching rule as data rows.
                    if self.csv_path and len(self._pending_rows) >= self.batch_size:
                        if now_ts < self._next_retry_ts:
                            continue
                        ok = self._append_rows_to_csv(self.csv_path, self._pending_rows)
                        if ok:
                            self._pending_rows.clear()
                            self._last_flush_ts = now_ts
                            self._next_retry_ts = 0.0
                            self.save_complete.emit()
                        else:
                            self._next_retry_ts = now_ts + self._retry_interval_sec
            elif cmd == "flush":
                with self._lock:
                    if self._pending_rows and self.csv_path:
                        ok = self._append_rows_to_csv(self.csv_path, self._pending_rows)
                        if ok:
                            self._pending_rows.clear()
                            self._last_flush_ts = time.time()
                            self._next_retry_ts = 0.0
                            self.save_complete.emit()
                        else:
                            # CSV 被占用：保留 pending，稍后再试
                            self._next_retry_ts = time.time() + self._retry_interval_sec
                continue

            elif cmd == "cycle_row":
                with self._lock:
                    if self.csv_path:
                        self._append_cycle_row_csv(self.csv_path, payload)

            elif cmd == "finalize":
                try:
                    csv_path = str((payload or {}).get("csv_path") or self.csv_path or "")
                    anode_min = (payload or {}).get("anode_min")
                    cycle_data = (payload or {}).get("cycle_data")

                    with self._lock:
                        # flush pending rows first
                        if self._pending_rows and csv_path:
                            ok = self._append_rows_to_csv(csv_path, self._pending_rows)
                            if ok:
                                self._pending_rows.clear()
                                self._last_flush_ts = time.time()
                                self._next_retry_ts = 0.0
                                self.save_complete.emit()
                            else:
                                rec = self._write_recovery_csv(csv_path, self._pending_rows)
                                if rec:
                                    self._pending_rows.clear()
                                    self.last_convert_success = True
                                    self.last_convert_message = f"CSV 被占用，剩余数据已写入: {rec}"
                                    self.save_complete.emit()

                        # overwrite summary/cycle
                        if csv_path:
                            self._write_summary_csv(csv_path, anode_min)
                            if cycle_data:
                                self._write_cycle_csv(csv_path, cycle_data)

                    self.last_convert_success = True
                    self.last_convert_message = "CSV 统计/循环数据已生成"
                except Exception as e:
                    self.last_convert_success = False
                    self.last_convert_message = str(e)

                self.convert_complete.emit()

        # shutdown flush
        try:
            with self._lock:
                if self._pending_rows and self.csv_path:
                    self._append_rows_to_csv(self.csv_path, self._pending_rows)
                    self._pending_rows.clear()
        except Exception:
            pass

    def stop(self):
        """请求停止线程：先 flush 再退出。"""
        self._stop_requested = True
        try:
            # 先触发一次 flush，再请求 stop，确保退出前落盘
            self.queue.put(("flush", None))
            self.queue.put(("stop", None))
        except Exception:
            pass
        try:
            self.wait(5000)
        except Exception:
            pass