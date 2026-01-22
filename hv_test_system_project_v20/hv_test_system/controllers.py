from __future__ import annotations

from .common import *
from .device_io.workers import VisaIOWorker, SerialIOWorker

class Keithley248Controller:
    """Keithley 248高压电源控制器（通过GPIB）"""

    def __init__(self):
        self.instrument = None  # legacy; kept for compatibility (not used after refactor)
        self.is_connected = False
        self.gpib_address = 14
        self.current_voltage = 0.0
        self.current_current = 0.0
        self._lock = threading.RLock()
        self.resource_name = ""
        self._worker: VisaIOWorker | None = None

    def connect_gpib(self, address):
        """连接GPIB设备"""
        with self._lock:
            # 先清理旧连接
            self.disconnect()

            # 尝试两种资源名格式
            candidates = [f"GPIB0::{address}::INSTR", f"GPIB::{address}::INSTR"]
            last_err = None
            for resource_name in candidates:
                try:
                    self._worker = VisaIOWorker(resource_name=resource_name, timeout_ms=5000)
                    self._worker.start()
                    # 测试连接
                    idn = self._worker.call(lambda inst: inst.query("*IDN?").strip(), timeout_s=4.0)
                    if idn:
                        self.is_connected = True
                        self.gpib_address = int(address)
                        self.resource_name = resource_name
                        return True, f"GPIB连接成功，地址: {address}, 设备: {idn.strip()}"
                    last_err = "无法获取设备ID"
                except ImportError:
                    return False, "未安装pyvisa库，请使用 'pip install pyvisa' 安装"
                except Exception as e:
                    last_err = str(e)
                    try:
                        if self._worker:
                            self._worker.stop()
                    except Exception:
                        pass
                    self._worker = None
                    continue

            return False, f"连接失败: {last_err}" if last_err else "连接失败"

    def disconnect(self):
        """断开连接"""
        with self._lock:
            # 停止worker
            if self._worker:
                try:
                    self._worker.stop()
                except Exception:
                    pass
                self._worker = None
            # legacy
            if self.instrument:
                try:
                    self.instrument.close()
                except Exception:
                    pass
                self.instrument = None
            self.is_connected = False
            self.resource_name = ""

    def send_command(self, command):
        """发送命令到设备"""
        with self._lock:
            if not self.is_connected or not self._worker:
                return None

            try:
                if command.endswith("?"):
                    # 查询命令
                    return self._worker.call(lambda inst: inst.query(command).strip(), timeout_s=3.0)
                else:
                    # 设置命令
                    self._worker.call(lambda inst: inst.write(command), timeout_s=3.0)
                    time.sleep(0.05)
                    return "OK"
            except Exception as e:
                logger.info(f"发送命令错误: {command}, 错误: {str(e)}")
                return None

    def read_voltage(self):
        """读取实际输出电压（VOUT?命令）"""
        with self._lock:
            response = self.send_command("VOUT?")
            if response:
                try:
                    voltage = float(response)
                    self.current_voltage = voltage
                    return voltage
                except:
                    return None
            return None

    def read_current(self):
        """读取实际输出电流（IOUT?命令）"""
        with self._lock:
            response = self.send_command("IOUT?")
            if response:
                try:
                    current = float(response)
                    self.current_current = current
                    return current * 1e6  # 转换为uA
                except:
                    return None
            return None

    def set_voltage(self, voltage):
        """设置输出电压（VSET命令）"""
        with self._lock:
            success = self.send_command(f"VSET {voltage}")
            if success is not None:
                self.current_voltage = voltage
                return True, f"电压设置为: {voltage}V"
            return False, "设置电压失败"

    def set_current_limit(self, current_ua):
        """设置电流限制（ILIM命令，单位转换为A）"""
        with self._lock:
            current_a = current_ua / 1e6
            success = self.send_command(f"ILIM {current_a}")
            if success is not None:
                return True, f"电流限制设置为: {current_ua}uA"
            return False, "设置电流限制失败"

    def set_current_trip(self, current_ua):
        """设置电流跳闸点（ITRP命令，单位转换为A）"""
        with self._lock:
            current_a = current_ua / 1e6
            success = self.send_command(f"ITRP {current_a}")
            if success is not None:
                return True, f"电流跳闸点设置为: {current_ua}uA"
            return False, "设置电流跳闸点失败"

    def set_voltage_limit(self, voltage):
        """设置电压限制（VLIM命令）"""
        with self._lock:
            success = self.send_command(f"VLIM {voltage}")
            if success is not None:
                return True, f"电压限制设置为: {voltage}V"
            return False, "设置电压限制失败"

    def enable_high_voltage(self):
        """开启高压输出（HVON命令）"""
        with self._lock:
            success = self.send_command("HVON")
            if success is not None:
                return True, "高压输出已开启"
            return False, "开启高压输出失败"

    def disable_high_voltage(self):
        """关闭高压输出（HVOF命令）"""
        with self._lock:
            success = self.send_command("HVOF")
            if success is not None:
                return True, "高压输出已关闭"
            return False, "关闭高压输出失败"

    def get_id(self):
        """获取设备ID（*IDN?命令）"""
        with self._lock:
            response = self.send_command("*IDN?")
            return response

class HAPS06Controller:
    """HAPS06 高压电源控制器（RS232 / Modbus-RTU）。

    关键点：
    - 所有串口 I/O 通过 SerialIOWorker 串行化，避免 UI 线程 + 轮询线程并发访问串口。
    - 对写操作（0x05 / 0x10）必须读取 8 字节确认帧，否则回包残留会污染后续读取，导致 CRC 错误/无响应。
    - 长测自愈：连续失败达到阈值后自动重连，并尽量恢复远控与测试电压（仅在测试中恢复）。
    """

    def __init__(self):
        # legacy; kept for compatibility with old code paths
        self.serial_port = None

        self._worker: SerialIOWorker | None = None
        self._lock = threading.RLock()

        # 状态
        self.is_testing = False
        self.is_cycle_testing = False
        self.is_remote_control = False

        self.current_voltage = 0.0   # 设定电压（逻辑上）
        self.actual_voltage = 0.0    # 实际电压（读回）
        self.set_voltage = 0.0       # 设定电压（读回）
        self.voltage_update_callback = None

        # Modbus 从站地址(1~64)，可自动探测
        self.slave_addr = 1

        # 长测稳定性
        self._consecutive_failures = 0
        self._last_port: str | None = None
        self._last_baudrate: int | None = None
        self._open_kwargs: dict = {}

    @property
    def is_connected(self) -> bool:
        return self._worker is not None

    def _io(self, fn, timeout_s: float = 2.0):
        if not self._worker:
            raise RuntimeError("串口未连接")
        return self._worker.call(fn, timeout_s=float(timeout_s))

    # ---------------------------
    # 连接/断开
    # ---------------------------
    def connect_serial(self, port, baudrate: int = 9600):
        """连接串口（启动 SerialIOWorker）。"""
        with self._lock:
            self.disconnect()

            try:
                self._last_port = str(port)
                self._last_baudrate = int(baudrate)
                self._open_kwargs = {
                    "timeout": 0.2,         # 配合 read_exact 轮询
                    "write_timeout": 3.0,   # 长测更稳
                    "rtscts": False,
                    "dsrdtr": False,
                    "xonxoff": False,
                    "open_settle_s": 0.25,
                }

                self._worker = SerialIOWorker(
                    port=self._last_port,
                    baudrate=self._last_baudrate,
                    open_kwargs=self._open_kwargs,
                )
                self._worker.start()

                # 等待 worker 打开串口（不宜太久）
                time.sleep(0.30)

                # 探测从站地址（防止拨码/地址不一致导致无响应）
                try:
                    self.slave_addr = self._probe_address(max_total_s=1.2)
                    logger.info(f"HAPS06 Modbus 地址探测结果: {self.slave_addr}")
                except Exception as e:
                    logger.info(f"HAPS06 地址探测失败: {e}")

                return True, f"连接成功 ({self._last_port})"
            except Exception as e:
                self._worker = None
                return False, f"连接错误: {e}"

    def disconnect(self):
        """断开连接（best-effort）。"""
        with self._lock:
            # best-effort：退出远控（不要卡住断开）
            try:
                if self._worker and self.is_remote_control:
                    try:
                        self.exit_remote_control()
                    except Exception:
                        pass
            except Exception:
                pass

            try:
                if self._worker:
                    self._worker.stop()
            except Exception:
                pass
            self._worker = None

            # legacy
            try:
                if self.serial_port and getattr(self.serial_port, "is_open", False):
                    self.serial_port.close()
            except Exception:
                pass
            self.serial_port = None

            self.is_remote_control = False
            self._consecutive_failures = 0

    # ---------------------------
    # Modbus helpers
    # ---------------------------
    def calculate_crc(self, data: bytes) -> bytes:
        crc = 0xFFFF
        for byte in data:
            crc ^= byte
            for _ in range(8):
                if crc & 0x0001:
                    crc >>= 1
                    crc ^= 0xA001
                else:
                    crc >>= 1
        return crc.to_bytes(2, byteorder="little")

    def float_to_bytes(self, value: float) -> bytes:
        return struct.pack(">f", float(value))

    def bytes_to_float(self, byte_array: bytes) -> float:
        return struct.unpack(">f", byte_array)[0]

    def _read_exact(self, ser, n: int, total_timeout: float = 1.2) -> bytes:
        data = bytearray()
        t0 = time.monotonic()
        while len(data) < n and (time.monotonic() - t0) < total_timeout:
            chunk = ser.read(n - len(data))
            if chunk:
                data.extend(chunk)
            else:
                time.sleep(0.01)
        return bytes(data)

    def _exchange(self, cmd: bytes, resp_len: int, timeout_s: float = 2.0) -> bytes:
        """发送 Modbus-RTU 帧并读取固定长度响应（校验 CRC）。"""
        if not self._worker:
            raise RuntimeError("串口未连接")

        def _do(ser):
            # 帧间隔（9600bps 时约 4ms），这里取 6ms 保险
            time.sleep(0.006)
            try:
                ser.reset_input_buffer()
            except Exception:
                pass
            try:
                ser.reset_output_buffer()
            except Exception:
                pass

            ser.write(cmd)
            try:
                ser.flush()
            except Exception:
                pass

            return self._read_exact(ser, resp_len, total_timeout=max(1.0, float(timeout_s)))

        # 重要：worker.call 的 timeout 要比串口读写总时长略大，避免误报“串口I/O超时”
        resp = self._io(_do, timeout_s=float(timeout_s) + 2.0)

        if len(resp) != resp_len:
            raise TimeoutError(f"无响应或响应长度不足(期望{resp_len}字节, 实际{len(resp)}字节)")
        received_crc = resp[-2:]
        calculated_crc = self.calculate_crc(resp[:-2])
        if received_crc != calculated_crc:
            raise ValueError("CRC错误")
        return resp

    def _probe_address(self, max_total_s: float = 1.5) -> int:
        """尝试探测从站地址（优先当前；失败则有限时扫描 1~64）。

        说明：
        - 某些电脑/USB-232 适配器在“第一次打开串口”后，设备响应可能会延迟。
        - 旧实现可能在无响应时扫描 1~64，导致连接阶段卡顿较长时间。
        - 这里加了总耗时上限，优先保证 UI 连接阶段不卡死。
        """
        def build(addr: int) -> bytes:
            c = bytes([addr, 0x03, 0x0B, 0x04, 0x00, 0x01])  # MODEL=0x0B04, u16
            return c + self.calculate_crc(c)

        t0 = time.time()
        candidates = [self.slave_addr] + [i for i in range(1, 65) if i != self.slave_addr]
        for a in candidates:
            if time.time() - t0 > float(max_total_s):
                break
            try:
                # 连接阶段尽量用较短超时，避免“第一次连接”卡住
                resp = self._exchange(build(a), resp_len=7, timeout_s=0.25)
                if resp[0] == a and resp[1] == 0x03 and resp[2] == 0x02:
                    return a
            except Exception:
                continue
        return self.slave_addr

    def _reconnect(self, reason: str = "") -> bool:
        """通讯异常后自动重连（长测自愈）。"""
        with self._lock:
            port = self._last_port
            baud = self._last_baudrate
            okw = dict(self._open_kwargs) if isinstance(self._open_kwargs, dict) else {}
            if not port or not baud:
                return False

            logger.info(f"HAPS06 触发自动重连: {reason} (port={port}, baud={baud})")

            # 记录当前状态，重连后尽量恢复
            want_remote = bool(self.is_remote_control)
            want_voltage = float(self.current_voltage or 0.0)
            testing = bool(self.is_testing or self.is_cycle_testing)

            # 停止旧 worker（不做 exit_remote_control，避免设备无响应时卡住）
            try:
                if self._worker:
                    try:
                        self._worker.stop()
                    except Exception:
                        pass
            except Exception:
                pass
            self._worker = None

            # 创建新 worker
            try:
                self._worker = SerialIOWorker(
                    port=str(port),
                    baudrate=int(baud),
                    open_kwargs=okw or {"timeout": 0.2, "write_timeout": 3.0},
                )
                self._worker.start()
                time.sleep(0.2)
            except Exception as e:
                logger.info(f"HAPS06 自动重连失败(打开串口): {e}")
                self._worker = None
                return False

            # 重新探测地址（可选）
            try:
                self.slave_addr = self._probe_address(max_total_s=1.2)
            except Exception:
                pass

            # 尝试恢复远控
            if want_remote:
                ok, msg = self.enable_remote_control()
                if not ok:
                    logger.info(f"HAPS06 重连后启用远控失败: {msg}")
                else:
                    logger.info("HAPS06 重连后远控已启用")

            # 仅在测试中恢复设定电压
            if testing and want_voltage > 0.0:
                ok, msg = self.set_voltage_only(want_voltage)
                if not ok:
                    logger.info(f"HAPS06 重连后恢复电压失败: {msg}")
                else:
                    logger.info(f"HAPS06 重连后已恢复电压: {want_voltage}V")

            return True

    # ---------------------------
    # 业务指令
    # ---------------------------
    def enable_remote_control(self, timeout_s: float = 2.0):
        """启用远程控制模式（写线圈 PC=1，地址 0x0500）。"""
        with self._lock:
            if not self._worker:
                return False, "串口未连接"
            try:
                cmd = bytes([self.slave_addr, 0x05, 0x05, 0x00, 0xFF, 0x00])
                cmd += self.calculate_crc(cmd)
                _ = self._exchange(cmd, resp_len=8, timeout_s=float(timeout_s))
                time.sleep(0.05)
                self.is_remote_control = True
                return True, "远程控制已启用"
            except Exception as e:
                return False, f"启用远程控制错误: {e}"

    def exit_remote_control(self, timeout_s: float = 2.0):
        """退出远程控制模式（写线圈 PC=0，地址 0x0500）。"""
        with self._lock:
            if not self._worker:
                return False, "串口未连接"
            try:
                cmd = bytes([self.slave_addr, 0x05, 0x05, 0x00, 0x00, 0x00])
                cmd += self.calculate_crc(cmd)
                _ = self._exchange(cmd, resp_len=8, timeout_s=float(timeout_s))
                time.sleep(0.05)
                self.is_remote_control = False
                return True, "远程控制已退出"
            except Exception as e:
                return False, f"退出远程控制错误: {e}"

    def read_set_voltage(self):
        """读取设置电压值（VSET 寄存器 0x0A05，float, 2 regs）。"""
        with self._lock:
            if not self._worker:
                return None
            try:
                cmd = bytes([self.slave_addr, 0x03, 0x0A, 0x05, 0x00, 0x02])
                cmd += self.calculate_crc(cmd)
                resp = self._exchange(cmd, resp_len=9, timeout_s=2.0)
                return self.bytes_to_float(resp[3:7])
            except Exception as e:
                logger.info(f"读取设置电压失败: {e}")
                return None

    def set_voltage_only(self, voltage: float):
        """仅设置电压值，不改变远控状态（VSET + CMD=1）。"""
        with self._lock:
            if not self._worker:
                return False, "串口未连接"

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    vbytes = self.float_to_bytes(float(voltage))
                    # 写 VSET(0x0A05, 2 regs, float)
                    cmd1 = bytes([self.slave_addr, 0x10, 0x0A, 0x05, 0x00, 0x02, 0x04]) + vbytes
                    cmd1 += self.calculate_crc(cmd1)
                    _ = self._exchange(cmd1, resp_len=8, timeout_s=2.0)

                    # 写 CMD(0x0A00, 1 reg) = 1
                    cmd2 = bytes([self.slave_addr, 0x10, 0x0A, 0x00, 0x00, 0x01, 0x02, 0x00, 0x01])
                    cmd2 += self.calculate_crc(cmd2)
                    _ = self._exchange(cmd2, resp_len=8, timeout_s=2.0)

                    self.current_voltage = float(voltage)

                    time.sleep(0.1)
                    set_v = self.read_set_voltage()
                    if set_v is not None and abs(set_v - float(voltage)) <= 1.0:
                        return True, f"电压已设置为: {float(voltage)}V (设置值: {set_v:.1f}V)"

                    if attempt < max_retries - 1:
                        time.sleep(0.2)
                        continue

                    return True, f"电压已设置为: {float(voltage)}V (设置值: {set_v if set_v is not None else '未知'}V)"
                except Exception as e:
                    if attempt < max_retries - 1:
                        time.sleep(0.2)
                        continue
                    return False, f"设置电压错误: {e}"

    def read_actual_voltage(self):
        """读取实际电压值（VS 寄存器 0x0B00，float, 2 regs）。

        长测优化：
        - 失败时短重试 1 次
        - 连续失败达到阈值后自动重连
        """
        with self._lock:
            if not self._worker:
                return None

            cmd = bytes([self.slave_addr, 0x03, 0x0B, 0x00, 0x00, 0x02])
            cmd += self.calculate_crc(cmd)

            last_err = None
            for attempt in range(2):
                try:
                    resp = self._exchange(cmd, resp_len=9, timeout_s=2.0)
                    voltage = self.bytes_to_float(resp[3:7])
                    self.actual_voltage = float(voltage)
                    self._consecutive_failures = 0
                    if self.voltage_update_callback:
                        try:
                            self.voltage_update_callback(float(voltage))
                        except Exception:
                            pass
                    return float(voltage)
                except Exception as e:
                    last_err = e
                    self._consecutive_failures += 1
                    if attempt == 0:
                        time.sleep(0.05)

            logger.info(f"读取实际电压失败: {last_err}")

            if self._consecutive_failures >= 5:
                self._consecutive_failures = 0
                try:
                    self._reconnect(reason=str(last_err))
                except Exception as e:
                    logger.info(f"HAPS06 自动重连异常: {e}")

            return None

    # 兼容旧 UI 调用
    def stop_output(self):
        return self.set_voltage_only(0.0)

    def reset_voltage(self):
        return self.set_voltage_only(100.0)

    def manual_set_voltage(self, voltage):
        return self.set_voltage_only(float(voltage))
