from __future__ import annotations

import threading
import time

import serial
from PyQt5.QtCore import QThread, pyqtSignal

class BaseSerialReconnectThread(QThread):
    """Shared stop/close/backoff helpers for serial polling threads."""

    BACKOFF_START_S = 1.0
    BACKOFF_MAX_S = 10.0
    STOP_SLEEP_STEP_MS = 200

    def __init__(self, parent=None):
        super().__init__(parent)
        self.ser = None
        self._stop_event = threading.Event()

    @property
    def running(self):
        return not self._stop_event.is_set()

    @running.setter
    def running(self, value):
        if value:
            self._stop_event.clear()
        else:
            self._stop_event.set()

    def _should_stop(self) -> bool:
        return self._stop_event.is_set()

    def _close_port(self):
        try:
            if self.ser and getattr(self.ser, "is_open", False):
                try:
                    if hasattr(self.ser, "cancel_read"):
                        self.ser.cancel_read()
                except Exception:
                    pass
                self.ser.close()
        except Exception:
            pass
        finally:
            self.ser = None

    def _sleep_with_stop(self, total_s: float):
        ms = int(max(0.0, total_s) * 1000)
        while ms > 0 and not self._should_stop():
            step = min(self.STOP_SLEEP_STEP_MS, ms)
            self.msleep(step)
            ms -= step

    def _next_backoff(self, current_s: float) -> float:
        base = float(current_s or self.BACKOFF_START_S)
        return min(self.BACKOFF_MAX_S, base * 2.0)

    def stop(self):
        self._stop_event.set()
        self._close_port()


class SerialThread(BaseSerialReconnectThread):
    """万用表串口读取线程"""

    FRAME_LENGTH = 14
    FRAME_TERMINATORS = (b"\x0d\x8a", b"\x0d\x0a")

    data_received = pyqtSignal(dict)
    log_message_signal = pyqtSignal(str)

    def __init__(self, port, meter_name):
        super().__init__()
        self.port = port
        self.meter_name = meter_name

    def run(self):

        """主循环：读万用表数据；异常时自动断线重连（指数退避）。"""

        # 退避策略：1s -> 2s -> 4s ... 上限 10s

        backoff_s = self.BACKOFF_START_S

        rx_buf = bytearray()  # 串口接收缓冲，用于帧同步


        try:

            while not self._should_stop():

                # 确保连接

                if not (self.ser and getattr(self.ser, "is_open", False)):

                    self._close_port()

                    try:

                        if not self.port:

                            self.log_message_signal.emit("CM52 串口未设置，等待重连...")

                            self._sleep_with_stop(1.0)

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

                        backoff_s = self.BACKOFF_START_S

                        self.log_message_signal.emit(f"串口已连接 ({self.meter_name})")

                    except Exception as e:

                        self.log_message_signal.emit(

                            f"串口连接错误 ({self.meter_name}): {str(e)}；{backoff_s:.0f}s 后重试"

                        )

                        self._sleep_with_stop(backoff_s)

                        backoff_s = self._next_backoff(backoff_s)

                        continue


                # 已连接：读取（使用缓冲区做帧同步，避免粘包/半包导致一直解析失败）
                try:
                    if self.ser:
                        # 读取当前可用字节；如果没有可用字节，读 1 个字节（由 timeout 控制）
                        waiting = 0
                        try:
                            waiting = int(getattr(self.ser, "in_waiting", 0) or 0)
                        except Exception:
                            waiting = 0
                        chunk = self.ser.read(waiting or 1)
                        if chunk:
                            rx_buf.extend(chunk)
                        frame = self._extract_frame(rx_buf)
                        if frame is None:
                            continue

                        parsed_data = self.parse_data(frame)
                        if parsed_data:
                            parsed_data['meter_name'] = self.meter_name
                            self.data_received.emit(parsed_data)


                except Exception as e:
                    # 读失败：关闭并进入重连
                    if not self._should_stop():
                        self.log_message_signal.emit(
                            f"串口读取错误 ({self.meter_name}): {str(e)}；准备重连"
                        )
                    self._close_port()
                    self._sleep_with_stop(min(2.0, backoff_s))
                    continue

                # 减少CPU占用
                self.msleep(50)



        except Exception as e:

            self.log_message_signal.emit(f"串口线程错误 ({self.meter_name}): {str(e)}")

        finally:

            self._close_port()


    def _cleanup(self):
        """清理资源"""
        self._close_port()

    def stop(self):
        """停止线程（线程安全）"""
        self.running = False
        super().stop()
        if not self.wait(2000):
            self.terminate()
            self.wait(500)

    def parse_data(self, data):
        try:
            if len(data) != self.FRAME_LENGTH or data[-2:] not in self.FRAME_TERMINATORS:
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

    @classmethod
    def _extract_frame(cls, rx_buf: bytearray):
        if rx_buf is None:
            return None

        min_frame_len = cls.FRAME_LENGTH
        if len(rx_buf) < min_frame_len:
            return None

        for end_idx in range(min_frame_len, len(rx_buf) + 1):
            tail = bytes(rx_buf[end_idx - 2:end_idx])
            if tail not in cls.FRAME_TERMINATORS:
                continue

            start_idx = end_idx - min_frame_len
            if start_idx < 0:
                continue

            frame = bytes(rx_buf[start_idx:end_idx])
            del rx_buf[:end_idx]
            return frame

        if len(rx_buf) > (min_frame_len * 4):
            del rx_buf[:- (min_frame_len - 1)]
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


def _modbus_crc16(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF


class RebornRTUVacuumThread(BaseSerialReconnectThread):
    """睿宝真空计 Modbus RTU 读取线程。"""

    data_received = pyqtSignal(dict)
    log_message_signal = pyqtSignal(str)

    def __init__(self, port: str, slave_address: int = 1, baudrate: int = 9600, poll_ms: int = 400, parent=None):
        super().__init__(parent)
        self.port = port
        self.slave_address = max(1, int(slave_address)) & 0xFF
        self.baudrate = int(baudrate)
        self.poll_ms = max(150, int(poll_ms))

    def _build_request(self) -> bytes:
        frame = bytes([self.slave_address, 0x03, 0x00, 0x6B, 0x00, 0x02])
        crc = _modbus_crc16(frame)
        return frame + bytes([crc & 0xFF, (crc >> 8) & 0xFF])

    def _parse_response(self, raw: bytes):
        if len(raw) < 9:
            return None
        frame = raw[:9]
        body, crc_bytes = frame[:-2], frame[-2:]
        crc_calc = _modbus_crc16(body)
        crc_recv = crc_bytes[0] | (crc_bytes[1] << 8)
        if crc_calc != crc_recv:
            raise ValueError('CRC 校验失败')
        if body[0] != self.slave_address or body[1] != 0x03 or body[2] != 0x04:
            raise ValueError('RTU 响应头不正确')
        chars = ''.join(chr(x) for x in body[3:7])
        if len(chars) != 4:
            return None
        try:
            value = float(f"{chars[0]}.{chars[1]}E{chars[2]}{chars[3]}")
        except Exception as exc:
            raise ValueError(f'无法解析真空值: {chars}') from exc
        return value, chars

    def run(self):
        backoff_s = self.BACKOFF_START_S
        req = self._build_request()

        try:
            while not self._should_stop():
                if not (self.ser and getattr(self.ser, 'is_open', False)):
                    self._close_port()
                    try:
                        if not self.port:
                            self.log_message_signal.emit('睿宝真空计串口未设置，等待重连...')
                            self._sleep_with_stop(1.0)
                            continue
                        self.ser = serial.Serial(
                            port=self.port,
                            baudrate=self.baudrate,
                            bytesize=8,
                            parity='N',
                            stopbits=1,
                            timeout=0.6,
                            write_timeout=0.6,
                        )
                        backoff_s = self.BACKOFF_START_S
                        self.log_message_signal.emit(f'睿宝真空计已连接: {self.port} (Addr {self.slave_address})')
                    except Exception as e:
                        self.log_message_signal.emit(f'睿宝真空计连接错误: {e}；{backoff_s:.0f}s 后重试')
                        self._sleep_with_stop(backoff_s)
                        backoff_s = self._next_backoff(backoff_s)
                        continue

                try:
                    try:
                        self.ser.reset_input_buffer()
                    except Exception:
                        pass
                    self.ser.write(req)
                    try:
                        self.ser.flush()
                    except Exception:
                        pass
                    raw = self.ser.read(9)
                    if len(raw) < 9:
                        self.msleep(self.poll_ms)
                        continue
                    parsed = self._parse_response(raw)
                    if parsed is not None:
                        value, chars = parsed
                        self.data_received.emit({
                            'meter_name': 'vacuum',
                            'type': 'vacuum',
                            'value': float(value),
                            'unit': 'Pa',
                            'raw': chars,
                        })
                except Exception as e:
                    if not self._should_stop():
                        self.log_message_signal.emit(f'睿宝真空计读取错误: {e}；准备重连')
                    self._close_port()
                    self._sleep_with_stop(min(2.0, backoff_s))
                    continue

                self.msleep(self.poll_ms)
        finally:
            self._close_port()


class AgilentXGS600Thread(BaseSerialReconnectThread):
    """Agilent / Varian XGS-600 RS232 压力读取线程。"""

    data_received = pyqtSignal(dict)
    log_message_signal = pyqtSignal(str)

    def __init__(self, port: str, sensor_index: int = 1, baudrate: int = 9600, unit: str = 'Pa', poll_ms: int = 400, parent=None):
        super().__init__(parent)
        self.port = port
        self.sensor_index = max(1, int(sensor_index))
        self.baudrate = int(baudrate)
        self.unit = str(unit or 'Pa')
        self.poll_ms = max(180, int(poll_ms))
        self._last_index_warn_ts = 0.0
        self._last_recover_log_ts = 0.0
        self._bad_read_count = 0
        self._last_good_payload = ''

    def _drain_input(self, max_ms: int = 120):
        if not (self.ser and getattr(self.ser, 'is_open', False)):
            return b''
        drained = bytearray()
        deadline = time.monotonic() + max(0.02, max_ms / 1000.0)
        while time.monotonic() < deadline and not self._should_stop():
            try:
                waiting = int(getattr(self.ser, 'in_waiting', 0) or 0)
            except Exception:
                waiting = 0
            if waiting <= 0:
                break
            chunk = self.ser.read(waiting)
            if not chunk:
                break
            drained.extend(chunk)
            if len(drained) > 4096:
                break
        return bytes(drained)

    @staticmethod
    def _normalize_line(text: str) -> str:
        line = (text or '').strip().strip('\x00')
        if not line:
            return ''
        if line.startswith('>'):
            line = line[1:].strip()
        if line.startswith('#'):
            return ''
        return line

    @classmethod
    def _parse_pressure_dump(cls, raw: bytes):
        text = raw.decode('ascii', errors='ignore').replace('\x00', '').strip()
        if not text:
            return [], text
        lines = []
        for piece in text.replace('\n', '\r').split('\r'):
            line = cls._normalize_line(piece)
            if not line:
                continue
            if line.startswith('?'):
                raise ValueError(f'控制器返回错误: {line}')
            lines.append(line)
        if not lines:
            return [], text

        payload = ''
        values = []
        for line in reversed(lines):
            tokens = [x.strip() for x in line.split(',') if x.strip()]
            if not tokens:
                continue
            parsed = []
            numeric_hits = 0
            for token in tokens:
                upper = token.upper()
                if upper in {'OFF', 'NOCBL', 'OPEN', 'NO_GAUGE', 'ERROR'}:
                    parsed.append(None)
                    continue
                try:
                    parsed.append(float(token))
                    numeric_hits += 1
                except Exception:
                    parsed.append(None)
            if parsed and (numeric_hits > 0 or len(parsed) > 1):
                values = parsed
                payload = line
                break

        if not payload:
            payload = lines[-1]
        return values, payload

    def _read_response(self, timeout_s: float = 1.0) -> bytes:
        if not (self.ser and getattr(self.ser, 'is_open', False)):
            return b''
        buf = bytearray()
        deadline = time.monotonic() + max(0.25, timeout_s)
        last_rx = 0.0
        while time.monotonic() < deadline and not self._should_stop():
            try:
                waiting = int(getattr(self.ser, 'in_waiting', 0) or 0)
            except Exception:
                waiting = 0
            read_size = waiting if waiting > 0 else 1
            chunk = self.ser.read(read_size)
            if chunk:
                buf.extend(chunk)
                last_rx = time.monotonic()
                if len(buf) >= 4096:
                    break
                if b'\r' in chunk or b'\n' in chunk:
                    try:
                        values, _ = self._parse_pressure_dump(bytes(buf))
                    except Exception:
                        return bytes(buf)
                    if values:
                        break
            elif buf and last_rx and (time.monotonic() - last_rx) > 0.15:
                break
        return bytes(buf)

    def _soft_recover_port(self, reason: str = ''):
        now = time.time()
        if reason and (now - self._last_recover_log_ts) > 5.0:
            self._last_recover_log_ts = now
            self.log_message_signal.emit(f'Agilent XGS-600 通讯异常，正在自动恢复: {reason}')
        self._close_port()
        if self._should_stop():
            return
        self.msleep(250)

    def run(self):
        backoff_s = self.BACKOFF_START_S
        cmd = b'#000F\r'

        try:
            while not self._should_stop():
                if not (self.ser and getattr(self.ser, 'is_open', False)):
                    self._close_port()
                    try:
                        if not self.port:
                            self.log_message_signal.emit('Agilent 真空计串口未设置，等待重连...')
                            self._sleep_with_stop(1.0)
                            continue
                        self.ser = serial.Serial(
                            port=self.port,
                            baudrate=self.baudrate,
                            bytesize=8,
                            parity='N',
                            stopbits=1,
                            timeout=0.25,
                            write_timeout=0.6,
                            inter_byte_timeout=0.15,
                            xonxoff=False,
                            rtscts=False,
                            dsrdtr=False,
                        )
                        try:
                            self.ser.reset_input_buffer()
                            self.ser.reset_output_buffer()
                        except Exception:
                            pass
                        try:
                            self.ser.setRTS(False)
                            self.ser.setDTR(False)
                            self.msleep(60)
                            self.ser.setRTS(True)
                            self.ser.setDTR(True)
                            self.msleep(80)
                        except Exception:
                            pass
                        backoff_s = self.BACKOFF_START_S
                        self._bad_read_count = 0
                        self.log_message_signal.emit(f'Agilent XGS-600 已连接: {self.port}')
                    except Exception as e:
                        self.log_message_signal.emit(f'Agilent XGS-600 连接错误: {e}；{backoff_s:.0f}s 后重试')
                        self._sleep_with_stop(backoff_s)
                        backoff_s = self._next_backoff(backoff_s)
                        continue

                try:
                    self._drain_input(80)
                    self.ser.write(cmd)
                    try:
                        self.ser.flush()
                    except Exception:
                        pass
                    raw = self._read_response(1.0)
                    if not raw:
                        self._bad_read_count += 1
                        if self._bad_read_count >= 3:
                            self._soft_recover_port('连续读取超时')
                        self.msleep(self.poll_ms)
                        continue

                    values, payload = self._parse_pressure_dump(raw)
                    idx = self.sensor_index - 1
                    if idx >= len(values):
                        self._bad_read_count += 1
                        now = time.time()
                        if (now - self._last_index_warn_ts) > 5.0:
                            self._last_index_warn_ts = now
                            self.log_message_signal.emit(
                                f'Agilent XGS-600 当前返回 {len(values)} 路压力，所选序号 {self.sensor_index} 超出范围；原始帧: {payload or raw.decode("ascii", errors="ignore").strip()}'
                            )
                        if self._bad_read_count >= 3:
                            self._soft_recover_port('返回帧异常或被截断')
                        self.msleep(self.poll_ms)
                        continue

                    value = values[idx]
                    self._bad_read_count = 0
                    self._last_good_payload = payload
                    if value is not None:
                        self.data_received.emit({
                            'meter_name': 'vacuum',
                            'type': 'vacuum',
                            'value': float(value),
                            'unit': self.unit,
                            'raw': payload,
                        })
                except Exception as e:
                    if not self._should_stop():
                        self.log_message_signal.emit(f'Agilent XGS-600 读取错误: {e}；准备重连')
                    self._soft_recover_port(str(e))
                    self._sleep_with_stop(min(2.0, backoff_s))
                    continue

                self.msleep(self.poll_ms)
        finally:
            self._close_port()

class CM52Thread(BaseSerialReconnectThread):
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

    def run(self):

        """主循环：主动查询 RPV；异常时自动断线重连（指数退避）。"""

        backoff_s = self.BACKOFF_START_S

        cmd = f"RPV{self.channel}\r".encode("ascii", errors="ignore")

        try:

            while not self._should_stop():

                # 确保连接

                if not (self.ser and getattr(self.ser, "is_open", False)):

                    self._close_port()

                    try:

                        if not self.port:

                            self.log_message_signal.emit("CM52 串口未设置，等待重连...")

                            self._sleep_with_stop(1.0)

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

                        backoff_s = self.BACKOFF_START_S

                        self.log_message_signal.emit("CM52 串口已连接")

                    except Exception as e:

                        self.log_message_signal.emit(

                            f"CM52 串口连接错误: {e}；{backoff_s:.0f}s 后重试"

                        )

                        self._sleep_with_stop(backoff_s)

                        backoff_s = self._next_backoff(backoff_s)

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

                    if not self._should_stop():

                        self.log_message_signal.emit(f"CM52 读取错误: {e}；准备重连")

                    self._close_port()

                    self._sleep_with_stop(min(2.0, backoff_s))

                    continue


                self.msleep(self.poll_ms)


        finally:

            self._close_port()

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

