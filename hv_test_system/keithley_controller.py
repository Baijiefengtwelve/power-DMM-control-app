from __future__ import annotations

from .common import *
from .device_io.workers import VisaIOWorker

class Keithley248Controller:
    """Keithley 类高压电源控制器（Keithley 248 / 2290-5 / 2290E-5，GPIB）。

    说明：
    - 现有项目内部仍沿用“Keithley 248”这一设备类型名，避免破坏旧配置与网页桥接。
    - 但连接层已经放宽为 Keithley 248 / 2290-5 / 2290E-5 兼容。
    - 对 2290 系列，手册说明命令结束符为 <LF> 或 EOI，查询响应默认以 <LF>+EOI 结束；
      因此这里显式设置 read/write termination，并在 query 失败时增加 write/read 回退，
      用来兼容部分 GPIB 适配器或较慢的仪器响应。
    """

    power_source_key = "keithley"
    power_source_name = "Keithley 248"
    supports_internal_current_readback = True

    def __init__(self):
        self.instrument = None  # legacy; kept for compatibility (not used after refactor)
        self.is_connected = False
        self.gpib_address = 14
        self.current_voltage = 0.0
        self.current_current = 0.0
        self._lock = threading.RLock()
        self.resource_name = ""
        self.detected_idn = ""
        self.detected_model = ""
        self._worker: VisaIOWorker | None = None
        self._session_configured = False
        self._last_error = ""
        self._last_write_ts = 0.0
        self._last_read_ts = 0.0

    def _build_resource_candidates(self, address) -> list[str]:
        raw = str(address).strip()
        if not raw:
            return []
        candidates = []
        board_index = '0'
        primary_address = ''

        if '::' in raw:
            candidates.append(raw)
            parts = [p.strip() for p in raw.split('::') if p.strip()]
            if parts:
                first = parts[0].upper()
                if first.startswith('GPIB'):
                    suffix = first[4:]
                    if suffix.isdigit():
                        board_index = suffix
            digit_parts = [p for p in parts if p.isdigit()]
            if digit_parts:
                primary_address = digit_parts[0]
        elif raw.isdigit():
            primary_address = raw
        else:
            candidates.append(raw)

        if primary_address:
            candidates.extend([
                f"GPIB{board_index}::{primary_address}::INSTR",
                f"GPIB{board_index}::{primary_address}",
                f"GPIB::{primary_address}::INSTR",
                f"GPIB::{primary_address}",
            ])
            if board_index != '0':
                candidates.extend([
                    f"GPIB0::{primary_address}::INSTR",
                    f"GPIB0::{primary_address}",
                ])

        # 去重，保持顺序
        deduped = []
        for item in candidates:
            if item and item not in deduped:
                deduped.append(item)
        return deduped

    def _configure_instrument_session(self, inst, force: bool = False):
        if self._session_configured and not force:
            return
        try:
            inst.timeout = max(int(getattr(inst, 'timeout', 8000) or 8000), 8000)
        except Exception:
            pass
        for attr, value in (
            ('write_termination', "\n"),
            ('read_termination', "\n"),
            ('send_end', True),
            ('query_delay', 0.02),
            ('chunk_size', 4096),
        ):
            try:
                setattr(inst, attr, value)
            except Exception:
                pass
        if force:
            for action in ('clear',):
                try:
                    getattr(inst, action)()
                except Exception:
                    pass
            try:
                inst.write('*CLS')
            except Exception:
                pass
            time.sleep(0.05)
        self._session_configured = True

    def _query_text(self, command: str, timeout_s: float = 5.5):
        if not self._worker:
            raise RuntimeError('VISA资源未打开')

        def _do_query(inst):
            self._configure_instrument_session(inst)
            attempts = []

            def _record_failure(method_name: str, exc: Exception):
                attempts.append(f"{method_name}: {exc}")

            # 优先用 query
            try:
                result = inst.query(command)
                if result is not None:
                    return str(result).strip()
            except Exception as exc:
                _record_failure('query', exc)

            # 某些适配器/机型在显式 write/read 下更稳定
            try:
                try:
                    inst.clear()
                except Exception:
                    pass
                inst.write(command)
                time.sleep(0.12)
                result = inst.read()
                if result is not None:
                    return str(result).strip()
            except Exception as exc:
                _record_failure('write/read', exc)

            # 再做一次更保守的重试：禁用 read_termination，读取裸字符串后剥离换行
            try:
                old_rt = getattr(inst, 'read_termination', None)
            except Exception:
                old_rt = None
            try:
                try:
                    inst.read_termination = None
                except Exception:
                    pass
                try:
                    inst.clear()
                except Exception:
                    pass
                inst.write(command)
                time.sleep(0.18)
                result = inst.read()
                if result is not None:
                    return str(result).strip()
            except Exception as exc:
                _record_failure('raw-read', exc)
            finally:
                try:
                    inst.read_termination = old_rt
                except Exception:
                    pass

            raise TimeoutError('; '.join(attempts) if attempts else '无响应')

        return self._worker.call(_do_query, timeout_s=float(timeout_s))

    def _write_only(self, command: str, timeout_s: float = 3.5):
        if not self._worker:
            raise RuntimeError('VISA资源未打开')

        def _do_write(inst):
            self._configure_instrument_session(inst)
            inst.write(command)
            return 'OK'

        return self._worker.call(_do_write, timeout_s=float(timeout_s))

    def _probe_identity(self) -> tuple[str, str]:
        """返回 (idn文本, 识别到的型号标签)。

        连接成功判据不再只依赖 *IDN?：
        - 先试 *IDN?
        - 失败时再试 VSET? / VLIM? 等兼容命令
        这样 2290E-5 在某些 GPIB 适配器上即使 *IDN? 读回较慢，也仍可被识别为兼容电源。
        """
        idn = ''
        model = ''
        last_err = None

        # 1) 识别信息
        for cmd in ('*IDN?',):
            try:
                idn = self._query_text(cmd, timeout_s=6.5)
                if idn:
                    upper = idn.upper()
                    if '2290' in upper:
                        model = 'Keithley 2290-5/2290E-5'
                    elif '248' in upper:
                        model = 'Keithley 248'
                    else:
                        model = 'Keithley-compatible'
                    return idn, model
            except Exception as exc:
                last_err = exc

        # 2) 命令兼容性探测
        for cmd in ('VSET?', 'VLIM?', 'IOUT?', 'VOUT?'):
            try:
                resp = self._query_text(cmd, timeout_s=6.0)
                if resp not in (None, ''):
                    text = str(resp).strip()
                    try:
                        float(text)
                    except Exception:
                        # 不是数值也视作有应答，仍可认为兼容
                        pass
                    return text, 'Keithley 2290/248 compatible'
            except Exception as exc:
                last_err = exc

        raise last_err or TimeoutError('未收到 Keithley 兼容响应')

    def connect_gpib(self, address):
        """连接GPIB设备。兼容 Keithley 248 / 2290-5 / 2290E-5。"""
        with self._lock:
            self.disconnect()

            candidates = self._build_resource_candidates(address)
            if not candidates:
                return False, '未提供有效的 GPIB 地址'

            last_err = None
            for resource_name in candidates:
                try:
                    self._worker = VisaIOWorker(resource_name=resource_name, timeout_ms=8000)
                    self._worker.start()
                    time.sleep(0.25)
                    try:
                        self._worker.call(lambda inst: self._configure_instrument_session(inst, force=True), timeout_s=2.5)
                    except Exception:
                        pass
                    ident_text, detected_model = self._probe_identity()
                    self.is_connected = True
                    try:
                        raw_addr = str(address).strip()
                        if raw_addr.isdigit():
                            self.gpib_address = int(raw_addr)
                        else:
                            parts = [p.strip() for p in str(resource_name).split('::') if p.strip()]
                            digit_parts = [p for p in parts if p.isdigit()]
                            if digit_parts:
                                self.gpib_address = int(digit_parts[0])
                    except Exception:
                        pass
                    self.resource_name = resource_name
                    self.detected_idn = ident_text or ''
                    self.detected_model = detected_model or ''
                    detail = ident_text.strip() if ident_text else detected_model or '兼容设备'
                    return True, f"GPIB连接成功，资源: {resource_name}, 设备: {detail}"
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

            return False, f"连接失败: {last_err}" if last_err else '连接失败'

    def disconnect(self):
        """断开连接"""
        with self._lock:
            if self._worker:
                try:
                    self._worker.stop()
                except Exception:
                    pass
                self._worker = None
            if self.instrument:
                try:
                    self.instrument.close()
                except Exception:
                    pass
                self.instrument = None
            self.is_connected = False
            self.resource_name = ""
            self.detected_idn = ""
            self.detected_model = ""
            self._session_configured = False

    def send_command(self, command):
        """发送命令到设备。

        返回：
        - 查询命令：返回字符串结果
        - 写命令：返回 "OK"
        - 失败：返回 None，并把详细原因写入 self._last_error
        """
        with self._lock:
            if not self.is_connected or not self._worker:
                self._last_error = "设备未连接"
                return None

            cmd = str(command).strip()
            last_error = None
            try:
                if cmd.endswith("?"):
                    result = self._query_text(cmd, timeout_s=5.0)
                    self._last_read_ts = time.time()
                    self._last_error = ""
                    return result

                attempts = 3 if cmd.upper().startswith('VSET') else 2
                for attempt in range(attempts):
                    try:
                        self._write_only(cmd, timeout_s=3.5)
                        self._last_write_ts = time.time()
                        self._last_error = ""
                        time.sleep(0.05)
                        return "OK"
                    except Exception as exc:
                        last_error = exc
                        # 设定电压失败时做一次轻量级会话恢复，尽量贴近旧版“直接写入”的稳定行为
                        try:
                            if self._worker:
                                self._worker.call(lambda inst: self._configure_instrument_session(inst, force=True), timeout_s=1.5)
                        except Exception:
                            pass
                        if attempt < attempts - 1:
                            time.sleep(0.12 * (attempt + 1))
                            continue
                        raise
            except Exception as e:
                self._last_error = str(last_error or e)
                logger.info(f"发送命令错误: {cmd}, 错误: {self._last_error}")
                return None

    def read_voltage(self):
        """读取实际输出电压（VOUT?命令）。

        当刚执行过 VSET/HVON/HVOF 等写命令时，优先返回缓存值，
        避免后台轮询在仪器尚未完成内部处理时立刻再次发起 GPIB 查询，
        与稳流/升压控制线程形成竞争。
        """
        with self._lock:
            if not self.is_connected or not self._worker:
                return None
            try:
                now = time.time()
                if (now - float(getattr(self, '_last_write_ts', 0.0))) < 0.45:
                    return float(self.current_voltage)
                response = self._query_text("VOUT?", timeout_s=2.0)
                if response in (None, ''):
                    return None
                voltage = float(response)
                self.current_voltage = voltage
                self._last_read_ts = now
                return voltage
            except Exception as exc:
                self._last_error = str(exc)
                return None

    def read_current(self):
        """读取实际输出电流（IOUT?命令）"""
        with self._lock:
            if not self.is_connected or not self._worker:
                return None
            try:
                response = self._query_text("IOUT?", timeout_s=2.0)
                if response in (None, ''):
                    return None
                current = float(response)
                self.current_current = current
                self._last_read_ts = time.time()
                return current * 1e6  # 转换为uA
            except Exception as exc:
                self._last_error = str(exc)
                return None

    def set_voltage(self, voltage):
        """设置输出电压（VSET命令）。

        这里保留“仅写设定值、不立即做额外查询校验”的旧版思路，
        减少与后台电压轮询/稳流线程之间的 GPIB 往返竞争。
        """
        with self._lock:
            try:
                fv = float(voltage)
            except Exception:
                return False, f"无效电压值: {voltage}"
            success = self.send_command(f"VSET {fv}")
            if success is not None:
                self.current_voltage = fv
                return True, f"电压设置为: {fv}V"
            detail = str(getattr(self, '_last_error', '') or '').strip()
            return False, f"设置电压失败: {detail}" if detail else "设置电压失败"

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

    # 与 HAPS06Controller 对齐的兼容接口
    def set_voltage_only(self, voltage):
        return self.set_voltage(voltage)

    def stop_output(self):
        ok1, msg1 = self.set_voltage(0.0)
        ok2, msg2 = self.disable_high_voltage()
        if ok1 and ok2:
            return True, "电压已置零且高压输出已关闭"
        return False, f"{msg1}; {msg2}"

    def reset_voltage(self):
        return self.set_voltage(100.0)

    def manual_set_voltage(self, voltage):
        voltage = float(voltage)
        if abs(voltage) <= 1e-12:
            ok1, msg1 = self.set_voltage(0.0)
            ok2, msg2 = self.disable_high_voltage()
            if ok1 and ok2:
                return True, "电压已设置为0V，并关闭高压输出"
            if ok1 and not ok2:
                return False, f"电压已设置为0V，但关闭高压输出失败: {msg2}"
            if (not ok1) and ok2:
                return False, f"高压输出已关闭，但电压置零失败: {msg1}"
            return False, f"{msg1}; {msg2}"
        return self.set_voltage(voltage)

