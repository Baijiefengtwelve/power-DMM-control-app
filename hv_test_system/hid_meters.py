from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from .common import *

try:
    import hid  # type: ignore
except Exception:  # pragma: no cover
    hid = None  # type: ignore


def _path_to_text(path) -> str:
    if isinstance(path, bytes):
        return path.decode("utf-8", errors="replace")
    return str(path or "")


def _path_to_bytes(path) -> bytes:
    if isinstance(path, bytes):
        return path
    return str(path or "").encode("utf-8", errors="replace")


@dataclass(frozen=True)
class HIDDeviceInfo:
    path: bytes
    vendor_id: int
    product_id: int
    interface_number: int
    usage_page: int
    usage: int
    manufacturer: str
    product: str
    serial_number: str

    @property
    def path_text(self) -> str:
        return _path_to_text(self.path)

    @classmethod
    def from_enumeration(cls, item: dict) -> "HIDDeviceInfo":
        return cls(
            path=_path_to_bytes(item.get("path", b"")),
            vendor_id=int(item.get("vendor_id", 0) or 0),
            product_id=int(item.get("product_id", 0) or 0),
            interface_number=int(item.get("interface_number", -1) or -1),
            usage_page=int(item.get("usage_page", 0) or 0),
            usage=int(item.get("usage", 0) or 0),
            manufacturer=str(item.get("manufacturer_string") or ""),
            product=str(item.get("product_string") or ""),
            serial_number=str(item.get("serial_number") or ""),
        )


def enumerate_hid_devices() -> list[HIDDeviceInfo]:
    if hid is None:
        return []
    devices = []
    try:
        for item in hid.enumerate():
            devices.append(HIDDeviceInfo.from_enumeration(item))
    except Exception:
        return []
    return devices


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    lowered = str(text or "").lower()
    return any(keyword in lowered for keyword in keywords)


def is_ut61eplus_device(info: HIDDeviceInfo) -> bool:
    searchable = " ".join(
        [
            info.path_text,
            info.manufacturer,
            info.product,
            info.serial_number,
            f"{info.vendor_id:04x}",
            f"{info.product_id:04x}",
        ]
    )
    if (info.vendor_id, info.product_id) == (0x10C4, 0xEA80):
        return True
    return _contains_any(searchable, ("ut61", "uni-t", "cp2110"))


def is_victor_86e_device(info: HIDDeviceInfo) -> bool:
    searchable = " ".join(
        [
            info.path_text,
            info.manufacturer,
            info.product,
            info.serial_number,
            f"{info.vendor_id:04x}",
            f"{info.product_id:04x}",
        ]
    )
    return _contains_any(searchable, ("victor", "86e"))


def enumerate_ut61eplus_devices() -> list[HIDDeviceInfo]:
    return [device for device in enumerate_hid_devices() if is_ut61eplus_device(device)]


def enumerate_victor_86e_devices() -> list[HIDDeviceInfo]:
    return [device for device in enumerate_hid_devices() if is_victor_86e_device(device)]


def _match_hid_device(candidate: HIDDeviceInfo, target: dict) -> bool:
    target_path = str(target.get("path") or "").strip()
    if target_path and candidate.path_text == target_path:
        return True

    target_serial = str(target.get("serial") or "").strip()
    if (
        target_serial
        and candidate.serial_number
        and target_serial == candidate.serial_number
        and int(target.get("vid", 0) or 0) == candidate.vendor_id
        and int(target.get("pid", 0) or 0) == candidate.product_id
    ):
        return True

    if (
        int(target.get("vid", 0) or 0) == candidate.vendor_id
        and int(target.get("pid", 0) or 0) == candidate.product_id
    ):
        product = str(target.get("product") or "").strip().lower()
        manufacturer = str(target.get("manufacturer") or "").strip().lower()
        if product and product == candidate.product.lower():
            return True
        if manufacturer and manufacturer == candidate.manufacturer.lower() and not product:
            return True
    return False


class _BaseHIDMeterThread(QThread):
    data_received = pyqtSignal(dict)
    log_message_signal = pyqtSignal(str)

    device_label = "HID meter"

    def __init__(self, device_config: dict, meter_name: str):
        super().__init__()
        self.device_config = dict(device_config or {})
        self.meter_name = meter_name
        self._running = True
        self._mutex = QMutex()
        self._device = None

    def stop(self):
        with QMutexLocker(self._mutex):
            self._running = False
        self._close_device()
        if not self.wait(2000):
            self.terminate()
            self.wait(500)

    def _should_stop(self) -> bool:
        with QMutexLocker(self._mutex):
            return not self._running

    def _sleep_with_stop(self, seconds: float):
        remaining_ms = int(max(0.0, float(seconds)) * 1000)
        while remaining_ms > 0 and not self._should_stop():
            step = min(200, remaining_ms)
            self.msleep(step)
            remaining_ms -= step

    def _close_device(self):
        device = self._device
        self._device = None
        if device is None:
            return
        try:
            device.close()
        except Exception:
            pass

    def _enumerate_candidates(self) -> list[HIDDeviceInfo]:
        return []

    def _open_device(self):
        if hid is None:
            raise RuntimeError("hid library is not available")

        selected = None
        for candidate in self._enumerate_candidates():
            if _match_hid_device(candidate, self.device_config):
                selected = candidate
                break
        if selected is None:
            raise RuntimeError("Configured HID device is not present")

        device = hid.device()
        device.open_path(selected.path)
        self._device = device
        return device, selected

    def _prepare_device(self, _device):
        return None

    def _read_measurement(self, _device):
        raise NotImplementedError

    def run(self):
        backoff_s = 1.0
        max_backoff_s = 10.0
        last_connected_label = ""

        try:
            while not self._should_stop():
                if self._device is None:
                    try:
                        device, info = self._open_device()
                        self._prepare_device(device)
                        last_connected_label = info.path_text
                        self.log_message_signal.emit(f"{self.device_label} connected ({self.meter_name}): {last_connected_label}")
                        backoff_s = 1.0
                    except Exception as exc:
                        self.log_message_signal.emit(
                            f"{self.device_label} connect error ({self.meter_name}): {exc}; retry in {backoff_s:.0f}s"
                        )
                        self._sleep_with_stop(backoff_s)
                        backoff_s = min(max_backoff_s, backoff_s * 2.0)
                        continue

                try:
                    measurement = self._read_measurement(self._device)
                    if measurement:
                        measurement["meter_name"] = self.meter_name
                        self.data_received.emit(measurement)
                except Exception as exc:
                    if not self._should_stop():
                        self.log_message_signal.emit(
                            f"{self.device_label} read error ({self.meter_name}): {exc}; reconnecting"
                        )
                    self._close_device()
                    self._sleep_with_stop(min(2.0, backoff_s))
                    continue

                self.msleep(60)
        finally:
            if last_connected_label and not self._should_stop():
                self.log_message_signal.emit(f"{self.device_label} disconnected ({self.meter_name})")
            self._close_device()


class _UT61EPlusMeasurement:
    _MODE = [
        "ACV", "ACmV", "DCV", "DCmV", "Hz", "%", "OHM", "CONT", "DIDOE",
        "CAP", "degC", "degF", "DCuA", "ACuA", "DCmA", "ACmA", "DCA", "ACA",
        "HFE", "Live", "NCV", "LozV", "ACA", "DCA", "LPF", "AC/DC", "LPF",
        "AC+DC", "LPF", "AC+DC2", "INRUSH",
    ]
    _UNITS = {
        "%": {"0": "%"},
        "AC+DC": {"1": "A"},
        "AC+DC2": {"1": "A"},
        "AC/DC": {"0": "V", "1": "V", "2": "V", "3": "V"},
        "ACA": {"1": "A"},
        "ACV": {"0": "V", "1": "V", "2": "V", "3": "V"},
        "ACmA": {"0": "mA", "1": "mA"},
        "ACmV": {"0": "mV"},
        "ACuA": {"0": "uA", "1": "uA"},
        "CAP": {"0": "nF", "1": "nF", "2": "uF", "3": "uF", "4": "uF", "5": "mF", "6": "mF", "7": "mF"},
        "CONT": {"0": "ohm"},
        "DCA": {"1": "A"},
        "DCV": {"0": "V", "1": "V", "2": "V", "3": "V"},
        "DCmA": {"0": "mA", "1": "mA"},
        "DCmV": {"0": "mV"},
        "DCuA": {"0": "uA", "1": "uA"},
        "DIDOE": {"0": "V"},
        "Hz": {"0": "Hz", "1": "Hz", "2": "kHz", "3": "kHz", "4": "kHz", "5": "MHz", "6": "MHz", "7": "MHz"},
        "LPF": {"0": "V", "1": "V", "2": "V", "3": "V"},
        "LozV": {"0": "V", "1": "V", "2": "V", "3": "V"},
        "OHM": {"0": "ohm", "1": "kohm", "2": "kohm", "3": "kohm", "4": "Mohm", "5": "Mohm", "6": "Mohm"},
        "degC": {"0": "degC", "1": "degC"},
        "degF": {"0": "degF", "1": "degF"},
        "HFE": {"0": "B"},
        "NCV": {"0": "NCV"},
    }
    _OVERLOAD = {".OL", "O.L", "OL.", "OL", "-.OL", "-O.L", "-OL.", "-OL"}
    _EXPONENTS = {"M": 6, "k": 3, "m": -3, "u": -6, "n": -9}
    _CURRENT_MODES = {"DCuA", "ACuA", "DCmA", "ACmA", "DCA", "ACA", "AC+DC", "AC+DC2"}
    _VOLTAGE_MODES = {"ACV", "ACmV", "DCV", "DCmV", "LozV", "LPF", "AC/DC"}

    def __init__(self, payload: bytes):
        if len(payload) < 14:
            raise ValueError("UT61E+ payload length must be at least 14 bytes")
        frame = bytes(payload[:14])
        self.mode = self._MODE[frame[0]] if frame[0] < len(self._MODE) else f"MODE_{frame[0]}"
        self.range_char = chr(frame[1])
        self.display = frame[2:9].decode("ascii", errors="ignore").replace(" ", "")
        self.is_overload = self.display in self._OVERLOAD
        self.display_unit = self._UNITS.get(self.mode, {}).get(self.range_char)
        self.unit = self.display_unit

        try:
            self.decimal_value = Decimal(self.display)
        except InvalidOperation:
            self.decimal_value = Decimal("NaN")

        if self.unit and self.unit[:1] in self._EXPONENTS and not self.is_overload:
            self.decimal_value = self.decimal_value.scaleb(self._EXPONENTS[self.unit[:1]])
            self.unit = self.unit[1:]

    @property
    def kind(self) -> str:
        if self.mode in self._VOLTAGE_MODES:
            return "voltage"
        if self.mode in self._CURRENT_MODES:
            return "current"
        return "other"

    def to_dict(self) -> dict:
        value = 0.0
        if not self.is_overload and not self.decimal_value.is_nan():
            value = float(self.decimal_value)
        return {
            "value": value,
            "unit": self.unit or "",
            "kind": self.kind,
            "mode": self.mode,
            "display": self.display,
            "overload": self.is_overload,
        }


class _UT61EPlusReader:
    _SEQUENCE_SEND_DATA = bytes.fromhex("AB CD 03 5E 01 D9")

    def __init__(self, device_config: dict):
        self.device_config = dict(device_config or {})
        self.dev = None
        self.read_buffer = bytearray()

    def open(self):
        if hid is None:
            raise RuntimeError("hid library is not available")
        selected = None
        for candidate in enumerate_ut61eplus_devices():
            if _match_hid_device(candidate, self.device_config):
                selected = candidate
                break
        if selected is None:
            raise RuntimeError("UT61E+ HID device is not present")

        self.dev = hid.device()
        self.dev.open_path(selected.path)
        self.dev.send_feature_report([0x41, 0x01])
        self.dev.send_feature_report([0x50, 0x00, 0x00, 0x25, 0x80, 0x00, 0x00, 0x03, 0x00, 0x00])
        self.dev.send_feature_report([0x43, 0x02])
        return selected

    def close(self):
        if self.dev is None:
            return
        try:
            self.dev.close()
        except Exception:
            pass
        self.dev = None

    def _write_frame(self, payload: bytes):
        if self.dev is None:
            raise RuntimeError("UT61E+ reader is not open")
        report = bytes([len(payload)]) + payload
        self.dev.write(report)

    def _read_report_payload(self, timeout_ms: int) -> bytes:
        if self.dev is None:
            raise RuntimeError("UT61E+ reader is not open")
        raw = self.dev.read(64, timeout_ms)
        if not raw:
            return b""
        if isinstance(raw, bytes):
            raw = list(raw)
        payload_len = int(raw[0])
        if payload_len <= 0:
            return b""
        return bytes(raw[1:1 + payload_len])

    @staticmethod
    def _checksum_ok(packet: bytes) -> bool:
        if len(packet) < 5:
            return False
        checksum = (packet[-2] << 8) | packet[-1]
        return (sum(packet[:-2]) & 0xFFFF) == checksum

    def _read_packet(self, timeout_s: float) -> bytes | None:
        deadline = time.time() + max(0.1, float(timeout_s))
        while time.time() < deadline:
            start = self.read_buffer.find(b"\xAB\xCD")
            if start >= 0 and len(self.read_buffer) >= start + 3:
                payload_len = self.read_buffer[start + 2]
                frame_len = 3 + payload_len
                if len(self.read_buffer) >= start + frame_len:
                    frame = bytes(self.read_buffer[start:start + frame_len])
                    del self.read_buffer[:start + frame_len]
                    if self._checksum_ok(frame):
                        return frame[3:]

            timeout_ms = max(1, int((deadline - time.time()) * 1000))
            chunk = self._read_report_payload(timeout_ms)
            if chunk:
                self.read_buffer.extend(chunk)
                continue
            time.sleep(0.01)
        return None

    def take_measurement(self, retries: int = 3, timeout_s: float = 1.0) -> dict | None:
        for _ in range(max(1, int(retries))):
            self._write_frame(self._SEQUENCE_SEND_DATA)
            payload = self._read_packet(timeout_s)
            if payload and len(payload) >= 16:
                measurement = _UT61EPlusMeasurement(payload[:-2])
                data = measurement.to_dict()
                if data.get("kind") in {"voltage", "current"} and not data.get("overload", False):
                    return data
            time.sleep(0.05)
        return None


class UT61EPlusHIDThread(_BaseHIDMeterThread):
    device_label = "UT61E+ HID"

    def __init__(self, device_config: dict, meter_name: str):
        super().__init__(device_config=device_config, meter_name=meter_name)
        self._reader = _UT61EPlusReader(device_config)

    def _enumerate_candidates(self) -> list[HIDDeviceInfo]:
        return enumerate_ut61eplus_devices()

    def _open_device(self):
        selected = self._reader.open()
        self._device = self._reader.dev
        return self._device, selected

    def _close_device(self):
        try:
            self._reader.close()
        finally:
            self._device = None

    def _read_measurement(self, _device):
        return self._reader.take_measurement(retries=2, timeout_s=1.0)


def parse_victor_86e_frame(data: bytes):
    if len(data) < 14:
        return None
    frame = bytes(data[:14])
    if frame[12] != 0x0D or frame[13] != 0x0A:
        return None

    status = frame[7]
    if status == 0x31:
        return None

    sign = -1 if status == 0x34 else 1
    func = frame[6]
    range_index = frame[0] & 0x0F

    range_map = {
        0x3B: {
            0x0: ("V", 10000.0),
            0x1: ("V", 1000.0),
            0x2: ("V", 100.0),
            0x3: ("V", 10.0),
            0x4: ("mV", 100.0),
        },
        0x3D: {
            0x0: ("uA", 100.0),
            0x1: ("uA", 10.0),
        },
        0x3F: {
            0x0: ("mA", 1000.0),
            0x1: ("mA", 100.0),
        },
        0x30: {
            0x0: ("A", 1000.0),
        },
    }

    mode_ranges = range_map.get(func, {})
    if range_index not in mode_ranges:
        return None

    raw_value = 0
    for offset in range(1, 6):
        digit = frame[offset] & 0x0F
        if digit > 9:
            return None
        raw_value = (raw_value * 10) + digit

    unit, divisor = mode_ranges[range_index]
    kind = "voltage" if unit in {"V", "mV"} else "current"
    return {
        "value": sign * (raw_value / divisor),
        "unit": unit,
        "kind": kind,
    }


class Victor86EHIDThread(_BaseHIDMeterThread):
    device_label = "Victor 86E HID"

    def _enumerate_candidates(self) -> list[HIDDeviceInfo]:
        return enumerate_victor_86e_devices()

    def _read_measurement(self, device):
        if device is None:
            raise RuntimeError("Victor 86E device is not open")
        report = device.read(64, 1000)
        if not report:
            return None
        if isinstance(report, bytes):
            report = list(report)
        if not report:
            return None
        if report[0] == 0x00 and len(report) >= 15:
            frame = bytes(report[1:15])
        else:
            frame = bytes(report[:14])
        return parse_victor_86e_frame(frame)
