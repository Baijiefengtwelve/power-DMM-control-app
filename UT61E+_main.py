#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""UT61E+ reader via CP2110 HID USB-to-UART bridge.

This script is intended as a replacement for the earlier CH9329-based attempt.
It keeps the HID enumeration / manual selection flow, but switches the transport
layer to CP2110, which matches the device that was actually enumerated on the
user's machine.
"""

from __future__ import annotations

import argparse
import decimal
import json
import logging
import sys
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional

import hid

LOG = logging.getLogger("ut61eplus")


@dataclass
class HIDDeviceInfo:
    index: int
    path: bytes
    vendor_id: int
    product_id: int
    interface_number: int
    usage_page: int
    usage: int
    manufacturer: str
    product: str
    serial_number: str

    @classmethod
    def from_enum(cls, index: int, item: dict) -> "HIDDeviceInfo":
        path = item.get("path", b"")
        if isinstance(path, str):
            path = path.encode("utf-8", errors="replace")
        return cls(
            index=index,
            path=path,
            vendor_id=int(item.get("vendor_id", 0) or 0),
            product_id=int(item.get("product_id", 0) or 0),
            interface_number=int(item.get("interface_number", -1) or -1),
            usage_page=int(item.get("usage_page", 0) or 0),
            usage=int(item.get("usage", 0) or 0),
            manufacturer=str(item.get("manufacturer_string") or ""),
            product=str(item.get("product_string") or ""),
            serial_number=str(item.get("serial_number") or ""),
        )

    @property
    def path_text(self) -> str:
        return self.path.decode("utf-8", errors="replace")

    def one_line(self) -> str:
        return (
            f"[{self.index:03d}] VID=0x{self.vendor_id:04X} PID=0x{self.product_id:04X} "
            f"iface={self.interface_number} usage_page={self.usage_page} usage={self.usage} "
            f"manufacturer='{self.manufacturer}' product='{self.product}' serial='{self.serial_number}'"
        )


class Measurement:
    """Decode a single UT61E+ measurement payload (14 bytes)."""

    _MODE = [
        'ACV', 'ACmV', 'DCV', 'DCmV', 'Hz', '%', 'OHM', 'CONT', 'DIDOE',
        'CAP', '°C', '°F', 'DCuA', 'ACuA', 'DCmA', 'ACmA', 'DCA', 'ACA',
        'HFE', 'Live', 'NCV', 'LozV', 'ACA', 'DCA', 'LPF', 'AC/DC', 'LPF',
        'AC+DC', 'LPF', 'AC+DC2', 'INRUSH'
    ]
    _UNITS = {
        '%': {'0': '%'},
        'AC+DC': {'1': 'A'},
        'AC+DC2': {'1': 'A'},
        'AC/DC': {'0': 'V', '1': 'V', '2': 'V', '3': 'V'},
        'ACA': {'1': 'A'},
        'ACV': {'0': 'V', '1': 'V', '2': 'V', '3': 'V'},
        'ACmA': {'0': 'mA', '1': 'mA'},
        'ACmV': {'0': 'mV'},
        'ACuA': {'0': 'uA', '1': 'uA'},
        'CAP': {'0': 'nF', '1': 'nF', '2': 'uF', '3': 'uF', '4': 'uF', '5': 'mF', '6': 'mF', '7': 'mF'},
        'CONT': {'0': 'Ω'},
        'DCA': {'1': 'A'},
        'DCV': {'0': 'V', '1': 'V', '2': 'V', '3': 'V'},
        'DCmA': {'0': 'mA', '1': 'mA'},
        'DCmV': {'0': 'mV'},
        'DCuA': {'0': 'uA', '1': 'uA'},
        'DIDOE': {'0': 'V'},
        'Hz': {'0': 'Hz', '1': 'Hz', '2': 'kHz', '3': 'kHz', '4': 'kHz', '5': 'MHz', '6': 'MHz', '7': 'MHz'},
        'LPF': {'0': 'V', '1': 'V', '2': 'V', '3': 'V'},
        'LozV': {'0': 'V', '1': 'V', '2': 'V', '3': 'V'},
        'OHM': {'0': 'Ω', '1': 'kΩ', '2': 'kΩ', '3': 'kΩ', '4': 'MΩ', '5': 'MΩ', '6': 'MΩ'},
        '°C': {'0': '°C', '1': '°C'},
        '°F': {'0': '°F', '1': '°F'},
        'HFE': {'0': 'B'},
        'NCV': {'0': 'NCV'},
    }
    _OVERLOAD = {'.OL', 'O.L', 'OL.', 'OL', '-.OL', '-O.L', '-OL.', '-OL'}
    _EXPONENTS = {'M': 6, 'k': 3, 'm': -3, 'u': -6, 'n': -9}
    _CURRENT_MODES = {'DCuA', 'ACuA', 'DCmA', 'ACmA', 'DCA', 'ACA', 'AC+DC', 'AC+DC2'}
    _VOLTAGE_MODES = {'ACV', 'ACmV', 'DCV', 'DCmV', 'LozV', 'LPF', 'AC/DC'}

    def __init__(self, payload: bytes):
        if not isinstance(payload, (bytes, bytearray)) or len(payload) < 14:
            raise TypeError("Measurement requires at least 14 bytes")
        b = bytes(payload[:14])
        self.raw_bytes = b
        self.mode = self._MODE[b[0]] if b[0] < len(self._MODE) else f"MODE_{b[0]}"
        self.range_char = chr(b[1])
        self.display = b[2:9].decode("ascii", errors="ignore").replace(" ", "")
        self.is_overload = self.display in self._OVERLOAD

        self.is_max = (b[11] & 8) > 0
        self.is_min = (b[11] & 4) > 0
        self.is_hold = (b[11] & 2) > 0
        self.is_rel = (b[11] & 1) > 0
        self.is_auto_range = (b[12] & 4) == 0
        self.has_battery_warning = (b[12] & 2) > 0
        self.has_hv_warning = (b[12] & 1) > 0
        self.is_max_peak = (b[13] & 4) > 0
        self.is_min_peak = (b[13] & 2) > 0

        try:
            self.decimal_value = decimal.Decimal(self.display)
        except decimal.InvalidOperation:
            self.decimal_value = decimal.Decimal("NaN")

        self.display_unit = self._UNITS.get(self.mode, {}).get(self.range_char)
        self.unit = self.display_unit
        if self.unit and self.unit[0] in self._EXPONENTS and not self.is_overload:
            self.decimal_value = self.decimal_value.scaleb(self._EXPONENTS[self.unit[0]])
            self.unit = self.unit[1:]

    @property
    def kind(self) -> str:
        if self.mode in self._VOLTAGE_MODES:
            return "voltage"
        if self.mode in self._CURRENT_MODES:
            return "current"
        return "other"

    def to_dict(self) -> dict:
        min_max_status = None
        if self.is_max:
            min_max_status = 'max'
        elif self.is_min:
            min_max_status = 'min'
        elif self.is_max_peak:
            min_max_status = 'p-max'
        elif self.is_min_peak:
            min_max_status = 'p-min'

        value = 0.0
        if not self.is_overload and not self.decimal_value.is_nan():
            value = float(self.decimal_value)

        return {
            "value": value,
            "unit": self.unit,
            "mode": self.mode,
            "kind": self.kind,
            "display": self.display,
            "range": "AUTO" if self.is_auto_range else "MANUAL",
            "overload": self.is_overload,
            "hold": self.is_hold,
            "min_max": min_max_status,
            "rel": self.is_rel,
            "hv_warning": self.has_hv_warning,
            "bat_low": self.has_battery_warning,
        }


def enumerate_all_hid() -> List[HIDDeviceInfo]:
    devices = []
    for idx, item in enumerate(hid.enumerate(), start=1):
        devices.append(HIDDeviceInfo.from_enum(idx, item))
    return devices


def print_devices(devices: Iterable[HIDDeviceInfo]) -> None:
    print("=" * 98)
    for dev in devices:
        print(dev.one_line())
        print(f"      path={dev.path_text}")
    print("=" * 98)


def choose_device_interactive(prefer_cp2110: bool = True) -> Optional[HIDDeviceInfo]:
    keyword = ""
    while True:
        devices = enumerate_all_hid()
        if prefer_cp2110:
            devices = sorted(devices, key=lambda d: 0 if (d.vendor_id, d.product_id) == (0x10C4, 0xEA80) else 1)
            for i, d in enumerate(devices, start=1):
                d.index = i
        if keyword:
            kw = keyword.lower()
            devices = [
                d for d in devices
                if kw in d.path_text.lower()
                or kw in d.manufacturer.lower()
                or kw in d.product.lower()
                or kw in f"{d.vendor_id:04x}" or kw in f"{d.product_id:04x}"
            ]
            for i, d in enumerate(devices, start=1):
                d.index = i
        if not devices:
            print("<没有枚举到任何 HID 设备>")
        else:
            print_devices(devices)
        print("输入序号选择设备；r 刷新；f 过滤；c 清除过滤；q 退出")
        choice = input("请选择: ").strip().lower()
        if choice == "q":
            return None
        if choice == "r":
            continue
        if choice == "f":
            keyword = input("输入过滤关键字（如 cp2110 / 10c4 / ea80 / uni-t）: ").strip()
            continue
        if choice == "c":
            keyword = ""
            continue
        if choice.isdigit():
            idx = int(choice)
            for d in devices:
                if d.index == idx:
                    return d
            print("无效序号，请重新选择。")
            continue
        print("无效输入，请重新选择。")


class UT61EPlusCP2110:
    CP2110_VID = 0x10C4
    CP2110_PID = 0xEA80
    _SEQUENCE_SEND_DATA = bytes.fromhex("AB CD 03 5E 01 D9")
    _SEQUENCE_SEND_CMD = bytes.fromhex("AB CD 03")
    _COMMANDS = {
        'min_max': 65,
        'not_min_max': 66,
        'range': 70,
        'auto': 71,
        'rel': 72,
        'select2': 73,
        'hold': 74,
        'lamp': 75,
        'select1': 76,
        'p_min_max': 77,
        'not_peak': 78,
    }

    def __init__(
        self,
        vid: int = CP2110_VID,
        pid: int = CP2110_PID,
        path: Optional[bytes] = None,
        auto_pick_on_fail: bool = True,
        read_timeout_ms: int = 20,
    ):
        self.vid = vid
        self.pid = pid
        self.path = path
        self.auto_pick_on_fail = auto_pick_on_fail
        self.read_timeout_ms = read_timeout_ms
        self.dev = hid.device()
        self.read_buffer = bytearray()
        self.opened_path_text: Optional[str] = None
        self._open_device()
        self._configure_device()

    def _open_device(self) -> None:
        last_err: Optional[Exception] = None

        if self.path is not None:
            try:
                self.dev.open_path(self.path)
                self.opened_path_text = self.path.decode("utf-8", errors="replace")
                print(f"Opened HID path: {self.opened_path_text}")
                return
            except Exception as exc:
                last_err = exc

        # First try path-based opening for all matching VID/PID devices.
        for item in hid.enumerate(self.vid, self.pid):
            candidate = item.get("path", b"")
            if isinstance(candidate, str):
                candidate = candidate.encode("utf-8", errors="replace")
            try:
                self.dev.open_path(candidate)
                self.path = candidate
                self.opened_path_text = candidate.decode("utf-8", errors="replace")
                print(f"Opened HID path: {self.opened_path_text}")
                return
            except Exception as exc:
                last_err = exc

        try:
            self.dev.open(self.vid, self.pid)
            self.opened_path_text = None
            print(f"Opened by VID/PID: VID=0x{self.vid:04X} PID=0x{self.pid:04X}")
            return
        except Exception as exc:
            last_err = exc

        if not self.auto_pick_on_fail:
            raise OSError(
                f"open(VID=0x{self.vid:04X}, PID=0x{self.pid:04X}) failed: {last_err}"
            ) from last_err

        print(f"固定 VID/PID 打开失败: VID=0x{self.vid:04X}, PID=0x{self.pid:04X}")
        print(f"最后错误: {last_err}")
        print("下面进入 HID 设备手动选择模式。")
        chosen = choose_device_interactive(prefer_cp2110=True)
        if chosen is None:
            raise SystemExit("用户取消了设备选择。")
        self.vid = chosen.vendor_id
        self.pid = chosen.product_id
        self.path = chosen.path
        self.dev.open_path(chosen.path)
        self.opened_path_text = chosen.path_text
        print(f"Opened HID path: {self.opened_path_text}")

    def _configure_device(self) -> None:
        # 0x41: enable UART; 0x50: UART config; 0x43: purge RX FIFO.
        # The 0x50 report encodes 9600 baud, no parity, no flow control,
        # 8 data bits, 1 stop bit.
        self.dev.send_feature_report([0x41, 0x01])
        self.dev.send_feature_report([0x50, 0x00, 0x00, 0x25, 0x80, 0x00, 0x00, 0x03, 0x00, 0x00])
        self.dev.send_feature_report([0x43, 0x02])

    def close(self) -> None:
        try:
            self.dev.close()
        except Exception:
            pass

    def _write_frame(self, payload: bytes) -> None:
        if len(payload) > 63:
            raise ValueError("CP2110 HID write payload must be <= 63 bytes per report")
        report = bytes([len(payload)]) + payload
        self.dev.write(report)

    def _read_report_payload(self, timeout_ms: Optional[int] = None) -> bytes:
        raw = self.dev.read(64, self.read_timeout_ms if timeout_ms is None else timeout_ms)
        if not raw:
            return b""
        if isinstance(raw, bytes):
            raw = list(raw)
        data_len = int(raw[0])
        if data_len <= 0:
            return b""
        return bytes(raw[1:1 + data_len])

    @staticmethod
    def _checksum_ok(packet: bytes) -> bool:
        if len(packet) < 5:
            return False
        checksum = (packet[-2] << 8) | packet[-1]
        return (sum(packet[:-2]) & 0xFFFF) == checksum

    def _read_packet(self, timeout: float = 1.0) -> Optional[bytes]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            # Try to parse existing buffered data first.
            start = self.read_buffer.find(b"\xAB\xCD")
            if start != -1 and len(self.read_buffer) >= start + 3:
                payload_len = self.read_buffer[start + 2]
                frame_len = 3 + payload_len
                if len(self.read_buffer) >= start + frame_len:
                    frame = bytes(self.read_buffer[start:start + frame_len])
                    del self.read_buffer[:start + frame_len]
                    if self._checksum_ok(frame):
                        return frame[3:]
                    LOG.warning("Checksum error, dropping frame: %s", frame.hex(" "))
                    continue

            chunk = self._read_report_payload(timeout_ms=max(1, int((deadline - time.time()) * 1000)))
            if chunk:
                self.read_buffer.extend(chunk)
                continue
            time.sleep(0.005)
        return None

    def take_measurement(self, retries: int = 3, timeout: float = 1.0) -> Optional[Measurement]:
        for _ in range(max(1, retries)):
            self._write_frame(self._SEQUENCE_SEND_DATA)
            payload = self._read_packet(timeout=timeout)
            if payload and len(payload) >= 16:
                try:
                    return Measurement(payload[:-2])
                except Exception as exc:
                    LOG.debug("Failed to decode measurement: %s", exc)
            time.sleep(0.05)
        return None

    def send_command(self, cmd: str | int) -> None:
        cmd_code = self._COMMANDS.get(cmd) if isinstance(cmd, str) else cmd
        if not isinstance(cmd_code, int):
            raise ValueError(f"Invalid command: {cmd}")
        seq = bytearray(self._SEQUENCE_SEND_CMD)
        checksum = (sum(seq) + cmd_code) & 0xFFFF
        seq.extend([cmd_code, (checksum >> 8) & 0xFF, checksum & 0xFF])
        self._write_frame(bytes(seq))
        _ = self._read_packet(timeout=0.2)


def format_measurement(m: Measurement) -> str:
    data = m.to_dict()
    tag = {
        "voltage": "VOLTAGE",
        "current": "CURRENT",
        "other": "OTHER",
    }[data["kind"]]
    return (
        f"[{tag}] mode={data['mode']:<7} value={data['value']:>12g} {data['unit'] or ''} "
        f"display={data['display']:<8} range={data['range']}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Read voltage/current from UT61E+ via CP2110")
    parser.add_argument("--vid", type=lambda x: int(x, 0), default=0x10C4)
    parser.add_argument("--pid", type=lambda x: int(x, 0), default=0xEA80)
    parser.add_argument("--path", type=str, default=None, help="Open a specific HID path")
    parser.add_argument("--list-all", action="store_true", help="List all HID devices and exit")
    parser.add_argument("--pick", action="store_true", help="Always enter interactive HID selection")
    parser.add_argument("--count", type=int, default=0, help="Number of reads; 0 means continuous")
    parser.add_argument("--interval", type=float, default=0.3, help="Seconds between reads")
    parser.add_argument("--json", action="store_true", help="Print JSON lines")
    parser.add_argument("--all", action="store_true", help="Print all measurement modes, not only voltage/current")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if args.list_all:
        devices = enumerate_all_hid()
        if not devices:
            print("<没有枚举到任何 HID 设备>")
            return 1
        print_devices(devices)
        return 0

    path_bytes: Optional[bytes] = None
    vid = args.vid
    pid = args.pid

    if args.pick:
        chosen = choose_device_interactive(prefer_cp2110=True)
        if chosen is None:
            return 1
        path_bytes = chosen.path
        vid = chosen.vendor_id
        pid = chosen.product_id
    elif args.path:
        path_bytes = args.path.encode("utf-8", errors="replace")

    meter: Optional[UT61EPlusCP2110] = None
    try:
        meter = UT61EPlusCP2110(vid=vid, pid=pid, path=path_bytes, auto_pick_on_fail=True)
        read_count = 0
        while True:
            m = meter.take_measurement(retries=3, timeout=1.0)
            if m is None:
                print("No valid measurement packet received.")
            else:
                if args.all or m.kind in {"voltage", "current"}:
                    if args.json:
                        print(json.dumps(m.to_dict(), ensure_ascii=False))
                    else:
                        print(format_measurement(m))
            read_count += 1
            if args.count > 0 and read_count >= args.count:
                break
            time.sleep(max(0.0, args.interval))
    except KeyboardInterrupt:
        pass
    finally:
        if meter is not None:
            meter.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
