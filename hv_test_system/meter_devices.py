from __future__ import annotations

import json
from dataclasses import dataclass

import serial.tools.list_ports

from .hid_meters import enumerate_ut61eplus_devices, enumerate_victor_86e_devices


SERIAL_DEVICE_PREFIX = "serial::"
HID_DEVICE_PREFIX = "hid::"


@dataclass(frozen=True)
class MeterDeviceOption:
    device_id: str
    label: str
    tooltip: str
    device_type: str
    protocol: str

    def to_dict(self) -> dict:
        return {
            "id": self.device_id,
            "label": self.label,
            "tooltip": self.tooltip,
            "device_type": self.device_type,
            "protocol": self.protocol,
        }


def list_serial_port_names() -> list[str]:
    try:
        return [port.device for port in serial.tools.list_ports.comports()]
    except Exception:
        return []


def encode_serial_device_id(port: str) -> str:
    return f"{SERIAL_DEVICE_PREFIX}{str(port or '').strip()}"


def _build_hid_payload(protocol: str, *, path: str, vid: int, pid: int, serial: str, manufacturer: str, product: str) -> dict:
    return {
        "transport": "hid",
        "protocol": protocol,
        "path": str(path or ""),
        "vid": int(vid or 0),
        "pid": int(pid or 0),
        "serial": str(serial or ""),
        "manufacturer": str(manufacturer or ""),
        "product": str(product or ""),
    }


def encode_hid_device_id(protocol: str, *, path: str, vid: int, pid: int, serial: str, manufacturer: str, product: str) -> str:
    payload = _build_hid_payload(
        protocol,
        path=path,
        vid=vid,
        pid=pid,
        serial=serial,
        manufacturer=manufacturer,
        product=product,
    )
    return HID_DEVICE_PREFIX + json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def canonicalize_meter_device_id(device_id: str) -> str:
    text = str(device_id or "").strip()
    if not text:
        return ""
    if text.startswith(SERIAL_DEVICE_PREFIX) or text.startswith(HID_DEVICE_PREFIX):
        return text
    return encode_serial_device_id(text)


def decode_meter_device_id(device_id: str) -> dict:
    text = canonicalize_meter_device_id(device_id)
    if not text:
        return {"transport": "", "protocol": "", "raw": ""}
    if text.startswith(SERIAL_DEVICE_PREFIX):
        return {"transport": "serial", "protocol": "serial", "port": text[len(SERIAL_DEVICE_PREFIX):], "raw": text}
    if text.startswith(HID_DEVICE_PREFIX):
        raw_payload = text[len(HID_DEVICE_PREFIX):]
        try:
            payload = json.loads(raw_payload)
        except Exception:
            payload = {"transport": "hid", "protocol": "", "path": "", "raw": raw_payload}
        payload.setdefault("transport", "hid")
        payload.setdefault("protocol", "")
        payload["raw"] = text
        return payload
    return {"transport": "serial", "protocol": "serial", "port": text, "raw": text}


def summarize_device_identity(device_id: str) -> str:
    info = decode_meter_device_id(device_id)
    if info.get("transport") == "serial":
        return str(info.get("port") or "")
    serial = str(info.get("serial") or "").strip()
    if serial:
        return serial
    path_text = str(info.get("path") or "").strip()
    if not path_text:
        return f"VID {int(info.get('vid', 0) or 0):04X}:PID {int(info.get('pid', 0) or 0):04X}"
    tail = path_text[-24:]
    return f"...{tail}" if len(path_text) > 24 else path_text


def _build_hid_label(prefix: str, device_id: str) -> str:
    identity = summarize_device_identity(device_id)
    return f"{prefix} ({identity})"


def _build_hid_tooltip(prefix: str, *, manufacturer: str, product: str, serial: str, path: str, vid: int, pid: int) -> str:
    lines = [
        prefix,
        f"VID: 0x{int(vid or 0):04X}",
        f"PID: 0x{int(pid or 0):04X}",
    ]
    if manufacturer:
        lines.append(f"Manufacturer: {manufacturer}")
    if product:
        lines.append(f"Product: {product}")
    if serial:
        lines.append(f"Serial: {serial}")
    if path:
        lines.append(f"Path: {path}")
    return "\n".join(lines)


def list_meter_device_options() -> list[MeterDeviceOption]:
    options: list[MeterDeviceOption] = []

    for port in list_serial_port_names():
        device_id = encode_serial_device_id(port)
        options.append(
            MeterDeviceOption(
                device_id=device_id,
                label=port,
                tooltip=f"Serial port: {port}",
                device_type="serial",
                protocol="serial",
            )
        )

    for info in enumerate_ut61eplus_devices():
        device_id = encode_hid_device_id(
            "ut61eplus",
            path=info.path_text,
            vid=info.vendor_id,
            pid=info.product_id,
            serial=info.serial_number,
            manufacturer=info.manufacturer,
            product=info.product,
        )
        options.append(
            MeterDeviceOption(
                device_id=device_id,
                label=_build_hid_label("UT61E+ HID", device_id),
                tooltip=_build_hid_tooltip(
                    "UT61E+ HID",
                    manufacturer=info.manufacturer,
                    product=info.product,
                    serial=info.serial_number,
                    path=info.path_text,
                    vid=info.vendor_id,
                    pid=info.product_id,
                ),
                device_type="hid",
                protocol="ut61eplus",
            )
        )

    for info in enumerate_victor_86e_devices():
        device_id = encode_hid_device_id(
            "victor86e",
            path=info.path_text,
            vid=info.vendor_id,
            pid=info.product_id,
            serial=info.serial_number,
            manufacturer=info.manufacturer,
            product=info.product,
        )
        options.append(
            MeterDeviceOption(
                device_id=device_id,
                label=_build_hid_label("Victor 86E HID", device_id),
                tooltip=_build_hid_tooltip(
                    "Victor 86E HID",
                    manufacturer=info.manufacturer,
                    product=info.product,
                    serial=info.serial_number,
                    path=info.path_text,
                    vid=info.vendor_id,
                    pid=info.product_id,
                ),
                device_type="hid",
                protocol="victor86e",
            )
        )

    options.sort(key=lambda item: (0 if item.device_type == "serial" else 1, item.label.lower(), item.device_id))
    return options


def list_meter_device_dicts() -> list[dict]:
    return [option.to_dict() for option in list_meter_device_options()]


def find_meter_device_option(device_id: str) -> MeterDeviceOption | None:
    canonical = canonicalize_meter_device_id(device_id)
    for option in list_meter_device_options():
        if option.device_id == canonical:
            return option
    if not canonical:
        return None
    summary = summarize_device_identity(canonical)
    info = decode_meter_device_id(canonical)
    if info.get("transport") == "hid":
        protocol = str(info.get("protocol") or "hid").strip() or "hid"
        label_prefix = "UT61E+ HID" if protocol == "ut61eplus" else "Victor 86E HID" if protocol == "victor86e" else "HID"
        return MeterDeviceOption(
            device_id=canonical,
            label=_build_hid_label(label_prefix, canonical),
            tooltip=str(info.get("path") or canonical),
            device_type="hid",
            protocol=protocol,
        )
    return MeterDeviceOption(
        device_id=canonical,
        label=summary,
        tooltip=f"Serial port: {summary}",
        device_type="serial",
        protocol="serial",
    )
