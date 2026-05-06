from __future__ import annotations

import json

from ..constants import DATA_HEADERS
from ..keithley_controller import Keithley248Controller
from ..parameter_models import AUTO_POWER_SOURCE_NAME, normalize_power_source_selection


class PowerCatalogService:
    """Own power-device catalog, source resolution, interlocks, and summary text."""

    def __init__(self, mw):
        self.mw = mw

    def normalize_power_type(self, power_type: str) -> str:
        text = str(power_type or "").strip().lower()
        if "keithley" in text or "248" in text or "2290" in text:
            return "Keithley 248"
        return "HAPS06"

    def type_to_legacy_key(self, power_type: str) -> str:
        return "keithley" if self.normalize_power_type(power_type) == "Keithley 248" else "haps06"

    def power_source_name(self, source_key: str) -> str:
        val = str(source_key or "").strip()
        if not val or val.lower() == "auto" or val == AUTO_POWER_SOURCE_NAME:
            return AUTO_POWER_SOURCE_NAME
        return val

    def resolve_power_source_selection(self, selected_name: str) -> tuple[str, str]:
        return normalize_power_source_selection(
            selected_name,
            valid_names=self.list_power_source_names(include_auto=True),
            resolve_legacy_key=lambda name: self.type_to_legacy_key(
                (self.find_power_device(name) or {}).get("type", "HAPS06")
            ),
        )

    def default_power_devices(self):
        mw = self.mw
        return [
            {
                "name": "HAPS06电源",
                "type": "HAPS06",
                "address": mw.hv_port_combo.currentText() if hasattr(mw, "hv_port_combo") else "",
                "baudrate": mw.hv_baudrate_combo.currentText() if hasattr(mw, "hv_baudrate_combo") else "9600",
            },
            {
                "name": "Keithley电源",
                "type": "Keithley 248",
                "address": mw.keithley_addr_combo.currentText() if hasattr(mw, "keithley_addr_combo") else "14",
                "baudrate": "",
            },
        ]

    def ensure_unique_power_name(self, name: str, exclude_index: int | None = None, devices=None) -> str:
        mw = self.mw
        base = str(name or "").strip() or "电源"
        source_devices = list(devices if devices is not None else getattr(mw, "power_devices", []))
        names = {
            str(device.get("name", "")).strip()
            for i, device in enumerate(source_devices)
            if exclude_index is None or i != exclude_index
        }
        if base not in names:
            return base

        idx = 2
        while f"{base}_{idx}" in names:
            idx += 1
        return f"{base}_{idx}"

    def load_power_devices_from_config(self):
        mw = self.mw
        devices = []
        try:
            raw = mw.config.get("PowerSources", "devices_json", fallback="").strip()
            if raw:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    for item in parsed:
                        if not isinstance(item, dict):
                            continue
                        devices.append(
                            {
                                "name": str(item.get("name", "")).strip() or "电源",
                                "type": self.normalize_power_type(item.get("type", "HAPS06")),
                                "address": str(item.get("address", "")).strip(),
                                "baudrate": str(item.get("baudrate", "9600")).strip() or "9600",
                            }
                        )
        except Exception:
            devices = []

        if not devices:
            haps_port = (
                mw.config.get("HighVoltage", "port", fallback="").strip()
                if mw.config.has_section("HighVoltage")
                else ""
            )
            haps_baud = (
                mw.config.get("HighVoltage", "baudrate", fallback="9600").strip()
                if mw.config.has_section("HighVoltage")
                else "9600"
            )
            keithley_addr = (
                mw.config.get("Keithley248", "gpib_address", fallback="14").strip()
                if mw.config.has_section("Keithley248")
                else "14"
            )
            devices = [
                {"name": "HAPS06电源", "type": "HAPS06", "address": haps_port, "baudrate": haps_baud or "9600"},
                {"name": "Keithley电源", "type": "Keithley 248", "address": keithley_addr, "baudrate": ""},
            ]

        normalized = []
        for item in devices:
            normalized.append(
                {
                    "name": str(item.get("name", "")).strip() or f"电源{len(normalized) + 1}",
                    "type": self.normalize_power_type(item.get("type", "HAPS06")),
                    "address": str(item.get("address", "")).strip(),
                    "baudrate": str(item.get("baudrate", "9600")).strip() or "9600",
                }
            )

        for index, item in enumerate(normalized):
            item["name"] = self.ensure_unique_power_name(item.get("name"), exclude_index=index, devices=normalized)
        mw.power_devices = normalized

    def power_devices_config_section(self):
        return {"devices_json": json.dumps(self.mw.power_devices, ensure_ascii=False)}

    def find_power_device_index(self, name: str):
        clean_name = str(name or "").strip()
        for index, device in enumerate(self.mw.power_devices):
            if str(device.get("name", "")).strip() == clean_name:
                return index
        return -1

    def find_power_device(self, name: str):
        idx = self.find_power_device_index(name)
        return self.mw.power_devices[idx] if idx >= 0 else None

    def list_power_source_names(self, include_auto: bool = True):
        names = [str(item.get("name", "")).strip() for item in self.mw.power_devices if str(item.get("name", "")).strip()]
        return ([AUTO_POWER_SOURCE_NAME] + names) if include_auto else names

    def get_selected_power_name(self, module: str) -> str:
        module_key = str(module).lower()
        params = self.mw.stabilization_params if module_key == "stabilization" else self.mw.test_params
        selected = str(params.get("power_source_name", "") or "").strip()
        if selected:
            return selected

        legacy = str(params.get("power_source", "auto") or "auto").strip().lower()
        if legacy in ("", "auto"):
            return AUTO_POWER_SOURCE_NAME
        if legacy in ("haps06", "haps"):
            return self.mw.connected_power_name_by_type.get("HAPS06") or next(
                (
                    device["name"]
                    for device in self.mw.power_devices
                    if self.normalize_power_type(device.get("type")) == "HAPS06"
                ),
                AUTO_POWER_SOURCE_NAME,
            )
        if legacy in ("keithley", "keithley248", "248"):
            return self.mw.connected_power_name_by_type.get("Keithley 248") or next(
                (
                    device["name"]
                    for device in self.mw.power_devices
                    if self.normalize_power_type(device.get("type")) == "Keithley 248"
                ),
                AUTO_POWER_SOURCE_NAME,
            )
        return legacy

    def connected_power_sources(self):
        sources = {}
        try:
            connected_map = dict(getattr(self.mw, "connected_named_power_controllers", {}) or {})
            for name, controller in connected_map.items():
                if controller is not None and bool(getattr(controller, "is_connected", False)):
                    sources[str(name).strip()] = controller
        except Exception:
            pass
        return sources

    def get_connected_keithley_names(self):
        names = []
        try:
            for device in self.mw.power_devices:
                name = str(device.get("name", "")).strip()
                if not name or self.normalize_power_type(device.get("type")) != "Keithley 248":
                    continue
                controller = getattr(self.mw, "connected_named_power_controllers", {}).get(name)
                if controller is not None and bool(getattr(controller, "is_connected", False)):
                    names.append(name)
        except Exception:
            pass
        return names

    def refresh_keithley_controller_alias(self, preferred_name: str | None = None):
        mw = self.mw
        preferred = str(preferred_name or "").strip()
        controller = None

        if preferred:
            controller = getattr(mw, "connected_named_power_controllers", {}).get(preferred)
            if controller is not None and not bool(getattr(controller, "is_connected", False)):
                controller = None

        if controller is None:
            for active_name in (
                str(getattr(mw, "active_stabilization_power_source", "") or "").strip(),
                str(getattr(mw, "active_test_power_source", "") or "").strip(),
            ):
                controller = getattr(mw, "connected_named_power_controllers", {}).get(active_name)
                if controller is not None and bool(getattr(controller, "is_connected", False)):
                    preferred = active_name
                    break
                controller = None

        if controller is None:
            for name in self.get_connected_keithley_names():
                controller = getattr(mw, "connected_named_power_controllers", {}).get(name)
                if controller is not None and bool(getattr(controller, "is_connected", False)):
                    preferred = name
                    break

        if controller is None:
            controller = Keithley248Controller()
            preferred = ""

        mw.keithley_controller = controller
        mw.connected_power_name_by_type["Keithley 248"] = preferred or None
        return controller

    def auto_bind_connected_power_to_modules(self, name: str):
        mw = self.mw
        clean_name = str(name or "").strip()
        if not clean_name:
            return

        device = self.find_power_device(clean_name)
        if not device:
            return

        new_type = self.normalize_power_type(device.get("type"))
        changed = False
        for params, module in ((mw.test_params, "test"), (mw.stabilization_params, "stabilization")):
            selected = str(self.get_selected_power_name(module) or "").strip()
            if selected in ("", "auto", AUTO_POWER_SOURCE_NAME) or selected == clean_name:
                continue

            selected_device = self.find_power_device(selected)
            if selected_device is None:
                params["power_source_name"] = clean_name
                params["power_source"] = self.type_to_legacy_key(new_type)
                changed = True
                continue

            selected_type = self.normalize_power_type(selected_device.get("type"))
            if selected_type != new_type:
                continue
            if not mw.is_power_device_connected(selected):
                params["power_source_name"] = clean_name
                params["power_source"] = self.type_to_legacy_key(new_type)
                changed = True

        if changed:
            mw.update_settings_display()

    def power_device_type_matches(self, source_name: str, power_type: str) -> bool:
        clean_name = str(source_name or "").strip()
        if not clean_name:
            return False

        device = self.find_power_device(clean_name)
        if device:
            return self.normalize_power_type(device.get("type")) == self.normalize_power_type(power_type)

        legacy = clean_name.lower()
        if legacy in ("haps06", "haps"):
            return self.normalize_power_type(power_type) == "HAPS06"
        if legacy in ("keithley", "keithley248", "248"):
            return self.normalize_power_type(power_type) == "Keithley 248"
        return False

    def resolve_power_controller_for_selection(self, module: str, selected_name: str, allow_auto: bool = True):
        sources = self.connected_power_sources()
        if not sources:
            return None, None, "未连接可用高压电源"

        if selected_name in ("", "auto", AUTO_POWER_SOURCE_NAME):
            if not allow_auto:
                return None, None, "当前模式不允许自动判断电源"
            if len(sources) == 1:
                key, controller = next(iter(sources.items()))
                return controller, key, ""
            module_label = "稳流设置" if str(module).lower() == "stabilization" else "测试设置"
            return None, None, f"当前已连接多个电源，请在{module_label}中明确选择电源名称"

        controller = sources.get(selected_name)
        if controller is not None:
            return controller, selected_name, ""

        legacy = str(selected_name).strip().lower()
        if legacy in ("haps06", "haps", "keithley", "keithley248", "248"):
            wanted_type = "Keithley 248" if "keithley" in legacy or legacy == "248" else "HAPS06"
            for name, ctrl in sources.items():
                if self.power_device_type_matches(name, wanted_type):
                    return ctrl, name, ""

        return None, None, f"所选电源 {self.power_source_name(selected_name)} 未连接"

    def resolve_power_controller(self, module: str, allow_auto: bool = True):
        return self.resolve_power_controller_for_selection(
            module,
            self.get_selected_power_name(module),
            allow_auto=allow_auto,
        )

    def can_execute_test_actions(self) -> bool:
        controller, source_name, _ = self.resolve_power_controller("test")
        if controller is None:
            return False
        return not bool(self.runtime_power_interlock_error("test", source_name))

    def can_execute_stabilization_actions(self) -> bool:
        controller, source_name, _ = self.resolve_power_controller("stabilization")
        if controller is None:
            return False

        current_source = str(self.mw.stabilization_params.get("current_source", "keithley"))
        if current_source == "keithley" and not bool(getattr(controller, "supports_internal_current_readback", False)):
            return False
        return not bool(self.runtime_power_interlock_error("stabilization", source_name))

    def validate_selected_power_interlock(self, *, test_source_name: str, stabilization_source_name: str) -> str:
        test_name = str(test_source_name or "").strip()
        stab_name = str(stabilization_source_name or "").strip()
        if not test_name or not stab_name:
            return ""
        if test_name == AUTO_POWER_SOURCE_NAME or stab_name == AUTO_POWER_SOURCE_NAME:
            return ""
        if test_name == stab_name:
            return "升压测试和稳流测试不能同时选择同一个电源"
        return ""

    def runtime_power_interlock_error(self, module: str, resolved_source_name: str) -> str:
        source_name = str(resolved_source_name or "").strip()
        if not source_name:
            return ""

        module_key = str(module or "").strip().lower()
        if module_key == "test":
            other_running = bool(getattr(self.mw, "is_stabilizing", False))
            other_source = str(getattr(self.mw, "active_stabilization_power_source", "") or "").strip()
            other_label = "稳流测试"
        else:
            other_running = bool(getattr(self.mw, "is_testing", False))
            other_source = str(getattr(self.mw, "active_test_power_source", "") or "").strip()
            other_label = "升压测试"

        if other_running and other_source and other_source == source_name:
            return f"{other_label}正在占用电源 {source_name}，请切换到其他电源后再开始"
        return ""

    def power_display_names(self) -> list[str]:
        names = []
        try:
            for device in self.mw.power_devices:
                name = str(device.get("name", "")).strip()
                if name:
                    names.append(name)
        except Exception:
            pass
        return names

    def display_power_name(self, slot_index: int) -> str:
        names = self.power_display_names()
        if 0 <= int(slot_index) < len(names):
            return names[int(slot_index)]
        return f"电源{int(slot_index) + 1}"

    def update_power_display_titles(self):
        try:
            if hasattr(self.mw, "hv_voltage_title_label"):
                self.mw.hv_voltage_title_label.setText(f"{self.display_power_name(0)}电压:")
        except Exception:
            pass
        try:
            if hasattr(self.mw, "keithley_voltage_title_label"):
                self.mw.keithley_voltage_title_label.setText(f"{self.display_power_name(1)}电压:")
        except Exception:
            pass

    def get_power_device_status_text(self, name: str) -> str:
        mw = self.mw
        device = self.find_power_device(name)
        if not device:
            return "配置不存在"

        power_type = self.normalize_power_type(device.get("type"))
        clean_name = str(name or "").strip()
        if power_type == "HAPS06" and mw.device_manager.is_hv_connecting_for(clean_name):
            return f"{name} 连接中（{power_type}）"
        if mw.is_power_device_connected(name):
            return f"{name} 已连接（{power_type}）"
        if power_type == "HAPS06" and mw.connected_power_name_by_type.get("HAPS06"):
            current = mw.connected_power_name_by_type.get("HAPS06")
            return f"当前在线: {current}（HAPS06 同时仅保留一台在线）"
        if power_type == "Keithley 248":
            names = self.get_connected_keithley_names()
            if names:
                return f"当前在线 {len(names)} 台: {'、'.join(names)}"
        return f"{name} 未连接"

    def build_power_summary_text(self) -> str:
        mw = self.mw
        parts = []
        if mw.pending_haps06_power_name and mw.device_manager.is_hv_connecting_for(mw.pending_haps06_power_name):
            parts.append(f"HAPS06: {mw.pending_haps06_power_name}（连接中）")
        elif bool(getattr(mw.hv_controller, "is_connected", False)):
            parts.append(f"HAPS06: {mw.connected_power_name_by_type.get('HAPS06') or '已连接'}")

        keithley_names = self.get_connected_keithley_names()
        if keithley_names:
            parts.append(f"Keithley({len(keithley_names)}): {'、'.join(keithley_names)}")
        return "；".join(parts) if parts else "未连接"

    def update_power_summary_label(self):
        mw = self.mw
        try:
            mw.refresh_power_voltage_slots()
            mw.power_summary_label.setText(self.build_power_summary_text())
            if hasattr(mw, "manual_voltage_target_combo"):
                current = mw.manual_voltage_target_combo.currentText()
                names = self.power_display_names()
                mw.manual_voltage_target_combo.clear()
                mw.manual_voltage_target_combo.addItems(names)
                if current in names:
                    mw.manual_voltage_target_combo.setCurrentText(current)
        except Exception:
            pass

    def get_record_power_name(self, module: str) -> str:
        module_key = str(module or "").strip().lower()
        active_name = ""
        try:
            if module_key == "test":
                active_name = str(getattr(self.mw, "active_test_power_source", "") or "").strip()
            elif module_key in ("stabilization", "stab"):
                active_name = str(getattr(self.mw, "active_stabilization_power_source", "") or "").strip()
        except Exception:
            active_name = ""
        if active_name:
            return active_name

        selected_name = str(
            self.get_selected_power_name(module_key if module_key != "stab" else "stabilization") or ""
        ).strip()
        if selected_name and selected_name not in ("auto", AUTO_POWER_SOURCE_NAME):
            return selected_name

        try:
            _, resolved_name, _ = self.resolve_power_controller(
                module_key if module_key != "stab" else "stabilization",
                allow_auto=True,
            )
            resolved_name = str(resolved_name or "").strip()
            if resolved_name:
                return resolved_name
        except Exception:
            pass
        return AUTO_POWER_SOURCE_NAME

    def get_record_power_voltage(self, source_name: str):
        clean_name = str(source_name or "").strip()
        if not clean_name or clean_name == AUTO_POWER_SOURCE_NAME:
            return None

        cached_value, _ = self.mw._get_power_voltage_cache(clean_name)
        if cached_value is not None:
            try:
                return float(cached_value)
            except Exception:
                pass

        device = self.find_power_device(clean_name)
        if not device:
            return None

        power_type = self.normalize_power_type(device.get("type"))
        if power_type == "HAPS06":
            try:
                if bool(getattr(self.mw.hv_controller, "is_connected", False)) and str(
                    self.mw.connected_power_name_by_type.get("HAPS06") or ""
                ).strip() == clean_name:
                    return float(getattr(self.mw.hv_controller, "actual_voltage", 0.0))
            except Exception:
                pass
        elif power_type == "Keithley 248":
            try:
                controller = getattr(self.mw, "connected_named_power_controllers", {}).get(clean_name)
                if controller is None:
                    controller = getattr(self.mw, "named_keithley_controllers", {}).get(clean_name)
                if controller is not None and bool(getattr(controller, "is_connected", False)):
                    return float(getattr(controller, "current_voltage", 0.0))
            except Exception:
                pass
        return None

    def build_record_headers(self):
        return list(DATA_HEADERS)
