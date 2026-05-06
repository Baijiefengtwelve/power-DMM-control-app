from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping

AUTO_POWER_SOURCE_NAME = "自动判断"


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def normalize_algorithm(value: Any) -> str:
    text = str(value or "pid").strip().lower()
    mapping = {
        "pid": "pid",
        "pid算法": "pid",
        "pid控制": "pid",
        "approach": "approach",
        "接近": "approach",
        "接近算法": "approach",
        "接近算法(1v步进)": "approach",
    }
    return mapping.get(text, "pid")


def normalize_current_source(value: Any) -> str:
    text = str(value or "keithley").strip().lower()
    mapping = {
        "keithley": "keithley",
        "keithley自身": "keithley",
        "电源自身": "keithley",
        "自身": "keithley",
        "cathode": "cathode",
        "阴极": "cathode",
        "gate": "gate",
        "栅极": "gate",
        "anode": "anode",
        "阳极": "anode",
        "backup": "backup",
        "收集极": "backup",
    }
    return mapping.get(text, "keithley")


def normalize_power_source_selection(
    raw_source: Any,
    *,
    valid_names: Iterable[str],
    resolve_legacy_key: Callable[[str], str],
) -> tuple[str, str]:
    selected = str(raw_source or AUTO_POWER_SOURCE_NAME).strip() or AUTO_POWER_SOURCE_NAME
    lowered = selected.lower()
    aliases = {
        "auto": ("auto", AUTO_POWER_SOURCE_NAME),
        "自动": ("auto", AUTO_POWER_SOURCE_NAME),
        "自动判断": ("auto", AUTO_POWER_SOURCE_NAME),
        "haps06": ("haps06", "HAPS06"),
        "haps": ("haps06", "HAPS06"),
        "keithley": ("keithley", "Keithley 248"),
        "keithley248": ("keithley", "Keithley 248"),
        "248": ("keithley", "Keithley 248"),
    }
    if lowered in aliases:
        return aliases[lowered]

    valid = set(valid_names or [])
    if selected in valid:
        if selected == AUTO_POWER_SOURCE_NAME:
            return "auto", AUTO_POWER_SOURCE_NAME
        legacy_key = str(resolve_legacy_key(selected) or "auto").strip().lower() or "auto"
        return legacy_key, selected

    raise ValueError(f"Invalid power source: {selected}")


class ParameterModel(dict):
    defaults: dict[str, Any] = {}
    float_fields: tuple[str, ...] = ()

    def __init__(self, initial: Mapping[str, Any] | None = None):
        super().__init__(self.defaults.copy())
        if initial:
            self.apply_update(initial)

    def apply_update(self, values: Mapping[str, Any] | None):
        if not values:
            return self
        for key, value in values.items():
            self[key] = self._normalize_value(key, value)
        return self

    def as_dict(self) -> dict[str, Any]:
        return dict(self)

    def _normalize_value(self, key: str, value: Any) -> Any:
        if key in self.float_fields:
            default = self.defaults.get(key, 0.0)
            return _coerce_float(value, float(default))
        return value


class TestParameters(ParameterModel):
    defaults = {
        "start_voltage": 0.0,
        "target_voltage": 1000.0,
        "voltage_step": 10.0,
        "step_delay": 1.0,
        "cycle_time": 10.0,
        "power_source_name": AUTO_POWER_SOURCE_NAME,
        "power_source": "auto",
    }
    float_fields = (
        "start_voltage",
        "target_voltage",
        "voltage_step",
        "step_delay",
        "cycle_time",
    )

    def _normalize_value(self, key: str, value: Any) -> Any:
        if key in ("power_source_name",):
            return str(value or AUTO_POWER_SOURCE_NAME).strip() or AUTO_POWER_SOURCE_NAME
        if key in ("power_source",):
            return str(value or "auto").strip().lower() or "auto"
        return super()._normalize_value(key, value)


class StabilizationParameters(ParameterModel):
    defaults = {
        "target_current": 1000.0,
        "stability_range": 5.0,
        "start_voltage": 100.0,
        "power_source_name": AUTO_POWER_SOURCE_NAME,
        "power_source": "auto",
        "current_source": "keithley",
        "adjust_frequency": 1.0,
        "max_adjust_voltage": 50.0,
        "algorithm": "pid",
        "pid_kp": 0.05,
        "pid_ki": 0.01,
        "pid_kd": 0.0,
    }
    float_fields = (
        "target_current",
        "stability_range",
        "start_voltage",
        "adjust_frequency",
        "max_adjust_voltage",
        "pid_kp",
        "pid_ki",
        "pid_kd",
    )

    def _normalize_value(self, key: str, value: Any) -> Any:
        if key == "power_source_name":
            return str(value or AUTO_POWER_SOURCE_NAME).strip() or AUTO_POWER_SOURCE_NAME
        if key == "power_source":
            return str(value or "auto").strip().lower() or "auto"
        if key == "current_source":
            return normalize_current_source(value)
        if key == "algorithm":
            return normalize_algorithm(value)
        return super()._normalize_value(key, value)
