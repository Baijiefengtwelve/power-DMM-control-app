from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .parameter_models import (
    AUTO_POWER_SOURCE_NAME,
    normalize_algorithm,
    normalize_current_source,
)


def _as_text(value: Any, default: str) -> str:
    text = str(value if value is not None else default).strip()
    return text or default


@dataclass(frozen=True)
class TestSettingsDialogState:
    start_voltage: str = "0"
    target_voltage: str = "1000"
    voltage_step: str = "10"
    step_delay: str = "1"
    cycle_time: str = "10"
    power_source_name: str = AUTO_POWER_SOURCE_NAME

    @classmethod
    def from_params(
        cls,
        params: Mapping[str, Any] | None,
        *,
        power_source_name: str | None = None,
    ) -> "TestSettingsDialogState":
        params = params or {}
        return cls(
            start_voltage=_as_text(params.get("start_voltage"), "0"),
            target_voltage=_as_text(params.get("target_voltage"), "1000"),
            voltage_step=_as_text(params.get("voltage_step"), "10"),
            step_delay=_as_text(params.get("step_delay"), "1"),
            cycle_time=_as_text(params.get("cycle_time"), "10"),
            power_source_name=_as_text(
                power_source_name if power_source_name is not None else params.get("power_source_name"),
                AUTO_POWER_SOURCE_NAME,
            ),
        )

    def to_param_updates(self) -> dict[str, str]:
        return {
            "start_voltage": self.start_voltage,
            "target_voltage": self.target_voltage,
            "voltage_step": self.voltage_step,
            "step_delay": self.step_delay,
            "cycle_time": self.cycle_time,
            "power_source_name": self.power_source_name,
        }


@dataclass(frozen=True)
class StabilizationSettingsDialogState:
    target_current: str = "1000"
    stability_range: str = "5"
    start_voltage: str = "100"
    power_source_name: str = AUTO_POWER_SOURCE_NAME
    current_source: str = "keithley"
    adjust_frequency: str = "1"
    max_adjust_voltage: str = "50"
    algorithm: str = "pid"
    pid_kp: str = "0.05"
    pid_ki: str = "0.01"
    pid_kd: str = "0.0"

    @classmethod
    def from_params(
        cls,
        params: Mapping[str, Any] | None,
        *,
        power_source_name: str | None = None,
    ) -> "StabilizationSettingsDialogState":
        params = params or {}
        return cls(
            target_current=_as_text(params.get("target_current"), "1000"),
            stability_range=_as_text(params.get("stability_range"), "5"),
            start_voltage=_as_text(params.get("start_voltage"), "100"),
            power_source_name=_as_text(
                power_source_name if power_source_name is not None else params.get("power_source_name"),
                AUTO_POWER_SOURCE_NAME,
            ),
            current_source=normalize_current_source(params.get("current_source")),
            adjust_frequency=_as_text(params.get("adjust_frequency"), "1"),
            max_adjust_voltage=_as_text(params.get("max_adjust_voltage"), "50"),
            algorithm=normalize_algorithm(params.get("algorithm")),
            pid_kp=_as_text(params.get("pid_kp"), "0.05"),
            pid_ki=_as_text(params.get("pid_ki"), "0.01"),
            pid_kd=_as_text(params.get("pid_kd"), "0.0"),
        )

    def to_param_updates(self) -> dict[str, str]:
        return {
            "target_current": self.target_current,
            "stability_range": self.stability_range,
            "start_voltage": self.start_voltage,
            "power_source_name": self.power_source_name,
            "current_source": normalize_current_source(self.current_source),
            "adjust_frequency": self.adjust_frequency,
            "max_adjust_voltage": self.max_adjust_voltage,
            "algorithm": normalize_algorithm(self.algorithm),
            "pid_kp": self.pid_kp,
            "pid_ki": self.pid_ki,
            "pid_kd": self.pid_kd,
        }
