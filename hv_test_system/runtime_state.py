from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable


def _default_service_runtime() -> dict[str, Any]:
    return {
        "web_start": None,
        "web_stop": None,
        "web_status": None,
        "influx_start": None,
        "influx_stop": None,
        "influx_status": None,
    }


@dataclass
class AppRuntimeState:
    """Single source of truth for mutable application runtime state."""

    session_id: str = field(default_factory=lambda: time.strftime("%Y%m%d_%H%M%S"))
    current_run_id: str = ""

    prev_testing: bool = False
    prev_stabilizing: bool = False
    prev_recording: bool = False

    hv_voltage_cache: float = 0.0
    hv_voltage_ts: float = 0.0
    keithley_voltage_cache: float = 0.0
    keithley_voltage_ts: float = 0.0
    power_voltage_cache: dict[str, float] = field(default_factory=dict)
    power_voltage_ts: dict[str, float] = field(default_factory=dict)

    connected_power_name_by_type: dict[str, str | None] = field(
        default_factory=lambda: {"HAPS06": None, "Keithley 248": None}
    )
    pending_haps06_power_name: str | None = None

    save_path: str = ""
    service_runtime: dict[str, Any] = field(default_factory=_default_service_runtime)

    is_recording: bool = False
    is_converting: bool = False
    is_testing: bool = False
    is_cycle_testing: bool = False
    current_cycle: int = 0
    test_mode: str = "升压"
    auto_recording: bool = False
    cycle_recording_active: bool = False

    is_stabilizing: bool = False
    stabilization_running: bool = False
    stabilization_thread: Any = None
    active_test_controller: Any = None
    active_test_power_source: str | None = None
    active_stabilization_controller: Any = None
    active_stabilization_power_source: str | None = None


class RuntimeStateField:
    """Descriptor that forwards MainWindow-style attributes into runtime_state."""

    def __init__(self, state_field: str, coerce: Callable[[Any], Any] | None = None):
        self.state_field = state_field
        self.coerce = coerce

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return getattr(instance.runtime_state, self.state_field)

    def __set__(self, instance, value):
        if self.coerce is not None and value is not None:
            value = self.coerce(value)
        setattr(instance.runtime_state, self.state_field, value)
